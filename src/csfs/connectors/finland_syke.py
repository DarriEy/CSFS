"""SYKE connector -- Finnish Environment Institute hydrology data.

Uses the confirmed OData 3.0 API for hydrological observations from Finland.
Base URL: https://rajapinnat.ymparisto.fi/api/Hydrologiarajapinta/1.0/odata

Endpoints:
  - Stations: /Paikka  (Paikka_Id, Nimi, KoordinaattiPiste)
  - Discharge: /Virtaama (filtered by Paikka_Id, ordered by Aika desc)
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# OData responses wrap records under a "value" key.
_ODATA_VALUE_KEY = "value"


def _quality_from_syke(raw: str | None) -> QualityFlag:
    """Map SYKE quality codes to CSFS quality flags.

    SYKE quality codes (known patterns):
        "good", "verified", "2"  -> GOOD
        "suspect", "1"           -> SUSPECT
        "estimated"              -> ESTIMATED
        None / ""                -> RAW (no quality info provided)
    """
    if raw is None:
        return QualityFlag.RAW
    code = raw.strip().lower()
    if code in ("good", "verified", "2", "approved"):
        return QualityFlag.GOOD
    if code in ("suspect", "1", "uncertain"):
        return QualityFlag.SUSPECT
    if code in ("estimated", "3"):
        return QualityFlag.ESTIMATED
    if code == "":
        return QualityFlag.RAW
    return QualityFlag.RAW


def _parse_coordinates(coord_value: str | dict | None) -> tuple[float, float]:
    """Extract (latitude, longitude) from the KoordinaattiPiste field.

    The field may be a GeoJSON-like dict with "coordinates": [lon, lat]
    or a WKT POINT string like "POINT (lon lat)".
    Returns (0.0, 0.0) if parsing fails.
    """
    if coord_value is None:
        return 0.0, 0.0

    if isinstance(coord_value, dict):
        coords = coord_value.get("coordinates")
        if isinstance(coords, (list, tuple)) and len(coords) >= 2:
            return float(coords[1]), float(coords[0])  # GeoJSON is [lon, lat]
        return 0.0, 0.0

    if isinstance(coord_value, str):
        # Try WKT POINT (lon lat)
        match = re.search(r"POINT\s*\(\s*([\d.+-]+)\s+([\d.+-]+)\s*\)", coord_value, re.IGNORECASE)
        if match:
            lon, lat = float(match.group(1)), float(match.group(2))
            return lat, lon
        return 0.0, 0.0

    return 0.0, 0.0


@register("finland_syke")
class FinlandSYKEConnector(BaseConnector):
    slug = "finland_syke"
    display_name = "SYKE Hydrology (Finland)"
    base_url = "https://rajapinnat.ymparisto.fi/api/Hydrologiarajapinta/1.0/odata"
    country_codes = ["FI"]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return hydrological stations from the SYKE OData /Paikka endpoint."""
        params = {
            "$select": "Paikka_Id,Nimi,KoordinaattiPiste",
            "$format": "json",
        }
        try:
            resp = await self._get("/Paikka", params=params)
        except (httpx.HTTPStatusError, ConnectorError) as exc:
            raise ConnectorError(
                self.slug, f"Failed to fetch stations: {exc}"
            ) from exc

        data = resp.json()
        return self._parse_stations(data)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations from the SYKE OData /Virtaama endpoint."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        # Ensure start/end are timezone-aware (UTC) for comparison
        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)
        if end.tzinfo is None:
            end = end.replace(tzinfo=UTC)

        params = {
            "$filter": f"Paikka_Id eq {native_id}",
            "$top": "1000",
            "$orderby": "Aika desc",
            "$format": "json",
        }
        try:
            resp = await self._get("/Virtaama", params=params)
        except (httpx.HTTPStatusError, ConnectorError) as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch observations for station {native_id}: {exc}",
            ) from exc

        data = resp.json()
        return self._parse_observations(data, station_id, start, end)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent observations (last 24 h)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    def _parse_stations(self, data: dict) -> list[Station]:
        """Parse the OData /Paikka response.

        Expected shape: {"value": [{"Paikka_Id": ..., "Nimi": ..., ...}, ...]}
        """
        entries = data.get(_ODATA_VALUE_KEY, []) if isinstance(data, dict) else data
        if not isinstance(entries, list):
            raise DataFormatError(
                self.slug, "Expected OData response with 'value' array from /Paikka"
            )

        stations: list[Station] = []
        for entry in entries:
            native_id = entry.get("Paikka_Id")
            if native_id is None:
                continue
            native_id = str(native_id)

            name = entry.get("Nimi", "")
            lat, lon = _parse_coordinates(entry.get("KoordinaattiPiste"))

            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=name,
                    latitude=lat,
                    longitude=lon,
                    country_code="FI",
                    is_active=True,
                ))
            except (ValueError, KeyError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    station=native_id,
                    error=str(exc),
                )
                continue
        return stations

    def _parse_observations(
        self,
        data: dict,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Parse the OData /Virtaama response.

        Expected shape: {"value": [{"Aika": ..., "Arvo": ..., ...}, ...]}
        Fields: Aika = timestamp, Arvo = value (discharge m3/s).
        """
        entries = data.get(_ODATA_VALUE_KEY, []) if isinstance(data, dict) else data
        if not isinstance(entries, list):
            raise DataFormatError(
                self.slug, "Expected OData response with 'value' array from /Virtaama"
            )

        observations: list[Observation] = []
        for entry in entries:
            time_str = entry.get("Aika")
            if time_str is None:
                continue

            try:
                ts = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            except (ValueError, AttributeError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid timestamp in observation: {time_str}",
                ) from exc

            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)

            # Client-side date range filter
            if ts < start or ts > end:
                continue

            raw_value = entry.get("Arvo")
            discharge = float(raw_value) if raw_value is not None else None
            quality_code = entry.get("Laatu")
            quality = QualityFlag.MISSING if discharge is None else _quality_from_syke(quality_code)

            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=discharge,
                quality=quality,
            ))

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )
