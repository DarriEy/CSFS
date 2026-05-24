"""Italy ARPA Piemonte connector — regional hydrological data.

ARPA Piemonte (Agenzia Regionale per la Protezione Ambientale)
publishes hydrological data for the Piedmont region through its
"Rischi Naturali" platform.

Endpoints used
--------------
* Station listing:
  GET /stations.json
  Returns ``[{code, name, lat, lon, river}]``.

* Observations:
  GET /data/{code}/portata?from={date}&to={date}&format=json
  Returns ``[{timestamp, value}, ...]``.

The connector is built defensively with fallback parsing and
structured logging.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import (
    Observation,
    QualityFlag,
    Station,
    TimeSeriesChunk,
)
from csfs.core.registry import register

logger = structlog.get_logger()


@register("italy_piedmont")
class ItalyPiedmontConnector(BaseConnector):
    """Connector for ARPA Piemonte hydrological discharge data."""

    slug = "italy_piedmont"
    display_name = "ARPA Piemonte (Italy)"
    base_url = (
        "https://www.arpa.piemonte.it"
        "/rischinaturali/tematismi/dati-idrologici"
    )
    country_codes = ["IT"]

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all hydrometric stations from ARPA Piemonte."""
        try:
            resp = await self._get("/stations.json")
        except httpx.HTTPStatusError as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch station list: "
                f"HTTP {exc.response.status_code}",
            ) from exc

        return self._parse_stations(resp.json())

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for *station_id*."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        params: dict[str, str] = {
            "from": start.strftime("%Y-%m-%d"),
            "to": end.strftime("%Y-%m-%d"),
            "format": "json",
        }

        try:
            resp = await self._get(
                f"/data/{native_id}/portata",
                params=params,
            )
        except httpx.HTTPStatusError as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch observations for {native_id}: "
                f"HTTP {exc.response.status_code}",
            ) from exc

        return self._parse_observations(resp.json(), station_id)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch most recent discharge observations (last 24 h)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # -----------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------

    def _parse_stations(
        self,
        data: list[dict] | dict,
    ) -> list[Station]:
        """Parse ARPA Piemonte station-list JSON.

        May be a bare list or wrapped under a key such as
        ``stations`` or ``stazioni``.
        """
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = (
                data.get("stations")
                or data.get("stazioni")
                or data.get("data", [])
            )
        else:
            logger.warning(
                "unexpected_stations_type",
                provider=self.slug,
                type=type(data).__name__,
            )
            return []

        stations: list[Station] = []
        for entry in items:
            native_id = str(
                entry.get("code", "")
            ).strip()
            if not native_id:
                continue

            lat = entry.get("lat") or entry.get("latitude")
            lon = entry.get("lon") or entry.get("longitude")
            if lat is None or lon is None:
                logger.warning(
                    "station_missing_coords",
                    provider=self.slug,
                    station=native_id,
                )
                continue

            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=entry.get("name", native_id),
                    latitude=float(lat),
                    longitude=float(lon),
                    country_code="IT",
                    river=entry.get("river"),
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
        data: dict | list,
        station_id: str,
    ) -> TimeSeriesChunk:
        """Parse ARPA Piemonte observations into a ``TimeSeriesChunk``.

        Expected shape::

            [
                {"timestamp": "2024-06-01T12:00:00", "value": 34.5},
                ...
            ]

        May also be wrapped as ``{"data": [...]}``.
        """
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = (
                data.get("values")
                or data.get("data", [])
            )
        else:
            raise DataFormatError(
                self.slug,
                f"Unexpected response type: {type(data).__name__}",
            )

        observations: list[Observation] = []
        for entry in items:
            ts_raw = (
                entry.get("timestamp")
                or entry.get("data")
                or entry.get("datetime")
            )
            if not ts_raw:
                logger.warning(
                    "observation_missing_timestamp",
                    provider=self.slug,
                    station=station_id,
                )
                continue

            try:
                ts = datetime.fromisoformat(str(ts_raw))
            except ValueError as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid timestamp '{ts_raw}': {exc}",
                ) from exc

            value = entry.get("value") or entry.get("valore")
            discharge = (
                float(value) if value is not None else None
            )
            quality = (
                QualityFlag.MISSING
                if discharge is None
                else QualityFlag.RAW
            )

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
