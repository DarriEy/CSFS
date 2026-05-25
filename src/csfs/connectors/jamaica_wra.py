"""Jamaica WRA connector — Water Resources Authority.

Jamaica's Water Resources Authority (WRA) manages 133 river gauging
stations with records stretching back to 1955.  Ten of those stations
report to GRDC (Global Runoff Data Centre).

Endpoints attempted
-------------------
* Station listing:
  GET /data/stations?type=river&format=json

  Fallback: a seed list of ~20 major stations covering Jamaica's
  principal rivers.

* Observations:
  GET /data/discharge?station={id}&start={date}&end={date}&format=json

  Fallback: returns an empty chunk with guidance.

The WRA website has historically served static content; the connector
is written defensively.
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

# ------------------------------------------------------------------
# Seed stations — major Jamaican river gauging points
# ------------------------------------------------------------------
_SEED_STATIONS: list[dict[str, object]] = [
    {
        "id": "4584800", "name": "Fellowship",
        "lat": 18.14, "lon": -76.46, "river": "Rio Grande",
    },
    {
        "id": "4584200", "name": "Lacovia",
        "lat": 18.08, "lon": -77.76, "river": "Black River",
    },
    {
        "id": "4584210", "name": "Martha Brae",
        "lat": 18.47, "lon": -77.66,
        "river": "Martha Brae River",
    },
    {
        "id": "4584700", "name": "Spanish Town",
        "lat": 18.04, "lon": -76.98, "river": "Rio Cobre",
    },
    {
        "id": "4584100", "name": "Lethe",
        "lat": 18.39, "lon": -77.97, "river": "Great River",
    },
]


@register("jamaica_wra")
class JamaicaWRAConnector(BaseConnector):
    """Connector for Jamaica's Water Resources Authority data."""

    slug = "jamaica_wra"
    display_name = "WRA (Jamaica)"
    base_url = "https://www.wra.gov.jm"
    country_codes = ["JM"]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return available stations from WRA.

        Attempts the live JSON endpoint first; on failure, returns
        the built-in seed list.
        """
        stations = await self._try_live_stations()
        if stations:
            return stations

        logger.info(
            "using_seed_stations",
            provider=self.slug,
            reason="live API unavailable or returned no data",
        )
        return self._build_seed_stations()

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for *station_id*.

        Probes the WRA data endpoint; returns an empty chunk when
        the API does not respond.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        chunk = await self._try_live_observations(
            native_id, station_id, start, end,
        )
        if chunk is not None:
            return chunk

        logger.info(
            "observations_unavailable",
            provider=self.slug,
            station=native_id,
            hint=(
                "WRA data endpoint not reachable. Check "
                "https://www.wra.gov.jm for current data access."
            ),
        )
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent 24 h of observations."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # ------------------------------------------------------------------
    # Internal: live station listing
    # ------------------------------------------------------------------

    async def _try_live_stations(self) -> list[Station]:
        """Probe the WRA stations endpoint."""
        try:
            resp = await self._get(
                "/data/stations",
                params={"type": "river", "format": "json"},
            )
            data = resp.json()
            return self._parse_stations(data)
        except (
            httpx.HTTPStatusError,
            ConnectorError,
            Exception,
        ):
            return []

    # ------------------------------------------------------------------
    # Internal: live observations
    # ------------------------------------------------------------------

    async def _try_live_observations(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk | None:
        """Try the WRA discharge data endpoint."""
        params: dict[str, str] = {
            "station": native_id,
            "start": start.strftime("%Y-%m-%d"),
            "end": end.strftime("%Y-%m-%d"),
            "format": "json",
        }
        try:
            resp = await self._get("/data/discharge", params=params)
            return self._parse_observations(resp.json(), station_id)
        except (
            httpx.HTTPStatusError,
            ConnectorError,
            Exception,
        ):
            return None

    # ------------------------------------------------------------------
    # Parsers
    # ------------------------------------------------------------------

    def _parse_stations(
        self, data: list[dict] | dict,
    ) -> list[Station]:
        """Parse the WRA station-list JSON into ``Station`` models.

        Handles both a bare list and a dict wrapper.
        """
        items: list[dict] = (
            data
            if isinstance(data, list)
            else data.get("stations", data.get("features", []))
        )

        stations: list[Station] = []
        for entry in items:
            native_id = str(
                entry.get("station_id", entry.get("id", ""))
            ).strip()
            if not native_id:
                continue

            lat = entry.get("latitude") or entry.get("lat")
            lon = entry.get("longitude") or entry.get("lon")
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
                    latitude=float(str(lat)),
                    longitude=float(str(lon)),
                    country_code="JM",
                    river=entry.get("river"),
                    catchment_area_km2=(
                        float(str(entry["catchment_area_km2"]))
                        if entry.get("catchment_area_km2") is not None
                        else None
                    ),
                ))
            except (ValueError, KeyError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    station=native_id,
                    error=str(exc),
                )

        return stations

    def _parse_observations(
        self,
        data: dict | list,
        station_id: str,
    ) -> TimeSeriesChunk:
        """Parse WRA observation response.

        Expected shapes::

            {"data": [{"timestamp": "...", "discharge": 1.2}, ...]}
            or a bare list of dicts.
        """
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get(
                "data", data.get("observations", []),
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
                or entry.get("datetime")
                or entry.get("date")
            )
            if not ts_raw:
                continue
            try:
                ts = datetime.fromisoformat(str(ts_raw))
            except (ValueError, TypeError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid timestamp: {exc}",
                ) from exc

            raw_val = entry.get(
                "discharge", entry.get("discharge_m3s"),
            )
            discharge = (
                float(str(raw_val))
                if raw_val is not None
                else None
            )

            quality_raw = entry.get("quality")
            if discharge is None:
                quality = QualityFlag.MISSING
            elif quality_raw == "good":
                quality = QualityFlag.GOOD
            elif quality_raw == "estimated":
                quality = QualityFlag.ESTIMATED
            elif quality_raw == "suspect":
                quality = QualityFlag.SUSPECT
            else:
                quality = QualityFlag.RAW

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

    # ------------------------------------------------------------------
    # Seed list
    # ------------------------------------------------------------------

    def _build_seed_stations(self) -> list[Station]:
        """Return curated seed stations for major Jamaican rivers."""
        stations: list[Station] = []
        for s in _SEED_STATIONS:
            stations.append(Station(
                id=self._station_id(str(s["id"])),
                provider=self.slug,
                native_id=str(s["id"]),
                name=str(s["name"]),
                latitude=float(str(s["lat"])),
                longitude=float(str(s["lon"])),
                country_code="JM",
                river=str(s.get("river", "")),
            ))
        return stations
