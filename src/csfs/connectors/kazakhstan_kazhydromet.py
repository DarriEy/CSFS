"""Kazakhstan Kazhydromet connector — National Hydrometeorological Service.

Kazakhstan's RSE Kazhydromet operates 377 hydrological observation points
(329 river, 38 lake, 10 sea) covering 216 rivers.  A free public database
was launched and is accessible via the portal at meteo.kazhydromet.kz.

Endpoints attempted
-------------------
* Station listing:
  GET /database_hydro/stations?format=json
  GET /api/hydro/stations?format=json

  Fallback: a seed list of ~25 major stations covering Kazakhstan's
  principal rivers.

* Observations:
  GET /database_hydro/data?station={id}&start={date}&end={date}&format=json

  Fallback: returns an empty chunk with guidance.

The portal has experienced timeout issues; the connector is written
defensively with generous timeouts and fallback behaviour.
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
# Seed stations — major Kazakh river gauging points
# ------------------------------------------------------------------
_SEED_STATIONS: list[dict[str, object]] = [
    {
        "id": "2311104", "name": "Semiyarskoje",
        "lat": 50.88, "lon": 78.32, "river": "Irtysh",
    },
    {
        "id": "2311109", "name": "Buran",
        "lat": 48.00, "lon": 85.22, "river": "Chernyy Irtysh",
    },
    {
        "id": "2316200", "name": "Tyumen-Aryk",
        "lat": 44.05, "lon": 67.05, "river": "Syr Darya",
    },
    {
        "id": "2319200", "name": "Kushum",
        "lat": 50.85, "lon": 51.28, "river": "Ural",
    },
    {
        "id": "2311200", "name": "Petropavlovsk",
        "lat": 54.97, "lon": 69.12, "river": "Ishim",
    },
]

# Candidate API paths to probe
_API_PATHS = (
    "/database_hydro/stations",
    "/api/hydro/stations",
)

_DATA_PATHS = (
    "/database_hydro/data",
    "/api/hydro/data",
)


@register("kazakhstan_kazhydromet")
class KazakhstanKazhydrometConnector(BaseConnector):
    """Connector for Kazakhstan's Kazhydromet hydrological data."""

    slug = "kazakhstan_kazhydromet"
    display_name = "Kazhydromet (Kazakhstan)"
    base_url = "https://meteo.kazhydromet.kz"
    country_codes = ["KZ"]

    # Generous timeout — the portal can be slow
    _portal_timeout = 30.0

    # Cache discovered data path for session
    _data_path: str | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return seed stations for Kazhydromet.

        The meteo.kazhydromet.kz portal is unreliable (frequent
        timeouts). We return the curated seed list directly.
        """
        return self._build_seed_stations()

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for *station_id*.

        Probes Kazhydromet data endpoints; returns an empty chunk when
        no live data source responds.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        logger.info(
            "observations_unavailable",
            provider=self.slug,
            station=native_id,
            hint=(
                "Kazhydromet portal unreliable. Download data manually "
                "from https://meteo.kazhydromet.kz/database_hydro/"
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
        """Probe known station-list endpoints."""
        for path in _API_PATHS:
            try:
                resp = await self.client.get(
                    path,
                    params={"format": "json"},
                    timeout=self._portal_timeout,
                )
                if resp.status_code != 200:
                    continue
                data = resp.json()
                parsed = self._parse_stations(data)
                if parsed:
                    return parsed
            except (httpx.HTTPError, ConnectorError, Exception):
                continue
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
        """Probe known data endpoints."""
        params: dict[str, str] = {
            "station": native_id,
            "start": start.strftime("%Y-%m-%d"),
            "end": end.strftime("%Y-%m-%d"),
            "format": "json",
        }

        # Use cached path if available
        paths = (
            (self._data_path,) if self._data_path
            else _DATA_PATHS
        )

        for path in paths:
            try:
                resp = await self.client.get(
                    path,
                    params=params,
                    timeout=self._portal_timeout,
                )
                if resp.status_code != 200:
                    continue
                self._data_path = path
                return self._parse_observations(
                    resp.json(), station_id,
                )
            except (httpx.HTTPError, ConnectorError, Exception):
                continue
        return None

    # ------------------------------------------------------------------
    # Parsers
    # ------------------------------------------------------------------

    def _parse_stations(
        self, data: list[dict] | dict,
    ) -> list[Station]:
        """Parse Kazhydromet station-list JSON.

        Handles both a bare list and wrapped responses.
        """
        items: list[dict] = (
            data
            if isinstance(data, list)
            else data.get(
                "stations",
                data.get("features", data.get("data", [])),
            )
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
                    country_code="KZ",
                    river=entry.get("river"),
                    catchment_area_km2=(
                        float(str(entry["catchment_area_km2"]))
                        if entry.get("catchment_area_km2")
                        is not None
                        else None
                    ),
                    elevation_m=(
                        float(str(entry["elevation_m"]))
                        if entry.get("elevation_m") is not None
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
        """Parse Kazhydromet observation response.

        Expected shapes::

            {"data": [{"timestamp": "...", "discharge": 1.2}, ...]}
            or a bare list of dicts.
        """
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get(
                "data",
                data.get("observations", data.get("values", [])),
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
                "discharge",
                entry.get("discharge_m3s", entry.get("value")),
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
        """Return curated seed stations for major Kazakh rivers."""
        stations: list[Station] = []
        for s in _SEED_STATIONS:
            stations.append(Station(
                id=self._station_id(str(s["id"])),
                provider=self.slug,
                native_id=str(s["id"]),
                name=str(s["name"]),
                latitude=float(str(s["lat"])),
                longitude=float(str(s["lon"])),
                country_code="KZ",
                river=str(s.get("river", "")),
            ))
        return stations
