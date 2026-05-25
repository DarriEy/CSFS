"""Philippines DPWH connector — Department of Public Works and Highways."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# Seed list of major Philippine river discharge stations.
# Used when the DPWH streams portal is unreachable (it is an
# ASP.NET site with no documented JSON API).
_SEED_STATIONS: list[dict[str, object]] = [
    {
        "id": "5654300",
        "name": "Pangal",
        "river": "Cagayan River",
        "lat": 16.60,
        "lon": 121.68,
    },
    {
        "id": "5654140",
        "name": "Bumagcat",
        "river": "Abra River",
        "lat": 17.62,
        "lon": 120.73,
    },
    {
        "id": "5654500",
        "name": "San Agustin",
        "river": "Pampanga River",
        "lat": 15.17,
        "lon": 120.78,
    },
    {
        "id": "5654400",
        "name": "Carmen",
        "river": "Agno River",
        "lat": 15.90,
        "lon": 120.60,
    },
    {
        "id": "5660400",
        "name": "Lapulabao",
        "river": "Padada River",
        "lat": 6.66,
        "lon": 125.28,
    },
]


@register("philippines_dpwh")
class PhilippinesDPWHConnector(BaseConnector):
    """Connector for Philippines DPWH Bureau of Design stream data.

    The DPWH streams portal is an ASP.NET WebForms site with no
    documented JSON API.  This connector attempts known URL patterns
    and falls back to a curated seed station list.
    """

    slug = "philippines_dpwh"
    display_name = "DPWH Philippines (Bureau of Design)"
    base_url = "https://apps.dpwh.gov.ph/streams_public"
    country_codes = ["PH"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return discharge stations from the DPWH portal.

        Tries remote endpoints; falls back to seed list.
        """
        stations = await self._try_fetch_stations_remote()
        if stations is not None:
            return stations

        logger.info(
            "using_seed_stations",
            provider=self.slug,
            reason="DPWH ASP.NET portal unreachable",
            count=len(_SEED_STATIONS),
        )
        return self._build_seed_stations()

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for a station.

        Tries API-like endpoints; returns an empty chunk with a
        log message when endpoints are unreachable (expected for
        this ASP.NET site).
        """
        native_id = station_id.removeprefix(
            f"{self.slug}:",
        )

        chunk = await self._try_fetch_observations_remote(
            native_id, station_id, start, end,
        )
        if chunk is not None:
            return chunk

        logger.warning(
            "observations_unavailable",
            provider=self.slug,
            station=native_id,
            hint=(
                "DPWH streams portal is ASP.NET-based "
                "with no public JSON API. Visit "
                "https://apps.dpwh.gov.ph/streams_public"
                " for manual data access."
            ),
        )
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )

    async def fetch_latest(
        self, station_id: str,
    ) -> TimeSeriesChunk:
        """Fetch recent observations (last 30 days)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(days=30),
            end=now,
        )

    # ------------------------------------------------------------------
    # Remote station fetching
    # ------------------------------------------------------------------

    _STATION_PATHS = (
        "/station_public.aspx",
        "/api/stations",
    )

    async def _try_fetch_stations_remote(
        self,
    ) -> list[Station] | None:
        """Try known endpoints for station listing."""
        for path in self._STATION_PATHS:
            try:
                resp = await self._get(
                    path, params={"format": "json"},
                )
                data = resp.json()
                if not isinstance(data, list):
                    if isinstance(data, dict):
                        data = (
                            data.get("data")
                            or data.get("stations")
                            or data.get("results", [])
                        )
                    if not isinstance(data, list):
                        logger.debug(
                            "stations_unexpected_format",
                            provider=self.slug,
                            path=path,
                        )
                        continue
                parsed = self._parse_stations_json(data)
                if parsed:
                    return parsed
            except (
                ConnectorError,
                httpx.HTTPStatusError,
                httpx.HTTPError,
            ) as exc:
                logger.debug(
                    "station_endpoint_failed",
                    provider=self.slug,
                    path=path,
                    error=str(exc),
                )
                continue
        return None

    # ------------------------------------------------------------------
    # Remote observation fetching
    # ------------------------------------------------------------------

    _OBSERVATION_PATHS = (
        "/station_summary.aspx",
        "/api/observations",
    )

    async def _try_fetch_observations_remote(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk | None:
        """Try known endpoints for observation data."""
        for path in self._OBSERVATION_PATHS:
            try:
                resp = await self._get(
                    path,
                    params={
                        "station": native_id,
                        "start": start.strftime("%Y-%m-%d"),
                        "end": end.strftime("%Y-%m-%d"),
                        "format": "json",
                    },
                )
                data = resp.json()
                return self._parse_observations(
                    data, station_id,
                )
            except (
                ConnectorError,
                httpx.HTTPStatusError,
                httpx.HTTPError,
            ) as exc:
                logger.debug(
                    "observation_endpoint_failed",
                    provider=self.slug,
                    path=path,
                    station=native_id,
                    error=str(exc),
                )
                continue
        return None

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    def _parse_stations_json(
        self, data: list[dict],
    ) -> list[Station]:
        """Parse station entries from JSON."""
        stations: list[Station] = []
        for entry in data:
            try:
                native_id = str(
                    entry.get("station_id")
                    or entry.get("id")
                    or "",
                ).strip()
                if not native_id:
                    continue

                name = str(
                    entry.get("station_name")
                    or entry.get("name")
                    or "",
                )
                lat = self._safe_float(
                    entry.get("latitude")
                    or entry.get("lat"),
                )
                lon = self._safe_float(
                    entry.get("longitude")
                    or entry.get("lon"),
                )

                stations.append(
                    Station(
                        id=self._station_id(native_id),
                        provider=self.slug,
                        native_id=native_id,
                        name=name,
                        latitude=lat or 0.0,
                        longitude=lon or 0.0,
                        country_code="PH",
                        river=entry.get("river_name")
                        or entry.get("river"),
                    ),
                )
            except (
                ValueError, KeyError, TypeError,
            ) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    entry=str(entry)[:200],
                    error=str(exc),
                )
                continue
        return stations

    def _build_seed_stations(self) -> list[Station]:
        """Build Station models from the hardcoded seed list."""
        stations: list[Station] = []
        for seed in _SEED_STATIONS:
            sid = str(seed["id"])
            stations.append(
                Station(
                    id=self._station_id(sid),
                    provider=self.slug,
                    native_id=sid,
                    name=str(seed["name"]),
                    latitude=float(str(seed["lat"])),
                    longitude=float(str(seed["lon"])),
                    country_code="PH",
                    river=str(seed.get("river", "")),
                ),
            )
        return stations

    def _parse_observations(
        self,
        data: dict | list,
        station_id: str,
    ) -> TimeSeriesChunk:
        """Parse observation data from JSON response."""
        obs_list: list[dict] = []
        if isinstance(data, dict):
            obs_list = (
                data.get("data")
                or data.get("observations")
                or data.get("results", [])
            )
        elif isinstance(data, list):
            obs_list = data
        if not isinstance(obs_list, list):
            obs_list = []

        observations: list[Observation] = []
        for entry in obs_list:
            try:
                ts = self._parse_timestamp(entry)
                if ts is None:
                    continue

                raw_value = (
                    entry.get("value")
                    or entry.get("discharge")
                )
                discharge = self._safe_float(raw_value)

                observations.append(
                    Observation(
                        station_id=station_id,
                        timestamp=ts,
                        discharge_m3s=discharge,
                        quality=(
                            QualityFlag.RAW
                            if discharge is not None
                            else QualityFlag.MISSING
                        ),
                    ),
                )
            except (ValueError, TypeError) as exc:
                logger.warning(
                    "observation_parse_failed",
                    provider=self.slug,
                    entry=str(entry)[:200],
                    error=str(exc),
                )
                continue

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    def _parse_timestamp(
        self, entry: dict,
    ) -> datetime | None:
        """Try multiple date field names and formats."""
        raw = (
            entry.get("date")
            or entry.get("timestamp")
            or entry.get("dateTime")
        )
        if raw is None:
            return None

        raw_str = str(raw).strip()
        if not raw_str:
            return None

        try:
            return datetime.fromisoformat(raw_str)
        except ValueError:
            pass

        for fmt in (
            "%Y-%m-%d",
            "%m/%d/%Y",
            "%d-%m-%Y",
        ):
            try:
                return datetime.strptime(raw_str, fmt)
            except ValueError:
                continue

        logger.warning(
            "timestamp_parse_failed",
            provider=self.slug,
            raw=raw_str,
        )
        return None

    @staticmethod
    def _safe_float(
        value: object,
    ) -> float | None:
        """Safely convert a value to float."""
        if value is None:
            return None
        try:
            return float(str(value))
        except (TypeError, ValueError):
            return None
