"""Iran IWRMC connector — Iran Water Resources Management Company via stu.wrm.ir."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# Seed list of major Iranian river gauging stations.
# Used as fallback when the IWRMC portal is unreachable or
# registration-gated.  Coordinates are approximate centroids
# of well-known hydrometric stations on each river.
_SEED_STATIONS: list[dict[str, object]] = [
    {
        "code": "21-001",
        "name": "Ahvaz (Band-e Ghir)",
        "river": "Karun",
        "basin": "Persian Gulf",
        "lat": 31.32,
        "lon": 48.67,
    },
    {
        "code": "21-003",
        "name": "Gotvand",
        "river": "Karun",
        "basin": "Persian Gulf",
        "lat": 32.25,
        "lon": 48.82,
    },
    {
        "code": "22-001",
        "name": "Hamidieh",
        "river": "Karkheh",
        "basin": "Persian Gulf",
        "lat": 31.48,
        "lon": 48.43,
    },
    {
        "code": "22-003",
        "name": "Payepol",
        "river": "Karkheh",
        "basin": "Persian Gulf",
        "lat": 32.39,
        "lon": 48.18,
    },
    {
        "code": "23-001",
        "name": "Dez Dam",
        "river": "Dez",
        "basin": "Persian Gulf",
        "lat": 32.60,
        "lon": 48.48,
    },
    {
        "code": "24-001",
        "name": "Chadegan",
        "river": "Zayandeh-Rud",
        "basin": "Gavkhouni",
        "lat": 32.77,
        "lon": 50.63,
    },
    {
        "code": "24-002",
        "name": "Pol-e Zamankhan",
        "river": "Zayandeh-Rud",
        "basin": "Gavkhouni",
        "lat": 32.65,
        "lon": 51.67,
    },
    {
        "code": "11-001",
        "name": "Gorganrud Bridge",
        "river": "Atrak",
        "basin": "Caspian Sea",
        "lat": 37.24,
        "lon": 55.17,
    },
    {
        "code": "12-001",
        "name": "Namin",
        "river": "Aras",
        "basin": "Caspian Sea",
        "lat": 38.42,
        "lon": 48.48,
    },
    {
        "code": "12-003",
        "name": "Pol-e Dasht",
        "river": "Aras",
        "basin": "Caspian Sea",
        "lat": 39.26,
        "lon": 45.45,
    },
    {
        "code": "13-001",
        "name": "Astaneh",
        "river": "Sefid-Rud",
        "basin": "Caspian Sea",
        "lat": 37.27,
        "lon": 49.94,
    },
    {
        "code": "13-003",
        "name": "Manjil Dam",
        "river": "Sefid-Rud",
        "basin": "Caspian Sea",
        "lat": 36.73,
        "lon": 49.40,
    },
    {
        "code": "41-001",
        "name": "Baft",
        "river": "Halil-Rud",
        "basin": "Hamoun-e Jaz Murian",
        "lat": 29.23,
        "lon": 56.60,
    },
    {
        "code": "41-003",
        "name": "Jiroft",
        "river": "Halil-Rud",
        "basin": "Hamoun-e Jaz Murian",
        "lat": 28.67,
        "lon": 57.74,
    },
    {
        "code": "25-001",
        "name": "Doroudzan Dam",
        "river": "Kor",
        "basin": "Bakhtegan",
        "lat": 30.27,
        "lon": 52.40,
    },
    {
        "code": "25-003",
        "name": "Khan Zenyan",
        "river": "Kor",
        "basin": "Bakhtegan",
        "lat": 29.73,
        "lon": 52.50,
    },
    {
        "code": "51-001",
        "name": "Dugharon",
        "river": "Helmand",
        "basin": "Hamoun",
        "lat": 34.35,
        "lon": 61.65,
    },
    {
        "code": "14-001",
        "name": "Babol",
        "river": "Babolrud",
        "basin": "Caspian Sea",
        "lat": 36.55,
        "lon": 52.68,
    },
    {
        "code": "14-003",
        "name": "Sari",
        "river": "Tajan",
        "basin": "Caspian Sea",
        "lat": 36.56,
        "lon": 53.06,
    },
    {
        "code": "31-001",
        "name": "Zanjan Bridge",
        "river": "Qezel Owzan",
        "basin": "Sefirud",
        "lat": 36.66,
        "lon": 48.50,
    },
    {
        "code": "31-003",
        "name": "Sanandaj",
        "river": "Qezel Owzan",
        "basin": "Sefirud",
        "lat": 35.31,
        "lon": 47.00,
    },
    {
        "code": "15-001",
        "name": "Gonbad-e Kavus",
        "river": "Gorgan-Rud",
        "basin": "Caspian Sea",
        "lat": 37.25,
        "lon": 55.17,
    },
    {
        "code": "16-001",
        "name": "Minab",
        "river": "Minab",
        "basin": "Strait of Hormuz",
        "lat": 27.10,
        "lon": 57.08,
    },
    {
        "code": "32-001",
        "name": "Sonqor",
        "river": "Gamasiab",
        "basin": "Karkheh",
        "lat": 34.78,
        "lon": 47.60,
    },
    {
        "code": "33-001",
        "name": "Kashkan Bridge",
        "river": "Kashkan",
        "basin": "Karkheh",
        "lat": 33.53,
        "lon": 48.17,
    },
    {
        "code": "34-001",
        "name": "Sardasht",
        "river": "Zarrineh-Rud",
        "basin": "Lake Urmia",
        "lat": 36.15,
        "lon": 45.50,
    },
    {
        "code": "34-003",
        "name": "Miandoab",
        "river": "Zarrineh-Rud",
        "basin": "Lake Urmia",
        "lat": 36.97,
        "lon": 46.10,
    },
    {
        "code": "35-001",
        "name": "Shapur Bridge",
        "river": "Shapur",
        "basin": "Persian Gulf",
        "lat": 29.78,
        "lon": 51.55,
    },
    {
        "code": "36-001",
        "name": "Shirvan",
        "river": "Kalshur",
        "basin": "Caspian Sea",
        "lat": 37.39,
        "lon": 57.92,
    },
    {
        "code": "37-001",
        "name": "Tabriz",
        "river": "Aji Chay",
        "basin": "Lake Urmia",
        "lat": 38.08,
        "lon": 46.30,
    },
]


@register("iran_iwrmc")
class IranIWRMCConnector(BaseConnector):
    """Connector for Iran Water Resources Management Company (IWRMC).

    The portal at stu.wrm.ir is Persian-language and typically
    registration-gated.  This connector tries known API-like
    endpoints first, then falls back to a curated seed list of
    major Iranian hydrometric stations.
    """

    slug = "iran_iwrmc"
    display_name = "IWRMC Iran (stu.wrm.ir)"
    base_url = "https://stu.wrm.ir"
    country_codes = ["IR"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return discharge stations from the IWRMC portal.

        Tries remote endpoints first; falls back to a curated seed
        list so that downstream code always has station metadata.
        """
        stations = await self._try_fetch_stations_remote()
        if stations is not None:
            return stations

        logger.info(
            "using_seed_stations",
            provider=self.slug,
            reason="remote endpoints unavailable",
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
        log message pointing users to stu.wrm.ir for manual data
        requests when the endpoints are unreachable.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

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
                "Data may require registration at "
                "https://stu.wrm.ir — submit a manual request "
                "for station data."
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
        """Fetch recent observations (last 365 days)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(days=365),
            end=now,
        )

    # ------------------------------------------------------------------
    # Remote station fetching
    # ------------------------------------------------------------------

    _STATION_PATHS = (
        "/amar/istgah_list.asp",
        "/api/stations",
    )

    async def _try_fetch_stations_remote(
        self,
    ) -> list[Station] | None:
        """Try known API endpoints for station listing."""
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
        "/amar/data.asp",
        "/api/observations",
    )

    async def _try_fetch_observations_remote(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk | None:
        """Try known API endpoints for observation data."""
        for path in self._OBSERVATION_PATHS:
            try:
                resp = await self._get(
                    path,
                    params={
                        "station": native_id,
                        "variable": "discharge",
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
        """Parse station entries from JSON into Station models."""
        stations: list[Station] = []
        for entry in data:
            try:
                native_id = str(
                    entry.get("station_code")
                    or entry.get("code")
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
                        country_code="IR",
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
            code = str(seed["code"])
            stations.append(
                Station(
                    id=self._station_id(code),
                    provider=self.slug,
                    native_id=code,
                    name=str(seed["name"]),
                    latitude=float(str(seed["lat"])),
                    longitude=float(str(seed["lon"])),
                    country_code="IR",
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
                    or entry.get("debi")
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
            or entry.get("tarikh")
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

        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d-%m-%Y"):
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
