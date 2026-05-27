"""Malaysia DID connector — Department of Irrigation and Drainage."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# Seed list of major Malaysian hydrological stations.
# Used when the DID Public Infobanjir portal is unreachable.
_SEED_STATIONS: list[dict[str, object]] = [
    {
        "id": "3527412",
        "name": "Temerloh",
        "river": "Pahang",
        "state": "Pahang",
        "lat": 3.45,
        "lon": 102.42,
    },
    {
        "id": "3628413",
        "name": "Lubok Paku",
        "river": "Pahang",
        "state": "Pahang",
        "lat": 3.62,
        "lon": 102.84,
    },
    {
        "id": "3924411",
        "name": "Pekan",
        "river": "Pahang",
        "state": "Pahang",
        "lat": 3.48,
        "lon": 103.39,
    },
    {
        "id": "5721442",
        "name": "Kuala Krai",
        "river": "Kelantan",
        "state": "Kelantan",
        "lat": 5.53,
        "lon": 102.20,
    },
    {
        "id": "5722443",
        "name": "Guillemard Bridge",
        "river": "Kelantan",
        "state": "Kelantan",
        "lat": 5.80,
        "lon": 102.15,
    },
    {
        "id": "5520441",
        "name": "Dabong",
        "river": "Galas (Kelantan)",
        "state": "Kelantan",
        "lat": 5.38,
        "lon": 101.96,
    },
    {
        "id": "4010401",
        "name": "Iskandar Bridge",
        "river": "Perak",
        "state": "Perak",
        "lat": 4.75,
        "lon": 100.73,
    },
    {
        "id": "4210402",
        "name": "Kuala Kangsar",
        "river": "Perak",
        "state": "Perak",
        "lat": 4.77,
        "lon": 100.94,
    },
    {
        "id": "4510403",
        "name": "Sungai Siput",
        "river": "Perak",
        "state": "Perak",
        "lat": 4.83,
        "lon": 101.07,
    },
    {
        "id": "1737451",
        "name": "Rantau Panjang",
        "river": "Johor",
        "state": "Johor",
        "lat": 1.77,
        "lon": 103.75,
    },
    {
        "id": "1836452",
        "name": "Kota Tinggi",
        "river": "Johor",
        "state": "Johor",
        "lat": 1.73,
        "lon": 103.90,
    },
    {
        "id": "2815421",
        "name": "Ampang",
        "river": "Klang",
        "state": "Selangor",
        "lat": 3.15,
        "lon": 101.75,
    },
    {
        "id": "2913422",
        "name": "Sulaiman Bridge",
        "river": "Klang",
        "state": "Selangor",
        "lat": 3.11,
        "lon": 101.68,
    },
    {
        "id": "5025461",
        "name": "Kuching",
        "river": "Sarawak",
        "state": "Sarawak",
        "lat": 1.55,
        "lon": 110.35,
    },
    {
        "id": "5125462",
        "name": "Batu Kitang",
        "river": "Sarawak",
        "state": "Sarawak",
        "lat": 1.53,
        "lon": 110.28,
    },
    {
        "id": "6040471",
        "name": "Beaufort",
        "river": "Padas",
        "state": "Sabah",
        "lat": 5.35,
        "lon": 115.75,
    },
    {
        "id": "6140472",
        "name": "Tenom",
        "river": "Padas",
        "state": "Sabah",
        "lat": 5.13,
        "lon": 115.95,
    },
    {
        "id": "5321481",
        "name": "Sibu",
        "river": "Rajang",
        "state": "Sarawak",
        "lat": 2.30,
        "lon": 111.83,
    },
    {
        "id": "5421482",
        "name": "Kapit",
        "river": "Rajang",
        "state": "Sarawak",
        "lat": 2.02,
        "lon": 112.93,
    },
    {
        "id": "4824431",
        "name": "Kuala Terengganu",
        "river": "Terengganu",
        "state": "Terengganu",
        "lat": 5.32,
        "lon": 103.13,
    },
    {
        "id": "4724432",
        "name": "Ajil",
        "river": "Terengganu",
        "state": "Terengganu",
        "lat": 5.10,
        "lon": 103.00,
    },
    {
        "id": "3425491",
        "name": "Arau",
        "river": "Perlis",
        "state": "Perlis",
        "lat": 6.43,
        "lon": 100.27,
    },
    {
        "id": "5410411",
        "name": "Alor Setar",
        "river": "Kedah",
        "state": "Kedah",
        "lat": 6.12,
        "lon": 100.37,
    },
    {
        "id": "2524441",
        "name": "Seremban",
        "river": "Linggi",
        "state": "Negeri Sembilan",
        "lat": 2.72,
        "lon": 101.94,
    },
    {
        "id": "2224451",
        "name": "Melaka",
        "river": "Melaka",
        "state": "Melaka",
        "lat": 2.19,
        "lon": 102.25,
    },
]


@register("malaysia_did")
class MalaysiaDIDConnector(BaseConnector):
    """Connector for Malaysia DID (Jabatan Pengairan dan Saliran).

    The DID Public Infobanjir portal provides water level and
    flow rate data.  This connector tries known URL patterns
    and falls back to a curated seed station list.
    """

    slug = "malaysia_did"
    display_name = "DID Malaysia (Public Infobanjir)"
    base_url = "https://publicinfobanjir.water.gov.my"
    country_codes = ["MY"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return hydrological stations from the DID portal.

        Tries remote endpoints; falls back to seed list.
        """
        stations = await self._try_fetch_stations_remote()
        if stations is not None:
            return stations

        logger.info(
            "using_seed_stations",
            provider=self.slug,
            reason="DID portal unreachable",
            count=len(_SEED_STATIONS),
        )
        return self._build_seed_stations()

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch flow rate observations for a station.

        Tries known endpoints on the Infobanjir portal; returns
        an empty chunk with guidance if unavailable.
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
                "DID flow rate data may require visiting "
                "https://publicinfobanjir.water.gov.my"
                " directly."
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
        """Fetch recent observations (last 7 days)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(days=7),
            end=now,
        )

    # ------------------------------------------------------------------
    # Remote station fetching
    # ------------------------------------------------------------------

    _STATION_PATHS = (
        "/aras-air/data-paras-air/",
        "/api/stations",
    )

    async def _try_fetch_stations_remote(
        self,
    ) -> list[Station] | None:
        """Try known endpoints for station listing."""
        for path in self._STATION_PATHS:
            try:
                resp = await self._get(
                    path,
                    params={
                        "state": "ALL",
                        "lang": "en",
                        "format": "json",
                    },
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
                ValueError,
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
        "/cerapan/kadar-alir/data-kadar-alir/",
        "/api/flow-rate",
    )

    async def _try_fetch_observations_remote(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk | None:
        """Try known endpoints for flow rate data."""
        for path in self._OBSERVATION_PATHS:
            try:
                resp = await self._get(
                    path,
                    params={
                        "station": native_id,
                        "state": "ALL",
                        "lang": "en",
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
                ValueError,
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
                    or entry.get("stationId")
                    or "",
                ).strip()
                if not native_id:
                    continue

                name = str(
                    entry.get("station_name")
                    or entry.get("name")
                    or entry.get("stationName")
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
                        country_code="MY",
                        river=entry.get("river_name")
                        or entry.get("river")
                        or entry.get("sungai"),
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
                    country_code="MY",
                    river=str(seed.get("river", "")),
                ),
            )
        return stations

    def _parse_observations(
        self,
        data: dict | list,
        station_id: str,
    ) -> TimeSeriesChunk:
        """Parse observation/flow-rate data from JSON."""
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
                    or entry.get("flow_rate")
                    or entry.get("kadar_alir")
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

        for fmt in (
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%Y-%m-%d %H:%M:%S",
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
