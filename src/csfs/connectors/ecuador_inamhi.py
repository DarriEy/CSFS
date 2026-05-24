"""Ecuador INAMHI connector via GEOGloWS ECMWF Streamflow Services."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# GEOGloWS API paths.
_PATH_HISTORIC = "/HistoricSimulation/"
_PATH_FORECAST = "/ForecastStats/"

# Curated seed list of major Ecuadorian river gauging points.
# Each entry maps a human-readable name to its GEOGloWS COMID
# (reach identifier), approximate coordinates, and river name.
_ECUADOR_SEED_STATIONS: list[dict] = [
    {
        "comid": "9027406",
        "name": "Guayas at Daule",
        "lat": -1.86,
        "lon": -79.97,
        "river": "Guayas",
    },
    {
        "comid": "9027814",
        "name": "Guayas at Babahoyo",
        "lat": -1.80,
        "lon": -79.53,
        "river": "Guayas",
    },
    {
        "comid": "9028190",
        "name": "Guayas at Vinces",
        "lat": -1.55,
        "lon": -79.75,
        "river": "Guayas",
    },
    {
        "comid": "9034764",
        "name": "Napo at Francisco de Orellana",
        "lat": -0.47,
        "lon": -76.97,
        "river": "Napo",
    },
    {
        "comid": "9034200",
        "name": "Napo at Tena",
        "lat": -1.00,
        "lon": -77.81,
        "river": "Napo",
    },
    {
        "comid": "9035100",
        "name": "Napo at Nuevo Rocafuerte",
        "lat": -0.92,
        "lon": -75.39,
        "river": "Napo",
    },
    {
        "comid": "9031500",
        "name": "Pastaza at Banos",
        "lat": -1.39,
        "lon": -78.42,
        "river": "Pastaza",
    },
    {
        "comid": "9031900",
        "name": "Pastaza at Shell",
        "lat": -1.50,
        "lon": -78.06,
        "river": "Pastaza",
    },
    {
        "comid": "9032700",
        "name": "Pastaza at Copataza",
        "lat": -2.13,
        "lon": -76.88,
        "river": "Pastaza",
    },
    {
        "comid": "9036400",
        "name": "Santiago at Santiago",
        "lat": -3.05,
        "lon": -78.35,
        "river": "Santiago",
    },
    {
        "comid": "9036800",
        "name": "Santiago at Yantzaza",
        "lat": -3.83,
        "lon": -78.76,
        "river": "Santiago",
    },
    {
        "comid": "9037200",
        "name": "Santiago at Yaupi",
        "lat": -3.11,
        "lon": -77.94,
        "river": "Santiago",
    },
    {
        "comid": "9025100",
        "name": "Esmeraldas at Quininde",
        "lat": 0.33,
        "lon": -79.47,
        "river": "Esmeraldas",
    },
    {
        "comid": "9025500",
        "name": "Esmeraldas at Rosa Zarate",
        "lat": 0.33,
        "lon": -79.48,
        "river": "Esmeraldas",
    },
    {
        "comid": "9025900",
        "name": "Esmeraldas at Esmeraldas",
        "lat": 0.96,
        "lon": -79.65,
        "river": "Esmeraldas",
    },
    {
        "comid": "9029400",
        "name": "Jubones at Pasaje",
        "lat": -3.33,
        "lon": -79.81,
        "river": "Jubones",
    },
    {
        "comid": "9030100",
        "name": "Chone at Chone",
        "lat": -0.69,
        "lon": -80.10,
        "river": "Chone",
    },
    {
        "comid": "9033400",
        "name": "Curaray at Curaray",
        "lat": -1.38,
        "lon": -76.95,
        "river": "Curaray",
    },
    {
        "comid": "9026300",
        "name": "Mira at San Lorenzo",
        "lat": 1.28,
        "lon": -78.84,
        "river": "Mira",
    },
    {
        "comid": "9038100",
        "name": "Zamora at Zamora",
        "lat": -4.07,
        "lon": -78.96,
        "river": "Zamora",
    },
]


@register("ecuador_inamhi")
class EcuadorINAMHIConnector(BaseConnector):
    """Connector for Ecuador streamflow data via GEOGloWS ECMWF API.

    GEOGloWS provides gridded hydrological simulations indexed by river
    reach COMID rather than traditional gauging stations. This connector
    exposes a curated set of major Ecuadorian river reaches as virtual
    stations and fetches historic simulation data from the GEOGloWS REST
    API.
    """

    slug = "ecuador_inamhi"
    display_name = "Ecuador INAMHI (GEOGloWS)"
    base_url = "https://geoglows.ecmwf.int/api"
    country_codes = ["EC"]

    # Alternate Tethys portal URL for future fallback.
    _alt_base_url = "https://inamhi.geoglows.org/api"

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        self._use_tethys: bool | None = None  # None = not yet probed

    async def __aenter__(self) -> EcuadorINAMHIConnector:
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(90.0, connect=15.0),
            headers={
                "User-Agent": "CSFS/0.1 (https://github.com/csfs)",
                "Accept": "application/json",
            },
            follow_redirects=True,
        )
        return self

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return the curated list of Ecuadorian river reaches."""
        stations: list[Station] = []
        for entry in _ECUADOR_SEED_STATIONS:
            comid = str(entry["comid"]).strip()
            lat = self._to_float(entry.get("lat"))
            lon = self._to_float(entry.get("lon"))
            if not comid or lat is None or lon is None:
                continue

            stations.append(Station(
                id=self._station_id(comid),
                provider=self.slug,
                native_id=comid,
                name=entry.get("name", comid),
                latitude=lat,
                longitude=lon,
                country_code="EC",
                river=entry.get("river"),
                is_active=True,
            ))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch historic simulation data for a river reach."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        if self._use_tethys is True:
            return await self._fetch_observations_tethys(
                native_id, station_id, start, end,
            )

        try:
            return await self._fetch_observations_geoglows(
                native_id, station_id, start, end,
            )
        except DataFormatError:
            raise
        except (ConnectorError, httpx.HTTPStatusError):
            logger.info(
                "ecuador_inamhi.geoglows_failed_trying_tethys",
                station=native_id,
            )
            return await self._fetch_observations_tethys(
                native_id, station_id, start, end,
            )

    async def fetch_forecast(
        self,
        station_id: str,
    ) -> TimeSeriesChunk:
        """Fetch forecast statistics for a river reach."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        resp = await self._get(_PATH_FORECAST, params={
            "reach_id": native_id,
            "return_format": "json",
        })
        data = self._json_or_raise(resp)
        return self._parse_forecast(data, station_id)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent 24 h of simulated observations."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # ------------------------------------------------------------------
    # GEOGloWS API helpers
    # ------------------------------------------------------------------

    async def _fetch_observations_geoglows(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        resp = await self._get(_PATH_HISTORIC, params={
            "reach_id": native_id,
            "return_format": "json",
        })
        data = self._json_or_raise(resp)
        self._use_tethys = False
        return self._parse_historic(data, station_id, start, end)

    # ------------------------------------------------------------------
    # Tethys fallback helpers
    # ------------------------------------------------------------------

    async def _get_tethys(
        self,
        path: str,
        params: dict | None = None,
    ) -> httpx.Response:
        """Issue a GET against the alternate Tethys portal."""
        url = self._alt_base_url + path
        resp = await self.client.get(url, params=params)
        if resp.status_code == 429:
            from csfs.core.exceptions import RateLimitError

            raise RateLimitError(self.slug, "Rate limited")
        if resp.status_code not in (200, 206):
            resp.raise_for_status()
        return resp

    async def _fetch_observations_tethys(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        resp = await self._get_tethys(
            "/HistoricSimulation/",
            params={
                "reach_id": native_id,
                "return_format": "json",
            },
        )
        data = self._json_or_raise(resp)
        self._use_tethys = True
        return self._parse_historic(data, station_id, start, end)

    # ------------------------------------------------------------------
    # Parsers
    # ------------------------------------------------------------------

    def _parse_historic(
        self,
        data: dict | list,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Parse historic simulation JSON into observations.

        GEOGloWS returns the full historical record; we filter to the
        requested time window.
        """
        raw_obs: list[dict] = self._extract_records(data)

        observations: list[Observation] = []
        for item in raw_obs:
            try:
                ts = self._parse_timestamp(
                    item.get("datetime", item.get("date", "")),
                )
                if ts is None:
                    continue
                # Filter to requested window.
                if ts < start.replace(tzinfo=UTC) or ts > end.replace(
                    tzinfo=UTC,
                ):
                    continue

                discharge = self._to_float(
                    item.get(
                        "streamflow_m3s",
                        item.get("flow", item.get("value")),
                    ),
                )
                if discharge is None:
                    continue

                observations.append(Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=QualityFlag.ESTIMATED,
                ))
            except (ValueError, TypeError) as exc:
                logger.debug(
                    "ecuador_inamhi.skipping_observation",
                    error=str(exc),
                )
                continue

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    def _parse_forecast(
        self,
        data: dict | list,
        station_id: str,
    ) -> TimeSeriesChunk:
        """Parse forecast statistics JSON into observations."""
        raw_obs: list[dict] = self._extract_records(data)

        observations: list[Observation] = []
        for item in raw_obs:
            try:
                ts = self._parse_timestamp(
                    item.get("datetime", item.get("date", "")),
                )
                if ts is None:
                    continue

                discharge = self._to_float(
                    item.get(
                        "flow_avg",
                        item.get("streamflow_m3s", item.get("value")),
                    ),
                )
                if discharge is None:
                    continue

                observations.append(Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=QualityFlag.ESTIMATED,
                ))
            except (ValueError, TypeError) as exc:
                logger.debug(
                    "ecuador_inamhi.skipping_forecast",
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
    # Shared utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_records(data: dict | list) -> list[dict]:
        """Pull the observation list out of various JSON shapes."""
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("data", "records", "time_series", "results"):
                val = data.get(key)
                if isinstance(val, list):
                    return val
            for v in data.values():
                if isinstance(v, list):
                    return v
        return []

    def _json_or_raise(self, resp: httpx.Response) -> dict | list:
        """Decode JSON from *resp*, raising DataFormatError on failure."""
        try:
            result: dict | list = resp.json()
            return result
        except Exception as exc:
            raise DataFormatError(
                self.slug, f"Invalid JSON in response: {exc}",
            ) from exc

    @staticmethod
    def _parse_timestamp(value: str) -> datetime | None:
        """Parse an ISO-ish timestamp string into a UTC datetime."""
        if not value or not value.strip():
            return None
        text = value.strip()
        for fmt in (
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d",
        ):
            try:
                dt = datetime.strptime(text, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                return dt
            except ValueError:
                continue
        return None

    @staticmethod
    def _to_float(value: object) -> float | None:
        """Coerce *value* to float, returning None for absent values."""
        if value is None:
            return None
        try:
            return float(str(value))
        except (ValueError, TypeError):
            return None
