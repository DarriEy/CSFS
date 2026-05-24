"""India CWC / WRIS connector — Central Water Commission gauging stations."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# Quality mapping from WRIS quality strings to CSFS flags
_QUALITY_MAP: dict[str, QualityFlag] = {
    "good": QualityFlag.GOOD,
    "suspect": QualityFlag.SUSPECT,
    "estimated": QualityFlag.ESTIMATED,
    "missing": QualityFlag.MISSING,
}


@register("india_cwc")
class IndiaCWCConnector(BaseConnector):
    """Connector for India's Central Water Commission Water Resources Information System."""

    slug = "india_cwc"
    display_name = "CWC India (WRIS)"
    base_url = "https://indiawris.gov.in/api"
    country_codes = ["IN"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all surface-water gauging stations from WRIS.

        Tries the primary endpoint first; falls back to alternative if it fails.
        """
        # Try primary endpoint
        stations = await self._try_fetch_stations_primary()
        if stations is not None:
            return stations

        # Fallback to alternative endpoint
        stations = await self._try_fetch_stations_fallback()
        if stations is not None:
            return stations

        logger.warning("fetch_stations_failed_all_endpoints", provider=self.slug)
        return []

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for a station over a time range.

        Tries the primary endpoint first; falls back to alternative if it fails.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        # Try primary endpoint
        chunk = await self._try_fetch_observations_primary(native_id, station_id, start, end)
        if chunk is not None:
            return chunk

        # Fallback to alternative endpoint
        chunk = await self._try_fetch_observations_fallback(native_id, station_id, start, end)
        if chunk is not None:
            return chunk

        logger.warning(
            "fetch_observations_failed_all_endpoints",
            provider=self.slug,
            station=native_id,
        )
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent observations (last 24 h)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # ------------------------------------------------------------------
    # Station fetching internals
    # ------------------------------------------------------------------

    async def _try_fetch_stations_primary(self) -> list[Station] | None:
        """Try GET /stations?type=SW endpoint."""
        try:
            resp = await self._get("/stations", params={"type": "SW"})
            data = resp.json()
            if not isinstance(data, list):
                logger.warning(
                    "stations_unexpected_format",
                    provider=self.slug,
                    endpoint="primary",
                    type=type(data).__name__,
                )
                return None
            return self._parse_stations(data)
        except (ConnectorError, httpx.HTTPStatusError, httpx.HTTPError) as exc:
            logger.warning(
                "stations_primary_endpoint_failed",
                provider=self.slug,
                error=str(exc),
            )
            return None

    async def _try_fetch_stations_fallback(self) -> list[Station] | None:
        """Try GET /SubInfo/getGaugeStation endpoint."""
        try:
            resp = await self._get("/SubInfo/getGaugeStation")
            data = resp.json()
            if not isinstance(data, list):
                # Some endpoints wrap results in a dict
                if isinstance(data, dict):
                    data = data.get("data") or data.get("stations") or data.get("results", [])
                if not isinstance(data, list):
                    logger.warning(
                        "stations_unexpected_format",
                        provider=self.slug,
                        endpoint="fallback",
                        type=type(data).__name__,
                    )
                    return None
            return self._parse_stations(data)
        except (ConnectorError, httpx.HTTPStatusError, httpx.HTTPError) as exc:
            logger.warning(
                "stations_fallback_endpoint_failed",
                provider=self.slug,
                error=str(exc),
            )
            return None

    # ------------------------------------------------------------------
    # Observation fetching internals
    # ------------------------------------------------------------------

    async def _try_fetch_observations_primary(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk | None:
        """Try GET /observations endpoint."""
        try:
            resp = await self._get(
                "/observations",
                params={
                    "station_id": native_id,
                    "start_date": start.strftime("%Y-%m-%d"),
                    "end_date": end.strftime("%Y-%m-%d"),
                    "parameter": "discharge",
                },
            )
            data = resp.json()
            return self._parse_observations_primary(data, station_id)
        except (ConnectorError, httpx.HTTPStatusError, httpx.HTTPError) as exc:
            logger.warning(
                "observations_primary_endpoint_failed",
                provider=self.slug,
                station=native_id,
                error=str(exc),
            )
            return None

    async def _try_fetch_observations_fallback(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk | None:
        """Try GET /SubInfo/getDischargeData endpoint."""
        try:
            resp = await self._get(
                "/SubInfo/getDischargeData",
                params={
                    "stationId": native_id,
                    "fromDate": start.strftime("%d-%m-%Y"),
                    "toDate": end.strftime("%d-%m-%Y"),
                },
            )
            data = resp.json()
            return self._parse_observations_fallback(data, station_id)
        except (ConnectorError, httpx.HTTPStatusError, httpx.HTTPError) as exc:
            logger.warning(
                "observations_fallback_endpoint_failed",
                provider=self.slug,
                station=native_id,
                error=str(exc),
            )
            return None

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    def _parse_stations(self, data: list[dict]) -> list[Station]:
        """Parse station entries into Station models."""
        stations: list[Station] = []
        for entry in data:
            try:
                native_id = str(
                    entry.get("station_id")
                    or entry.get("stationId")
                    or entry.get("id")
                    or ""
                )
                if not native_id:
                    continue

                name = str(
                    entry.get("station_name")
                    or entry.get("stationName")
                    or entry.get("name")
                    or ""
                )
                lat = self._safe_float(
                    entry.get("latitude") or entry.get("lat"), default=0.0
                ) or 0.0
                lon = self._safe_float(
                    entry.get("longitude") or entry.get("lng") or entry.get("lon"),
                    default=0.0,
                ) or 0.0
                river = (
                    entry.get("river_name")
                    or entry.get("riverName")
                    or entry.get("river")
                    or None
                )

                stations.append(
                    Station(
                        id=self._station_id(native_id),
                        provider=self.slug,
                        native_id=native_id,
                        name=name,
                        latitude=lat,
                        longitude=lon,
                        country_code="IN",
                        river=river,
                    )
                )
            except (ValueError, KeyError, TypeError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    entry=str(entry)[:200],
                    error=str(exc),
                )
                continue
        return stations

    def _parse_observations_primary(
        self, data: dict | list, station_id: str
    ) -> TimeSeriesChunk | None:
        """Parse response from the /observations endpoint.

        Expected format: {"observations": [{date, value, quality}, ...]}
        """
        obs_list: list[dict] = []
        if isinstance(data, dict):
            obs_list = data.get("observations", [])
        elif isinstance(data, list):
            obs_list = data
        else:
            logger.warning(
                "observations_unexpected_format",
                provider=self.slug,
                endpoint="primary",
            )
            return None

        if not isinstance(obs_list, list):
            return None

        observations = self._parse_obs_entries(obs_list, station_id)
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    def _parse_observations_fallback(
        self, data: dict | list, station_id: str
    ) -> TimeSeriesChunk | None:
        """Parse response from the /SubInfo/getDischargeData endpoint."""
        obs_list: list[dict] = []
        if isinstance(data, dict):
            obs_list = (
                data.get("data")
                or data.get("observations")
                or data.get("results", [])
            )
        elif isinstance(data, list):
            obs_list = data
        else:
            logger.warning(
                "observations_unexpected_format",
                provider=self.slug,
                endpoint="fallback",
            )
            return None

        if not isinstance(obs_list, list):
            return None

        observations = self._parse_obs_entries(obs_list, station_id)
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    def _parse_obs_entries(
        self, entries: list[dict], station_id: str
    ) -> list[Observation]:
        """Parse individual observation entries."""
        observations: list[Observation] = []
        for entry in entries:
            try:
                ts = self._parse_timestamp(entry)
                if ts is None:
                    continue

                value = entry.get("value") or entry.get("discharge")
                discharge = self._safe_float(value) if value is not None else None

                quality_raw = str(entry.get("quality", "")).lower().strip()
                quality = _QUALITY_MAP.get(
                    quality_raw,
                    QualityFlag.RAW if discharge is not None else QualityFlag.MISSING,
                )

                observations.append(
                    Observation(
                        station_id=station_id,
                        timestamp=ts,
                        discharge_m3s=discharge,
                        quality=quality,
                    )
                )
            except (ValueError, TypeError) as exc:
                logger.warning(
                    "observation_parse_failed",
                    provider=self.slug,
                    entry=str(entry)[:200],
                    error=str(exc),
                )
                continue
        return observations

    def _parse_timestamp(self, entry: dict) -> datetime | None:
        """Try multiple date field names and formats."""
        raw = entry.get("date") or entry.get("timestamp") or entry.get("dateTime")
        if raw is None:
            return None

        raw_str = str(raw).strip()
        if not raw_str:
            return None

        # Try ISO format first
        try:
            return datetime.fromisoformat(raw_str)
        except ValueError:
            pass

        # Try DD-MM-YYYY format (used by WRIS)
        try:
            return datetime.strptime(raw_str, "%d-%m-%Y")
        except ValueError:
            pass

        # Try DD/MM/YYYY format
        try:
            return datetime.strptime(raw_str, "%d/%m/%Y")
        except ValueError:
            pass

        logger.warning(
            "timestamp_parse_failed",
            provider=self.slug,
            raw=raw_str,
        )
        return None

    @staticmethod
    def _safe_float(value, default: float | None = None) -> float | None:
        """Safely convert a value to float, returning default on failure."""
        if value is None:
            return default
        try:
            return float(value)
        except (ValueError, TypeError):
            return default
