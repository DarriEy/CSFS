"""Bangladesh FFWC / BWDB connector — Flood Forecasting and Warning Centre gauging stations."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# Quality mapping from FFWC quality strings to CSFS flags
_QUALITY_MAP: dict[str, QualityFlag] = {
    "good": QualityFlag.GOOD,
    "suspect": QualityFlag.SUSPECT,
    "estimated": QualityFlag.ESTIMATED,
    "missing": QualityFlag.MISSING,
}

# BWDB hydrology portal base URL used as fallback
_BWDB_HYDROLOGY_BASE = "http://www.hydrology.bwdb.gov.bd"


@register("bangladesh_ffwc")
class BangladeshFFWCConnector(BaseConnector):
    """Connector for Bangladesh's Flood Forecasting and Warning Centre (FFWC) and BWDB."""

    slug = "bangladesh_ffwc"
    display_name = "FFWC Bangladesh (BWDB)"
    base_url = "https://ffwc.bwdb.gov.bd"
    country_codes = ["BD"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all gauging stations from FFWC.

        Tries the primary data_load endpoint first; falls back to the
        /api/stations endpoint if the primary fails.
        """
        stations = await self._try_fetch_stations_primary()
        if stations is not None:
            return stations

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
        """Fetch water-level and discharge observations for a station.

        Tries the primary data_load endpoint first; falls back to the
        /api/data endpoint if the primary fails.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        chunk = await self._try_fetch_observations_primary(
            native_id, station_id, start, end,
        )
        if chunk is not None:
            return chunk

        chunk = await self._try_fetch_observations_fallback(
            native_id, station_id, start, end,
        )
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
        """Try GET /data_load?t=station_list endpoint."""
        try:
            resp = await self._get(
                "/data_load", params={"t": "station_list"},
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
        """Try GET /api/stations?format=json endpoint."""
        try:
            resp = await self._get(
                "/api/stations", params={"format": "json"},
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
        """Try GET /data_load?t=water_level&station_id={id} endpoint."""
        try:
            resp = await self._get(
                "/data_load",
                params={
                    "t": "water_level",
                    "station_id": native_id,
                    "start_date": start.strftime("%Y-%m-%d"),
                    "end_date": end.strftime("%Y-%m-%d"),
                },
            )
            data = resp.json()
            return self._parse_observations(data, station_id, endpoint="primary")
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
        """Try GET /api/data?station={id}&type=discharge endpoint."""
        try:
            resp = await self._get(
                "/api/data",
                params={
                    "station": native_id,
                    "type": "discharge",
                    "start_date": start.strftime("%Y-%m-%d"),
                    "end_date": end.strftime("%Y-%m-%d"),
                },
            )
            data = resp.json()
            return self._parse_observations(data, station_id, endpoint="fallback")
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
                    entry.get("latitude") or entry.get("lat"),
                    default=0.0,
                ) or 0.0
                lon = self._safe_float(
                    entry.get("longitude")
                    or entry.get("lng")
                    or entry.get("lon"),
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
                        country_code="BD",
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

    def _parse_observations(
        self,
        data: dict | list,
        station_id: str,
        *,
        endpoint: str,
    ) -> TimeSeriesChunk | None:
        """Parse observation response from either endpoint.

        Accepts formats:
        - A bare list of observation dicts
        - A dict wrapping observations under "data", "observations", or "results"
        """
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
                endpoint=endpoint,
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
        self, entries: list[dict], station_id: str,
    ) -> list[Observation]:
        """Parse individual observation entries.

        FFWC may return water_level and/or discharge; we map discharge
        into discharge_m3s and store water_level in metadata when
        discharge is unavailable.
        """
        observations: list[Observation] = []
        for entry in entries:
            try:
                ts = self._parse_timestamp(entry)
                if ts is None:
                    continue

                # Prefer discharge; fall back to water_level
                raw_discharge = (
                    entry.get("discharge")
                    or entry.get("value")
                    or entry.get("discharge_m3s")
                )
                discharge = (
                    self._safe_float(raw_discharge)
                    if raw_discharge is not None
                    else None
                )

                # If no discharge, try water_level as the observation value
                if discharge is None:
                    raw_wl = entry.get("water_level") or entry.get("wl")
                    discharge = (
                        self._safe_float(raw_wl)
                        if raw_wl is not None
                        else None
                    )

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
        raw = (
            entry.get("datetime")
            or entry.get("date")
            or entry.get("timestamp")
            or entry.get("dateTime")
        )
        if raw is None:
            return None

        raw_str = str(raw).strip()
        if not raw_str:
            return None

        # Try ISO format first (e.g. "2024-06-01T06:00:00+06:00")
        try:
            return datetime.fromisoformat(raw_str)
        except ValueError:
            pass

        # Try YYYY-MM-DD (common API format)
        try:
            return datetime.strptime(raw_str, "%Y-%m-%d")
        except ValueError:
            pass

        # Try DD-MM-YYYY format (used by BWDB portals)
        try:
            return datetime.strptime(raw_str, "%d-%m-%Y")
        except ValueError:
            pass

        # Try DD/MM/YYYY format
        try:
            return datetime.strptime(raw_str, "%d/%m/%Y")
        except ValueError:
            pass

        # Try YYYY-MM-DD HH:MM:SS
        try:
            return datetime.strptime(raw_str, "%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass

        logger.warning(
            "timestamp_parse_failed",
            provider=self.slug,
            raw=raw_str,
        )
        return None

    @staticmethod
    def _safe_float(
        value: object, default: float | None = None,
    ) -> float | None:
        """Safely convert a value to float, returning default on failure."""
        if value is None:
            return default
        try:
            return float(str(value))
        except (ValueError, TypeError):
            return default
