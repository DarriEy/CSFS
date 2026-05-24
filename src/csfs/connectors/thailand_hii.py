"""Thailand HII connector -- Hydro-Informatics Institute.

The HII publishes water-discharge observations through a JSON REST API.
A secondary ThaiWater endpoint is used as a fallback when the primary
HII data portal is unreachable.

Endpoints used
--------------
* Primary station listing:
  GET https://data.hii.or.th/api/v1/stations?type=water_discharge&format=json
  Returns ``[{station_id, station_name, latitude, longitude, basin_name, river_name}, ...]``

* Primary observations:
  GET https://data.hii.or.th/api/v1/data?station_id={id}&param=discharge&start={ISO}&end={ISO}&format=json
  Returns ``[{datetime, value}, ...]``

* Fallback (ThaiWater):
  GET https://api-v3.thaiwater.net/api/v1/stations?type=discharge&format=json
  GET https://api-v3.thaiwater.net/api/v1/data?station_id={id}&param=discharge&start={ISO}&end={ISO}&format=json
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

_FALLBACK_BASE = "https://api-v3.thaiwater.net/api/v1"


@register("thailand_hii")
class ThailandHIIConnector(BaseConnector):
    """Connector for Thailand's HII hydrological discharge data.

    Uses dual-endpoint fallback: the primary HII data portal is tried
    first; on failure the ThaiWater API is consulted.
    """

    slug = "thailand_hii"
    display_name = "HII (Thailand)"
    base_url = "https://data.hii.or.th/api/v1"
    country_codes = ["TH"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all discharge gauging stations.

        Tries HII primary endpoint first, then ThaiWater fallback.
        """
        stations = await self._try_fetch_stations_primary()
        if stations is not None:
            return stations

        stations = await self._try_fetch_stations_fallback()
        if stations is not None:
            return stations

        logger.warning(
            "fetch_stations_failed_all_endpoints",
            provider=self.slug,
        )
        return []

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for *station_id*.

        Tries HII primary endpoint first, then ThaiWater fallback.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        chunk = await self._try_fetch_obs_primary(
            native_id, station_id, start, end,
        )
        if chunk is not None:
            return chunk

        chunk = await self._try_fetch_obs_fallback(
            native_id, station_id, start, end,
        )
        if chunk is not None:
            return chunk

        logger.warning(
            "fetch_observations_failed_all_endpoints",
            provider=self.slug,
            station=native_id,
        )
        return self._empty_chunk(station_id)

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

    async def _try_fetch_stations_primary(
        self,
    ) -> list[Station] | None:
        """Try the HII primary stations endpoint."""
        try:
            resp = await self._get(
                "/stations",
                params={
                    "type": "water_discharge",
                    "format": "json",
                },
            )
            data = resp.json()
            return self._parse_stations(self._unwrap(data))
        except (
            ConnectorError,
            httpx.HTTPStatusError,
            httpx.HTTPError,
        ) as exc:
            logger.warning(
                "stations_primary_failed",
                provider=self.slug,
                error=str(exc),
            )
            return None

    async def _try_fetch_stations_fallback(
        self,
    ) -> list[Station] | None:
        """Try the ThaiWater fallback stations endpoint."""
        try:
            resp = await self.client.get(
                f"{_FALLBACK_BASE}/stations",
                params={
                    "type": "discharge",
                    "format": "json",
                },
            )
            if resp.status_code != 200:
                resp.raise_for_status()
            data = resp.json()
            return self._parse_stations(self._unwrap(data))
        except (
            ConnectorError,
            httpx.HTTPStatusError,
            httpx.HTTPError,
        ) as exc:
            logger.warning(
                "stations_fallback_failed",
                provider=self.slug,
                error=str(exc),
            )
            return None

    # ------------------------------------------------------------------
    # Observation fetching internals
    # ------------------------------------------------------------------

    async def _try_fetch_obs_primary(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk | None:
        """Try the HII primary observations endpoint."""
        try:
            resp = await self._get(
                "/data",
                params={
                    "station_id": native_id,
                    "param": "discharge",
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "format": "json",
                },
            )
            data = resp.json()
            return self._parse_observations(
                self._unwrap(data), station_id,
            )
        except (
            ConnectorError,
            httpx.HTTPStatusError,
            httpx.HTTPError,
        ) as exc:
            logger.warning(
                "observations_primary_failed",
                provider=self.slug,
                station=native_id,
                error=str(exc),
            )
            return None

    async def _try_fetch_obs_fallback(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk | None:
        """Try the ThaiWater fallback observations endpoint."""
        try:
            resp = await self.client.get(
                f"{_FALLBACK_BASE}/data",
                params={
                    "station_id": native_id,
                    "param": "discharge",
                    "start": start.isoformat(),
                    "end": end.isoformat(),
                    "format": "json",
                },
            )
            if resp.status_code != 200:
                resp.raise_for_status()
            data = resp.json()
            return self._parse_observations(
                self._unwrap(data), station_id,
            )
        except (
            ConnectorError,
            httpx.HTTPStatusError,
            httpx.HTTPError,
        ) as exc:
            logger.warning(
                "observations_fallback_failed",
                provider=self.slug,
                station=native_id,
                error=str(exc),
            )
            return None

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _unwrap(data: dict | list) -> list[dict]:
        """Extract a list from a possibly-wrapped response."""
        if isinstance(data, list):
            return data
        if isinstance(data, dict):
            for key in ("data", "stations", "results"):
                val = data.get(key)
                if isinstance(val, list):
                    return val
            return []
        return []

    def _parse_stations(self, items: list[dict]) -> list[Station]:
        """Parse station entries into ``Station`` models."""
        stations: list[Station] = []
        for entry in items:
            try:
                native_id = str(
                    entry.get("station_id") or entry.get("id") or ""
                )
                if not native_id:
                    continue

                name = str(
                    entry.get("station_name")
                    or entry.get("name")
                    or ""
                )
                lat = self._safe_float(
                    entry.get("latitude") or entry.get("lat"),
                )
                lon = self._safe_float(
                    entry.get("longitude") or entry.get("lon"),
                )
                river = (
                    entry.get("river_name")
                    or entry.get("river")
                    or None
                )

                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=name,
                    latitude=lat or 0.0,
                    longitude=lon or 0.0,
                    country_code="TH",
                    river=river,
                ))
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
        items: list[dict],
        station_id: str,
    ) -> TimeSeriesChunk:
        """Parse observation entries into a ``TimeSeriesChunk``."""
        observations: list[Observation] = []
        for entry in items:
            try:
                raw_ts = (
                    entry.get("datetime")
                    or entry.get("timestamp")
                    or entry.get("date")
                )
                if raw_ts is None:
                    continue
                ts = datetime.fromisoformat(str(raw_ts))
            except (ValueError, TypeError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid timestamp in observation: {exc}",
                ) from exc

            value = entry.get("value") or entry.get("discharge")
            discharge = (
                self._safe_float(value) if value is not None else None
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

    def _empty_chunk(self, station_id: str) -> TimeSeriesChunk:
        """Return an empty ``TimeSeriesChunk`` for failed requests."""
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )

    @staticmethod
    def _safe_float(
        value: object, default: float | None = None,
    ) -> float | None:
        """Safely convert a value to float."""
        if value is None:
            return default
        try:
            return float(value)  # type: ignore[arg-type]
        except (ValueError, TypeError):
            return default
