"""Slovakia SHMU connector -- Slovensky hydrometeorologicky ustav.

The Slovak Hydrometeorological Institute (SHMU) publishes hydrological
data.  The public REST API is experimental and may be unreliable.

Endpoints used
--------------
* Station listing:
  GET https://www.shmu.sk/sk/?page=1&id=hydro_aktual /api/hydro/stations?format=json
  Returns ``[{id, nazov, zs (lat), zd (lon), tok (river)}, ...]``

* Observations:
  GET /api/hydro/data?station={id}&param=prutok&from={date}&to={date}&format=json
  Returns ``[{datum, hodnota}, ...]``

This connector is written very defensively as SHMU may not expose a
clean REST API.
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


@register("slovakia_shmu")
class SlovakiaSHMUConnector(BaseConnector):
    """Connector for Slovakia's SHMU hydrological discharge data.

    The SHMU API is experimental; all requests are wrapped in defensive
    error handling.  Failures are logged and empty results returned.
    """

    slug = "slovakia_shmu"
    display_name = "SHMU (Slovakia)"
    base_url = "https://www.shmu.sk"
    country_codes = ["SK"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all discharge gauging stations from SHMU."""
        try:
            resp = await self._get(
                "/api/hydro/stations",
                params={"format": "json"},
            )
            data = resp.json()
            return self._parse_stations(self._unwrap(data))
        except (
            ConnectorError,
            httpx.HTTPStatusError,
            httpx.HTTPError,
        ) as exc:
            logger.warning(
                "fetch_stations_failed",
                provider=self.slug,
                error=str(exc),
            )
            return []

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for *station_id*."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        try:
            resp = await self._get(
                "/api/hydro/data",
                params={
                    "station": native_id,
                    "param": "prutok",
                    "from": start.strftime("%Y-%m-%d"),
                    "to": end.strftime("%Y-%m-%d"),
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
                "fetch_observations_failed",
                provider=self.slug,
                station=native_id,
                error=str(exc),
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
                    entry.get("id")
                    or entry.get("station_id")
                    or ""
                )
                if not native_id:
                    continue

                name = str(
                    entry.get("nazov")
                    or entry.get("name")
                    or ""
                )
                lat = self._safe_float(
                    entry.get("zs")
                    or entry.get("latitude")
                    or entry.get("lat"),
                )
                lon = self._safe_float(
                    entry.get("zd")
                    or entry.get("longitude")
                    or entry.get("lon"),
                )
                river = (
                    entry.get("tok")
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
                    country_code="SK",
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
                    entry.get("datum")
                    or entry.get("datetime")
                    or entry.get("timestamp")
                )
                if raw_ts is None:
                    continue
                ts = datetime.fromisoformat(str(raw_ts))
            except (ValueError, TypeError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid timestamp in observation: {exc}",
                ) from exc

            value = (
                entry.get("hodnota")
                or entry.get("value")
                or entry.get("discharge")
            )
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
