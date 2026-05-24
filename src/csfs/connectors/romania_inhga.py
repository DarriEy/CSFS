"""Romania INHGA connector -- Institutul National de Hidrologie si Gospodarire a Apelor.

The National Institute of Hydrology and Water Management (INHGA)
publishes hydrological data through inhga.ro.  An alternative hidro.ro
portal is used as fallback.

Endpoints used
--------------
* Primary station listing:
  GET https://www.inhga.ro/api/stations?type=hidro&format=json
  Returns ``[{cod, denumire, latitudine, longitudine, rau, bazin}, ...]``

* Primary observations:
  GET https://www.inhga.ro/api/data?station={cod}&param=debit&from={date}&to={date}&format=json
  Returns ``[{data, valoare}, ...]``

* Fallback portal: hidro.ro
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

_FALLBACK_BASE = "https://www.hidro.ro/api"


@register("romania_inhga")
class RomaniaINHGAConnector(BaseConnector):
    """Connector for Romania's INHGA hydrological discharge data.

    Uses dual-endpoint fallback: the primary inhga.ro API is tried
    first; on failure the hidro.ro portal is consulted.
    """

    slug = "romania_inhga"
    display_name = "INHGA (Romania)"
    base_url = "https://www.inhga.ro"
    country_codes = ["RO"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all discharge gauging stations.

        Tries inhga.ro first, then hidro.ro fallback.
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

        Tries inhga.ro first, then hidro.ro fallback.
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
        """Try the inhga.ro stations endpoint."""
        try:
            resp = await self._get(
                "/api/stations",
                params={"type": "hidro", "format": "json"},
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
        """Try the hidro.ro stations endpoint."""
        try:
            resp = await self.client.get(
                f"{_FALLBACK_BASE}/stations",
                params={"type": "discharge", "format": "json"},
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
        """Try the inhga.ro observations endpoint."""
        try:
            resp = await self._get(
                "/api/data",
                params={
                    "station": native_id,
                    "param": "debit",
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
        """Try the hidro.ro observations endpoint."""
        try:
            resp = await self.client.get(
                f"{_FALLBACK_BASE}/data",
                params={
                    "station": native_id,
                    "param": "debit",
                    "from": start.strftime("%Y-%m-%d"),
                    "to": end.strftime("%Y-%m-%d"),
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
                    entry.get("cod")
                    or entry.get("station_id")
                    or entry.get("id")
                    or ""
                )
                if not native_id:
                    continue

                name = str(
                    entry.get("denumire")
                    or entry.get("name")
                    or ""
                )
                lat = self._safe_float(
                    entry.get("latitudine")
                    or entry.get("latitude")
                    or entry.get("lat"),
                )
                lon = self._safe_float(
                    entry.get("longitudine")
                    or entry.get("longitude")
                    or entry.get("lon"),
                )
                river = (
                    entry.get("rau")
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
                    country_code="RO",
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
                    entry.get("data")
                    or entry.get("datum")
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
                entry.get("valoare")
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
