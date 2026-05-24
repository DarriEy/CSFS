"""UK NRFA connector — National River Flow Archive.

The NRFA (operated by UKCEH) provides well-documented JSON APIs for
station metadata and gauged daily flow (GDF) time series at
https://nrfaapps.ceh.ac.uk/nrfa/ws.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()


@register("uk_nrfa")
class UKNRFAConnector(BaseConnector):
    """Connector for the UK National River Flow Archive."""

    slug = "uk_nrfa"
    display_name = "UK NRFA"
    base_url = "https://nrfaapps.ceh.ac.uk/nrfa/ws"
    country_codes = ["GB"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all NRFA gauging stations."""
        try:
            resp = await self._get(
                "/station-info",
                params={
                    "station": "*",
                    "format": "json-object",
                    "fields": "id,name,lat,lng,river,catchment-area",
                },
            )
            data = resp.json()
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

        items = data.get("data", []) if isinstance(data, dict) else data
        if not isinstance(items, list):
            logger.warning(
                "stations_unexpected_format",
                provider=self.slug,
            )
            return []

        return self._parse_stations(items)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch gauged daily flow for a station over a time range."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        try:
            resp = await self._get(
                "/time-series",
                params={
                    "format": "json-object",
                    "data-type": "gdf",
                    "station": native_id,
                    "start-date": start.strftime("%Y-%m-%d"),
                    "end-date": end.strftime("%Y-%m-%d"),
                },
            )
            data = resp.json()
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
            return TimeSeriesChunk(
                station_id=station_id,
                provider=self.slug,
                observations=[],
                fetched_at=datetime.now(UTC),
            )

        return self._parse_observations(data, station_id)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent 30 days of gauged daily flow."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(days=30),
            end=now,
        )

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    def _parse_stations(self, data: list[dict]) -> list[Station]:
        """Parse station entries from NRFA JSON."""
        stations: list[Station] = []
        for entry in data:
            try:
                native_id = str(entry.get("id", ""))
                if not native_id:
                    continue

                name = str(entry.get("name", ""))
                lat = _safe_float(entry.get("lat"))
                lon = _safe_float(entry.get("lng"))
                if lat is None or lon is None:
                    continue

                river = entry.get("river")
                area = _safe_float(entry.get("catchment-area"))

                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=name,
                    latitude=lat,
                    longitude=lon,
                    country_code="GB",
                    river=river,
                    catchment_area_km2=area,
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
        data: dict | list,
        station_id: str,
    ) -> TimeSeriesChunk:
        """Parse GDF time-series from NRFA JSON response."""
        stream: list[dict] = []
        if isinstance(data, dict):
            stream = data.get("data-stream", [])
        elif isinstance(data, list):
            stream = data

        if not isinstance(stream, list):
            stream = []

        observations: list[Observation] = []
        for entry in stream:
            try:
                date_str = entry.get("date")
                if not date_str:
                    continue
                ts = datetime.fromisoformat(str(date_str))

                value = entry.get("gdf-mean-flow")
                discharge = _safe_float(value)
                quality = (
                    QualityFlag.GOOD
                    if discharge is not None
                    else QualityFlag.MISSING
                )

                observations.append(Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=quality,
                ))
            except (ValueError, TypeError):
                continue

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------


def _safe_float(value: object) -> float | None:
    """Safely convert a value to float."""
    if value is None:
        return None
    try:
        return float(str(value))
    except (ValueError, TypeError):
        return None
