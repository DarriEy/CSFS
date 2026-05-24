"""DMI Hydrological Data connector — Danish Meteorological Institute waterflow API."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# DMI quality codes
_QUALITY_MAP: dict[str, QualityFlag] = {
    "approved": QualityFlag.GOOD,
    "controlled": QualityFlag.GOOD,
    "raw": QualityFlag.RAW,
    "suspect": QualityFlag.SUSPECT,
}


def _map_quality(quality_str: str | None) -> QualityFlag:
    """Map a DMI quality string to a CSFS QualityFlag."""
    if quality_str is None:
        return QualityFlag.RAW
    return _QUALITY_MAP.get(quality_str.lower(), QualityFlag.RAW)


@register("denmark_dmihyd")
class DenmarkDmihydConnector(BaseConnector):
    slug = "denmark_dmihyd"
    display_name = "DMI Hydrological Data (Denmark)"
    base_url = "https://dmigw.govcloud.dk/v1/waterflow"
    country_codes = ["DK"]

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)

    async def fetch_stations(self) -> list[Station]:
        """Return all stations available from DMI waterflow API."""
        params: dict[str, str] = {}
        api_key = self.config.get("api_key", "")
        if api_key:
            params["api_key"] = api_key

        try:
            resp = await self._get("/stations", params=params)
        except Exception as exc:
            raise ConnectorError(
                self.slug, f"Failed to fetch stations: {exc}"
            ) from exc

        return self._parse_stations(resp.json())

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for a station over a time range."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        params: dict[str, str] = {
            "stationId": native_id,
            "from": start.isoformat(),
            "to": end.isoformat(),
        }
        api_key = self.config.get("api_key", "")
        if api_key:
            params["api_key"] = api_key

        try:
            resp = await self._get("/observations", params=params)
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch observations for station '{native_id}': {exc}",
            ) from exc

        return self._parse_observations(resp.json(), station_id)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent discharge observations (last 24 h)."""
        from datetime import timedelta

        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_stations(self, data: list | dict) -> list[Station]:
        """Parse the DMI station list JSON response."""
        # Response may be a list directly or a dict with a key
        entries: list[dict] = []
        if isinstance(data, list):
            entries = data
        elif isinstance(data, dict):
            entries = data.get("stations", data.get("features", []))

        stations: list[Station] = []
        for entry in entries:
            try:
                native_id = str(entry.get("stationId", ""))
                if not native_id:
                    continue

                name = entry.get("name", "")
                lat = float(entry.get("latitude", 0.0))
                lon = float(entry.get("longitude", 0.0))
                river = entry.get("waterBodyName")

                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=name or "",
                    latitude=lat,
                    longitude=lon,
                    country_code="DK",
                    river=river,
                ))
            except (ValueError, KeyError, TypeError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    station=entry.get("stationId", "unknown"),
                    error=str(exc),
                )
                continue
        return stations

    def _parse_observations(
        self, data: list | dict, station_id: str
    ) -> TimeSeriesChunk:
        """Parse the DMI observations JSON response."""
        entries: list[dict] = []
        if isinstance(data, list):
            entries = data
        elif isinstance(data, dict):
            entries = data.get("observations", data.get("features", []))

        observations: list[Observation] = []
        for entry in entries:
            try:
                ts_str = entry.get("observed", "")
                if not ts_str:
                    continue
                ts = datetime.fromisoformat(ts_str)
            except (ValueError, TypeError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid timestamp in observation: {exc}",
                ) from exc

            raw_value = entry.get("value")
            try:
                discharge = float(raw_value) if raw_value is not None else None
            except (ValueError, TypeError):
                discharge = None

            quality = _map_quality(entry.get("quality"))
            if discharge is None:
                quality = QualityFlag.MISSING

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
