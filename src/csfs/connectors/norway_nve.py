"""NVE HydAPI connector — Norwegian Water Resources and Energy Directorate."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# NVE parameter code for discharge (Vannforing)
_PARAM_DISCHARGE = "1001"

# Resolution codes: 60 = hourly, 1440 = daily
_RESOLUTION_DAILY = "1440"


def _correction_to_quality(correction: int | None) -> QualityFlag:
    """Map NVE correction codes to CSFS quality flags.

    0 = raw, 1 = corrected (good), 2 = estimated.
    """
    if correction is None:
        return QualityFlag.RAW
    if correction == 1:
        return QualityFlag.GOOD
    if correction == 2:
        return QualityFlag.ESTIMATED
    return QualityFlag.RAW


@register("norway_nve")
class NorwayNVEConnector(BaseConnector):
    slug = "norway_nve"
    display_name = "NVE HydAPI (Norway)"
    base_url = "https://hydapi.nve.no/api/v1"
    country_codes = ["NO"]

    async def __aenter__(self) -> NorwayNVEConnector:
        await super().__aenter__()
        api_key = self.config.get("api_key", "")
        if api_key:
            self.client.headers["X-API-Key"] = api_key
        return self

    async def fetch_stations(self) -> list[Station]:
        """Return all active stations with discharge observations."""
        resp = await self._get(
            "/Stations",
            params={
                "Active": "1",
                "StationFunctionality": "1",
                "Parameter": _PARAM_DISCHARGE,
            },
        )
        return self._parse_stations(resp.json())

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for a station over a time range."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        reference_time = (
            f"{start.strftime('%Y-%m-%d')}/{end.strftime('%Y-%m-%d')}"
        )

        resp = await self._get(
            "/Observations",
            params={
                "StationId": native_id,
                "Parameter": _PARAM_DISCHARGE,
                "ResolutionTime": _RESOLUTION_DAILY,
                "ReferenceTime": reference_time,
            },
        )
        return self._parse_observations(resp.json(), station_id)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent discharge observations (last 24 h)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_stations(self, data: list[dict]) -> list[Station]:
        """Parse the NVE station list JSON into Station models."""
        stations: list[Station] = []
        for entry in data:
            native_id = str(entry.get("stationId", "")).strip()
            if not native_id:
                continue

            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=entry.get("stationName", ""),
                    latitude=float(entry.get("latitude", 0.0)),
                    longitude=float(entry.get("longitude", 0.0)),
                    country_code="NO",
                    river=entry.get("riverName"),
                    catchment_area_km2=entry.get("drainageBasinArea_km2"),
                ))
            except (ValueError, KeyError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    station=native_id,
                    error=str(exc),
                )
                continue
        return stations

    def _parse_observations(
        self, data: dict, station_id: str
    ) -> TimeSeriesChunk:
        """Parse the NVE observations response into a TimeSeriesChunk.

        Response shape:
        {
            "data": [
                {
                    "stationId": "...",
                    "parameter": "1001",
                    "observations": [
                        {"time": "2024-06-01T00:00:00Z", "value": 12.3, "correction": 1},
                        ...
                    ]
                }
            ]
        }
        """
        observations: list[Observation] = []

        data_list = data.get("data", [])
        for series in data_list:
            for obs in series.get("observations", []):
                try:
                    ts = datetime.fromisoformat(obs["time"])
                except (KeyError, ValueError) as exc:
                    raise DataFormatError(
                        self.slug,
                        f"Invalid timestamp in observation: {exc}",
                    ) from exc

                value = obs.get("value")
                discharge = float(value) if value is not None else None
                correction = obs.get("correction")
                quality = QualityFlag.MISSING if discharge is None else _correction_to_quality(correction)

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
