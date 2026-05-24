"""PEGELONLINE connector — German federal waterway gauging stations."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()


@register("germany_pegelonline")
class GermanyPegelonlineConnector(BaseConnector):
    slug = "germany_pegelonline"
    display_name = "PEGELONLINE (WSV Germany)"
    base_url = "https://www.pegelonline.wsv.de/webservices/rest-api/v2"
    country_codes = ["DE"]

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        # Cache mapping station number -> uuid, populated by fetch_stations
        self._number_to_uuid: dict[str, str] = {}

    async def fetch_stations(self) -> list[Station]:
        """Return all stations that have a discharge (Q) timeseries."""
        resp = await self._get(
            "/stations.json",
            params={
                "includeTimeseries": "true",
                "includeCurrentMeasurement": "true",
            },
        )
        return self._parse_stations(resp.json())

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge measurements for a station over a time range."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        uuid = await self._resolve_uuid(native_id)

        resp = await self._get(
            f"/stations/{uuid}/Q/measurements.json",
            params={
                "start": start.isoformat(),
                "end": end.isoformat(),
            },
        )
        return self._parse_measurements(resp.json(), station_id)

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

    def _parse_stations(self, data: list[dict]) -> list[Station]:
        """Parse the station list JSON and filter to discharge-capable stations."""
        stations: list[Station] = []
        for entry in data:
            timeseries = entry.get("timeseries", [])
            has_discharge = any(
                ts.get("shortname") == "Q" for ts in timeseries
            )
            if not has_discharge:
                continue

            native_id = entry.get("number", "")
            uuid = entry.get("uuid", "")
            if not native_id or not uuid:
                continue

            # Cache the number -> uuid mapping
            self._number_to_uuid[native_id] = uuid

            water = entry.get("water", {})
            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=entry.get("longname", entry.get("shortname", "")),
                    latitude=entry.get("latitude", 0.0),
                    longitude=entry.get("longitude", 0.0),
                    country_code="DE",
                    river=water.get("longname"),
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

    def _parse_measurements(
        self, data: list[dict], station_id: str
    ) -> TimeSeriesChunk:
        """Parse the measurements JSON array into a TimeSeriesChunk."""
        observations: list[Observation] = []
        for entry in data:
            try:
                ts = datetime.fromisoformat(entry["timestamp"])
            except (KeyError, ValueError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid timestamp in measurement: {exc}",
                )

            value = entry.get("value")
            discharge = float(value) if value is not None else None

            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=discharge,
                quality=QualityFlag.RAW if discharge is not None else QualityFlag.MISSING,
            ))

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    async def _resolve_uuid(self, native_id: str) -> str:
        """Return the PEGELONLINE uuid for a station number.

        Uses the cached mapping first; falls back to fetching the full
        station list if the mapping is empty.
        """
        if native_id in self._number_to_uuid:
            return self._number_to_uuid[native_id]

        # Populate cache by fetching all stations
        await self.fetch_stations()

        if native_id not in self._number_to_uuid:
            raise DataFormatError(
                self.slug,
                f"No uuid found for station number '{native_id}'",
            )
        return self._number_to_uuid[native_id]
