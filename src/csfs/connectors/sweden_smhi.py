"""SMHI connector — Swedish Meteorological and Hydrological Institute hydrology data."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()


def _quality_from_smhi(raw: str) -> QualityFlag:
    """Map SMHI quality codes to CSFS quality flags.

    SMHI codes:
        "G"          -> good (green)
        "Controlled" -> good (manually verified)
        "Y"          -> suspect (yellow / uncertain)
    """
    code = raw.strip()
    if code in ("G", "Controlled"):
        return QualityFlag.GOOD
    if code == "Y":
        return QualityFlag.SUSPECT
    return QualityFlag.RAW


@register("sweden_smhi")
class SwedenSMHIConnector(BaseConnector):
    slug = "sweden_smhi"
    display_name = "SMHI Hydrology (Sweden)"
    base_url = "https://opendata-download-hydroobs.smhi.se/api"
    country_codes = ["SE"]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all stations that report water discharge (parameter 1)."""
        resp = await self._get("/version/latest/parameter/1.json")
        data = resp.json()
        return self._parse_stations(data)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for a station, filtered to [start, end].

        SMHI's ``latest-months`` period returns roughly the last four months
        of data. Date-range filtering is done client-side because the API
        does not accept arbitrary date parameters on the observations
        endpoint.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        resp = await self._get(
            f"/version/latest/parameter/1/station/{native_id}"
            f"/period/latest-day/data.json",
        )
        data = resp.json()
        return self._parse_observations(data, station_id, start, end)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent observations (last 24 h)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_stations(self, data: dict) -> list[Station]:
        """Parse the station listing JSON from SMHI parameter 1 endpoint."""
        stations: list[Station] = []
        for entry in data.get("station", []):
            native_id = str(entry.get("key", ""))
            if not native_id:
                continue

            is_active = entry.get("active", False)

            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=entry.get("name", ""),
                    latitude=float(entry.get("latitude", 0.0)),
                    longitude=float(entry.get("longitude", 0.0)),
                    country_code="SE",
                    is_active=is_active,
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
        self,
        data: dict,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Parse the observation JSON and filter to [start, end]."""
        # Ensure start/end are timezone-aware (UTC) for comparison
        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)
        if end.tzinfo is None:
            end = end.replace(tzinfo=UTC)

        observations: list[Observation] = []
        for entry in data.get("value", []):
            date_ms = entry.get("date")
            if date_ms is None:
                continue

            try:
                ts = datetime.fromtimestamp(date_ms / 1000.0, tz=UTC)
            except (OSError, ValueError, OverflowError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid epoch timestamp in observation: {exc}",
                ) from exc

            # Client-side date range filter
            if ts < start or ts > end:
                continue

            raw_value = entry.get("value")
            discharge = float(raw_value) if raw_value is not None else None
            quality_code = entry.get("quality", "")
            quality = QualityFlag.MISSING if discharge is None else _quality_from_smhi(quality_code)

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
