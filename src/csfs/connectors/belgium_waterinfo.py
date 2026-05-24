"""Waterinfo.be connector — Belgian (Flanders) KiWIS hydrological data."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# KiWIS quality code ranges (same convention as Australia BOM)
_QUALITY_GOOD_MAX = 9
_QUALITY_FAIR_MAX = 19
_QUALITY_MISSING = 130
_QUALITY_NOT_AVAILABLE = 255


def _map_quality(code_str: str | None) -> QualityFlag:
    """Map a KiWIS quality code to a CSFS QualityFlag."""
    if code_str is None:
        return QualityFlag.MISSING
    try:
        code = int(code_str)
    except (ValueError, TypeError):
        return QualityFlag.RAW

    if code in (_QUALITY_MISSING, _QUALITY_NOT_AVAILABLE):
        return QualityFlag.MISSING
    if code <= _QUALITY_GOOD_MAX:
        return QualityFlag.GOOD
    if code <= _QUALITY_FAIR_MAX:
        return QualityFlag.GOOD
    return QualityFlag.SUSPECT


@register("belgium_waterinfo")
class BelgiumWaterinfoConnector(BaseConnector):
    slug = "belgium_waterinfo"
    display_name = "Waterinfo.be (Belgium Flanders)"
    base_url = "https://www.waterinfo.be/tsmpub/KiWIS/KiWIS"
    country_codes = ["BE"]

    # Column names requested in returnFields for station listing
    _STATION_FIELDS = (
        "station_no,station_name,station_latitude,"
        "station_longitude,parametertype_name"
    )

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        # Cache mapping station_no -> ts_id for discharge timeseries
        self._station_to_ts_id: dict[str, str] = {}

    async def fetch_stations(self) -> list[Station]:
        """Return all stations with a Discharge parameter."""
        resp = await self._get(
            "",
            params={
                "service": "kisters",
                "type": "queryServices",
                "request": "getStationList",
                "datasource": "1",
                "format": "json",
                "returnFields": self._STATION_FIELDS,
                "parametertype_name": "Discharge",
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
        ts_id = await self._resolve_ts_id(native_id)

        resp = await self._get(
            "",
            params={
                "service": "kisters",
                "type": "queryServices",
                "request": "getTimeseriesValues",
                "ts_id": ts_id,
                "from": start.isoformat(),
                "to": end.isoformat(),
                "format": "json",
                "returnFields": "Timestamp,Value,Quality Code",
            },
        )
        return self._parse_timeseries(resp.json(), station_id)

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

    def _parse_stations(self, data: list) -> list[Station]:
        """Parse the KiWIS station list response.

        The response is a list where the first element is a column-header
        array and subsequent elements are data rows (positional arrays).
        """
        if not data or len(data) < 2:
            return []

        columns: list[str] = data[0]
        try:
            idx_no = columns.index("station_no")
            idx_name = columns.index("station_name")
            idx_lat = columns.index("station_latitude")
            idx_lon = columns.index("station_longitude")
        except ValueError as exc:
            raise DataFormatError(
                self.slug,
                f"Unexpected column layout in station list: {columns}",
            ) from exc

        stations: list[Station] = []
        for row in data[1:]:
            try:
                native_id = str(row[idx_no])
                if not native_id:
                    continue

                lat = float(row[idx_lat]) if row[idx_lat] is not None else 0.0
                lon = float(row[idx_lon]) if row[idx_lon] is not None else 0.0

                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=str(row[idx_name] or ""),
                    latitude=lat,
                    longitude=lon,
                    country_code="BE",
                ))
            except (ValueError, IndexError, TypeError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    row=row,
                    error=str(exc),
                )
                continue
        return stations

    def _parse_timeseries(
        self, data: list, station_id: str,
    ) -> TimeSeriesChunk:
        """Parse the KiWIS getTimeseriesValues response.

        The response is a list containing a single dict with a ``data``
        key whose value is a list of [timestamp, value, quality_code]
        arrays.
        """
        observations: list[Observation] = []

        # Extract the data array from the response envelope
        ts_data: list = []
        if data and isinstance(data, list) and len(data) > 0:
            first = data[0]
            if isinstance(first, dict):
                ts_data = first.get("data", [])

        for entry in ts_data:
            if not isinstance(entry, list) or len(entry) < 2:
                continue
            try:
                ts = datetime.fromisoformat(entry[0])
            except (ValueError, TypeError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid timestamp in timeseries: {entry[0]}",
                ) from exc

            raw_value = entry[1]
            quality_code = entry[2] if len(entry) > 2 else None

            try:
                discharge = float(raw_value) if raw_value is not None else None
            except (ValueError, TypeError):
                discharge = None

            quality = _map_quality(
                str(quality_code) if quality_code is not None else None
            )
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

    async def _resolve_ts_id(self, native_id: str) -> str:
        """Return the KiWIS ts_id for a station's discharge timeseries.

        Uses the cached mapping first; falls back to querying
        getTimeseriesList for the station.
        """
        if native_id in self._station_to_ts_id:
            return self._station_to_ts_id[native_id]

        resp = await self._get(
            "",
            params={
                "service": "kisters",
                "type": "queryServices",
                "request": "getTimeseriesList",
                "datasource": "1",
                "format": "json",
                "station_no": native_id,
                "parametertype_name": "Discharge",
                "returnFields": "ts_id,ts_name,station_no",
            },
        )
        ts_list = resp.json()
        self._parse_ts_list(ts_list, native_id)

        if native_id not in self._station_to_ts_id:
            raise ConnectorError(
                self.slug,
                f"No discharge timeseries found for station '{native_id}'",
            )
        return self._station_to_ts_id[native_id]

    def _parse_ts_list(self, data: list, native_id: str) -> None:
        """Parse getTimeseriesList response and cache the first ts_id."""
        if not data or len(data) < 2:
            return

        columns: list[str] = data[0]
        try:
            idx_ts_id = columns.index("ts_id")
            idx_station_no = columns.index("station_no")
        except ValueError as exc:
            raise DataFormatError(
                self.slug,
                f"Unexpected column layout in timeseries list: {columns}",
            ) from exc

        for row in data[1:]:
            try:
                station_no = str(row[idx_station_no])
                ts_id = str(row[idx_ts_id])
                if station_no and ts_id and station_no not in self._station_to_ts_id:
                    self._station_to_ts_id[station_no] = ts_id
            except (IndexError, TypeError) as exc:
                logger.warning(
                    "ts_list_parse_failed",
                    provider=self.slug,
                    row=row,
                    error=str(exc),
                )
                continue
