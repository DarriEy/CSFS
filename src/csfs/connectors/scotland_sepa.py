# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""SEPA connector — Scottish Environment Protection Agency via KiWIS.

SEPA covers Scotland (the UK EA connector covers England & Wales only). Its
public KiWIS time-series service needs no authentication:

    https://timeseries.sepa.org.uk/KiWIS/KiWIS

Discharge is the ``Flow`` station parameter. A single filtered
``getTimeseriesList`` call maps every flow series across all stations; we then
pick the best cadence per station (15-minute, else daily mean) and pull values
via ``getTimeseriesValues``. Of ~900 stations, ~320 measure flow.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# KISTERS quality codes: low = validated, 130/255 = missing, otherwise the
# value is provisional/unvalidated real-time data.
_QUALITY_GOOD_MAX = 40
_QUALITY_MISSING = (130, 255)


def _map_quality(code: object) -> QualityFlag:
    if code is None:
        return QualityFlag.MISSING
    try:
        value = int(str(code))
    except (ValueError, TypeError):
        return QualityFlag.RAW
    if value in _QUALITY_MISSING:
        return QualityFlag.MISSING
    if value <= _QUALITY_GOOD_MAX:
        return QualityFlag.GOOD
    return QualityFlag.RAW


@register("scotland_sepa")
class ScotlandSepaConnector(BaseConnector):
    """Connector for SEPA's KiWIS time-series service (Scotland)."""

    slug = "scotland_sepa"
    display_name = "SEPA (Scotland)"
    base_url = "https://timeseries.sepa.org.uk"
    country_codes = ["GB"]

    _KIWIS_PATH = "/KiWIS/KiWIS"
    _STATION_FIELDS = (
        "station_no,station_name,station_latitude,station_longitude,river_name"
    )
    # Prefer real-time 15-minute flow, then daily means.
    _TS_PREFERENCE = ("15minute", "Hour.Mean", "Day.Mean", "Day.Mean.Natural")

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        # station_no -> {ts_name: ts_id} for Flow series.
        self._flow_series: dict[str, dict[str, str]] | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return SEPA stations that have a discharge (Flow) timeseries."""
        flow_series = await self._load_flow_series()
        resp = await self._get(
            self._KIWIS_PATH,
            params={
                "service": "kisters",
                "type": "queryServices",
                "request": "getStationList",
                "datasource": "0",
                "format": "json",
                "returnfields": self._STATION_FIELDS,
            },
        )
        return self._parse_stations(resp.json(), flow_series)

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
            self._KIWIS_PATH,
            params={
                "service": "kisters",
                "type": "queryServices",
                "request": "getTimeseriesValues",
                "datasource": "0",
                "format": "json",
                "ts_id": ts_id,
                "returnfields": "Timestamp,Value,Quality Code",
                "from": start.isoformat(),
                "to": end.isoformat(),
            },
        )
        return self._parse_timeseries(resp.json(), station_id)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent discharge observations (last 24 h)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id, start=now - timedelta(hours=24), end=now,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_stations(
        self, data: list, flow_series: dict[str, dict[str, str]],
    ) -> list[Station]:
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
                self.slug, f"Unexpected station column layout: {columns}",
            ) from exc
        idx_river = columns.index("river_name") if "river_name" in columns else None

        stations: list[Station] = []
        for row in data[1:]:
            try:
                native_id = str(row[idx_no])
                if not native_id or native_id not in flow_series:
                    continue
                lat = _to_float(row[idx_lat])
                lon = _to_float(row[idx_lon])
                if lat is None or lon is None:
                    continue
                river = (
                    str(row[idx_river]).strip()
                    if idx_river is not None and row[idx_river]
                    else None
                )
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=str(row[idx_name] or "").strip() or native_id,
                    latitude=lat,
                    longitude=lon,
                    country_code="GB",
                    river=river,
                ))
            except (IndexError, TypeError) as exc:
                logger.warning(
                    "station_parse_failed", provider=self.slug,
                    row=row, error=str(exc),
                )
                continue
        return stations

    def _parse_timeseries(self, data: list, station_id: str) -> TimeSeriesChunk:
        observations: list[Observation] = []
        ts_data: list = []
        if data and isinstance(data, list) and isinstance(data[0], dict):
            ts_data = data[0].get("data", [])

        for entry in ts_data:
            if not isinstance(entry, list) or len(entry) < 2:
                continue
            ts = _parse_iso(entry[0])
            if ts is None:
                raise DataFormatError(
                    self.slug, f"Invalid timestamp in timeseries: {entry[0]}",
                )
            discharge = _to_float(entry[1])
            quality = _map_quality(entry[2] if len(entry) > 2 else None)
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

    async def _load_flow_series(self) -> dict[str, dict[str, str]]:
        """Load and cache {station_no: {ts_name: ts_id}} for Flow series."""
        if self._flow_series is not None:
            return self._flow_series
        resp = await self._get(
            self._KIWIS_PATH,
            params={
                "service": "kisters",
                "type": "queryServices",
                "request": "getTimeseriesList",
                "datasource": "0",
                "format": "json",
                "stationparameter_name": "Flow",
                "returnfields": "station_no,ts_id,ts_name",
            },
            timeout=120.0,
        )
        self._flow_series = self._parse_flow_series(resp.json())
        return self._flow_series

    def _parse_flow_series(self, data: list) -> dict[str, dict[str, str]]:
        result: dict[str, dict[str, str]] = {}
        if not data or len(data) < 2:
            return result
        columns: list[str] = data[0]
        try:
            idx_no = columns.index("station_no")
            idx_ts_id = columns.index("ts_id")
            idx_name = columns.index("ts_name")
        except ValueError as exc:
            raise DataFormatError(
                self.slug, f"Unexpected timeseries column layout: {columns}",
            ) from exc
        for row in data[1:]:
            try:
                station_no = str(row[idx_no])
                ts_id = str(row[idx_ts_id])
                ts_name = str(row[idx_name])
                if station_no and ts_id:
                    result.setdefault(station_no, {})[ts_name] = ts_id
            except (IndexError, TypeError):
                continue
        return result

    async def _resolve_ts_id(self, native_id: str) -> str:
        flow_series = await self._load_flow_series()
        series = flow_series.get(native_id)
        if series:
            for ts_name in self._TS_PREFERENCE:
                if ts_name in series:
                    return series[ts_name]
            return next(iter(series.values()))
        raise ConnectorError(
            self.slug, f"No discharge (Flow) timeseries for station '{native_id}'",
        )


def _to_float(value: object) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(str(value))
    except (ValueError, TypeError):
        return None


def _parse_iso(value: object) -> datetime | None:
    if not isinstance(value, str):
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None
