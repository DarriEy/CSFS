# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""SPW Hydrométrie connector — Wallonia (southern Belgium) via KiWIS.

Complements ``belgium_vmm`` (which covers Flanders) with the French-speaking
Walloon region — the Meuse and Sambre basins. The Service Public de Wallonie
exposes a public KiWIS time-series service (no authentication):

    https://hydrometrie.wallonie.be/services/KiWIS/KiWIS

Discharge is the ``Débit`` station parameter. The server is capacity-limited
and returns transient ``503`` errors — especially for large responses — so we
(1) filter ``getTimeseriesList`` to a single cadence (the hourly mean,
``10-Debit.1h.Moyen``, available at virtually every discharge station) to keep
responses small, and (2) retry transient 5xx locally. Of ~330 catalogue rows,
~305 are real discharge stations (the rest are basin-grouping entries with no
coordinates).
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import httpx
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
# Transient server errors worth retrying locally (the SPW KiWIS host is flaky).
_TRANSIENT_STATUS = (502, 503, 504)


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


@register("belgium_wallonia")
class BelgiumWalloniaConnector(BaseConnector):
    """Connector for SPW Hydrométrie's KiWIS service (Wallonia, Belgium)."""

    slug = "belgium_wallonia"
    display_name = "SPW Hydrométrie (Wallonia)"
    base_url = "https://hydrometrie.wallonie.be"
    country_codes = ["BE"]

    _KIWIS_PATH = "/services/KiWIS/KiWIS"
    _STATION_FIELDS = (
        "station_no,station_name,station_latitude,station_longitude,river_name"
    )
    _DISCHARGE_PARAM = "Débit"
    # The hourly-mean cadence is present at ~all discharge stations; filtering
    # to it keeps the timeseries-list response small enough to dodge 503s.
    _CADENCE = "10-Debit.1h.Moyen"
    _TRANSIENT_RETRIES = 6

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        # station_no -> ts_id for the hourly discharge series.
        self._discharge_ts: dict[str, str] | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return Walloon stations that have a discharge (Débit) timeseries."""
        ts_map = await self._load_discharge_ts()
        resp = await self._get_kiwis({
            "request": "getStationList",
            "returnfields": self._STATION_FIELDS,
        })
        return self._parse_stations(resp.json(), ts_map)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for a station over a time range."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        ts_map = await self._load_discharge_ts()
        ts_id = ts_map.get(native_id)
        if ts_id is None:
            raise ConnectorError(
                self.slug, f"No discharge (Débit) timeseries for station '{native_id}'",
            )

        resp = await self._get_kiwis({
            "request": "getTimeseriesValues",
            "ts_id": ts_id,
            "returnfields": "Timestamp,Value,Quality Code",
            "from": start.isoformat(),
            "to": end.isoformat(),
        })
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

    async def _get_kiwis(self, params: dict) -> httpx.Response:
        """KiWIS GET with the common params, retrying transient 5xx locally."""
        full = {
            "service": "kisters",
            "type": "queryServices",
            "datasource": "0",
            "format": "json",
            **params,
        }
        last_exc: httpx.HTTPStatusError | None = None
        for attempt in range(self._TRANSIENT_RETRIES):
            try:
                return await self._get(self._KIWIS_PATH, params=full, timeout=120.0)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code not in _TRANSIENT_STATUS:
                    raise
                last_exc = exc
                logger.warning(
                    "wallonia_transient",
                    provider=self.slug, status=exc.response.status_code,
                    attempt=attempt + 1,
                )
                await asyncio.sleep(1.0 * (attempt + 1))
        raise ConnectorError(
            self.slug,
            f"SPW KiWIS unavailable after {self._TRANSIENT_RETRIES} attempts",
        ) from last_exc

    async def _load_discharge_ts(self) -> dict[str, str]:
        """Load and cache {station_no: ts_id} for the hourly discharge series."""
        if self._discharge_ts is not None:
            return self._discharge_ts
        resp = await self._get_kiwis({
            "request": "getTimeseriesList",
            "stationparameter_name": self._DISCHARGE_PARAM,
            "ts_name": self._CADENCE,
            "returnfields": "station_no,ts_id,ts_name",
        })
        self._discharge_ts = self._parse_ts_list(resp.json())
        return self._discharge_ts

    def _parse_ts_list(self, data: list) -> dict[str, str]:
        result: dict[str, str] = {}
        if not data or len(data) < 2:
            return result
        columns: list[str] = data[0]
        try:
            idx_no = columns.index("station_no")
            idx_ts_id = columns.index("ts_id")
        except ValueError as exc:
            raise DataFormatError(
                self.slug, f"Unexpected timeseries column layout: {columns}",
            ) from exc
        for row in data[1:]:
            try:
                station_no = str(row[idx_no])
                ts_id = str(row[idx_ts_id])
                if station_no and ts_id:
                    result.setdefault(station_no, ts_id)
            except (IndexError, TypeError):
                continue
        return result

    def _parse_stations(
        self, data: list, ts_map: dict[str, str],
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
                if not native_id or native_id not in ts_map:
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
                    country_code="BE",
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
