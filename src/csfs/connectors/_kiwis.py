# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Shared base for connectors backed by a public KiWIS (KISTERS WISKI) API.

Several agencies expose discharge through the same KiWIS ``QueryServices``
interface (``belgium_vmm``, ``scotland_sepa``, ``belgium_wallonia``). The flow
is always the same:

1. ``getTimeseriesList`` filtered by the discharge ``stationparameter_name``
   builds a ``{station_no: {ts_name: ts_id}}`` map.
2. ``getStationList`` gives station metadata; keep only stations that have a
   discharge series, attaching ``river_name`` when the field is requested.
3. ``getTimeseriesValues`` for the station's preferred cadence returns the
   ``[timestamp, value, quality_code]`` rows.

Subclasses set the class attributes below and may override ``_map_quality``.
This base is not registered and is never instantiated directly.
"""

from __future__ import annotations

import asyncio
from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk

logger = structlog.get_logger()

_TRANSIENT_STATUS = (502, 503, 504)


class KiWISConnector(BaseConnector):
    """Base class for KiWIS-backed discharge connectors."""

    # -- required per subclass ------------------------------------------------
    _KIWIS_PATH: str
    _DISCHARGE_PARAM: str  # stationparameter_name value, e.g. Q / Flow / Débit
    _country: str  # ISO 3166-1 alpha-2 for the stations

    # -- optional tuning ------------------------------------------------------
    _STATION_FIELDS: str = (
        "station_no,station_name,station_latitude,station_longitude"
    )
    # Cadence preference for selecting a station's discharge series.
    _TS_PREFERENCE: tuple[str, ...] = ()
    # Restrict getTimeseriesList to one ts_name (keeps the response small on
    # capacity-limited hosts). None = fetch every discharge cadence.
    _TS_NAME_FILTER: str | None = None
    # Timeout for the (potentially large) getTimeseriesList call only.
    _LIST_TIMEOUT: float | None = None
    # >0 enables local retry of transient 5xx responses (for flaky hosts).
    _TRANSIENT_RETRIES: int = 0
    # Some hosts spell the values request with a lowercase "v".
    _VALUES_REQUEST: str = "getTimeseriesValues"
    # If set, empty station coordinates become this value instead of skipping.
    _DEFAULT_COORD: float | None = None

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        # station_no -> {ts_name: ts_id} for discharge series.
        self._series: dict[str, dict[str, str]] | None = None

    # -- public API -----------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return stations that have a discharge timeseries."""
        series = await self._load_series()
        resp = await self._get_kiwis({
            "request": "getStationList",
            "returnfields": self._STATION_FIELDS,
        })
        return self._parse_stations(resp.json(), series)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for a station over a time range."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        ts_id = await self._resolve_ts_id(native_id)
        resp = await self._get_kiwis({
            "request": self._VALUES_REQUEST,
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

    # -- KiWIS request --------------------------------------------------------

    async def _get_kiwis(
        self, params: dict, timeout: float | None = None,
    ) -> httpx.Response:
        """GET the KiWIS endpoint with common params, retrying transient 5xx."""
        full = {
            "service": "kisters",
            "type": "queryServices",
            "datasource": "0",
            "format": "json",
            **params,
        }
        if self._TRANSIENT_RETRIES <= 0:
            return await self._get(self._KIWIS_PATH, params=full, timeout=timeout)

        last_exc: httpx.HTTPStatusError | None = None
        for attempt in range(self._TRANSIENT_RETRIES):
            try:
                return await self._get(self._KIWIS_PATH, params=full, timeout=timeout)
            except httpx.HTTPStatusError as exc:
                if exc.response.status_code not in _TRANSIENT_STATUS:
                    raise
                last_exc = exc
                logger.warning(
                    "kiwis_transient",
                    provider=self.slug, status=exc.response.status_code,
                    attempt=attempt + 1,
                )
                await asyncio.sleep(1.0 * (attempt + 1))
        raise ConnectorError(
            self.slug,
            f"KiWIS host unavailable after {self._TRANSIENT_RETRIES} attempts",
        ) from last_exc

    # -- series map -----------------------------------------------------------

    async def _load_series(self) -> dict[str, dict[str, str]]:
        """Load and cache {station_no: {ts_name: ts_id}} for discharge series."""
        if self._series is not None:
            return self._series
        params = {
            "request": "getTimeseriesList",
            "stationparameter_name": self._DISCHARGE_PARAM,
            "returnfields": "station_no,ts_id,ts_name",
        }
        if self._TS_NAME_FILTER:
            params["ts_name"] = self._TS_NAME_FILTER
        resp = await self._get_kiwis(params, timeout=self._LIST_TIMEOUT)
        self._series = self._parse_series_list(resp.json())
        return self._series

    def _parse_series_list(self, data: list) -> dict[str, dict[str, str]]:
        result: dict[str, dict[str, str]] = {}
        if not data or len(data) < 2:
            return result
        columns: list[str] = data[0]
        try:
            i_no = columns.index("station_no")
            i_id = columns.index("ts_id")
            i_name = columns.index("ts_name")
        except ValueError as exc:
            raise DataFormatError(
                self.slug, f"Unexpected column layout in timeseries list: {columns}",
            ) from exc
        for row in data[1:]:
            try:
                station_no = str(row[i_no])
                ts_id = str(row[i_id])
                ts_name = str(row[i_name])
                if station_no and ts_id:
                    result.setdefault(station_no, {})[ts_name] = ts_id
            except (IndexError, TypeError):
                continue
        return result

    async def _resolve_ts_id(self, native_id: str) -> str:
        series = await self._load_series()
        station_series = series.get(native_id)
        if station_series:
            for ts_name in self._TS_PREFERENCE:
                if ts_name in station_series:
                    return station_series[ts_name]
            return next(iter(station_series.values()))
        raise ConnectorError(
            self.slug,
            f"No discharge ({self._DISCHARGE_PARAM}) timeseries "
            f"for station '{native_id}'",
        )

    # -- parsing --------------------------------------------------------------

    def _parse_stations(
        self, data: list, series: dict[str, dict[str, str]],
    ) -> list[Station]:
        if not data or len(data) < 2:
            return []
        columns: list[str] = data[0]
        try:
            i_no = columns.index("station_no")
            i_name = columns.index("station_name")
            i_lat = columns.index("station_latitude")
            i_lon = columns.index("station_longitude")
        except ValueError as exc:
            raise DataFormatError(
                self.slug, f"Unexpected column layout in station list: {columns}",
            ) from exc
        i_river = columns.index("river_name") if "river_name" in columns else None

        stations: list[Station] = []
        for row in data[1:]:
            try:
                native_id = str(row[i_no])
                if not native_id or native_id not in series:
                    continue
                lat = self._coord(row[i_lat])
                lon = self._coord(row[i_lon])
                if lat is None or lon is None:
                    continue
                river = (
                    str(row[i_river]).strip()
                    if i_river is not None and row[i_river]
                    else None
                )
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=str(row[i_name] or "").strip() or native_id,
                    latitude=lat,
                    longitude=lon,
                    country_code=self._country,
                    river=river,
                ))
            except (IndexError, TypeError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug, row=row, error=str(exc),
                )
                continue
        return stations

    def _coord(self, value: object) -> float | None:
        if value is None or value == "":
            return self._DEFAULT_COORD
        try:
            return float(str(value))
        except (ValueError, TypeError):
            return None

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
            quality = self._map_quality(entry[2] if len(entry) > 2 else None)
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

    @staticmethod
    def _map_quality(code: object) -> QualityFlag:
        """Default KISTERS quality mapping; subclasses may override."""
        if code is None:
            return QualityFlag.MISSING
        try:
            value = int(str(code))
        except (ValueError, TypeError):
            return QualityFlag.RAW
        if value in (130, 255):
            return QualityFlag.MISSING
        if value <= 40:
            return QualityFlag.GOOD
        return QualityFlag.RAW


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
