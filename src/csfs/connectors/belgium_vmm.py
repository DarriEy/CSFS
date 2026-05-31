# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""VMM Waterinfo connector — Belgian Flanders water data via KiWIS."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# KiWIS quality code ranges (same convention as BOM)
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


@register("belgium_vmm")
class BelgiumVmmConnector(BaseConnector):
    """Connector for the VMM Waterinfo KiWIS service (Belgium/Flanders)."""

    slug = "belgium_vmm"
    display_name = "VMM Waterinfo (Belgium)"
    base_url = "https://download.waterinfo.be"
    country_codes = ["BE"]

    _KIWIS_PATH = "/tsmdownload/KiWIS/KiWIS"

    # Station metadata only — do NOT request parametertype_name here: it
    # explodes the response to one row per (station x parameter) and floods
    # it with non-discharge series (rainfall, conductivity, drought indices).
    _STATION_FIELDS = (
        "station_no,station_name,station_latitude,station_longitude"
    )

    # Discharge timeseries selection. VMM publishes many cadences per station
    # under the canonical Q parameter; pick the best available in this order:
    #   P.15      validated ("Productie") real-time 15-minute discharge
    #   DagGem    daily mean discharge
    #   Basis.15  base 15-minute series
    #   Basis     base series
    #   O.15      raw ("Origineel") 15-minute series
    _TS_PREFERENCE = ("P.15", "DagGem", "Basis.15", "Basis", "O.15")

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        # station_no -> {ts_name: ts_id} for canonical discharge (Q) series.
        self._q_series: dict[str, dict[str, str]] | None = None

    async def fetch_stations(self) -> list[Station]:
        """Return VMM stations that have a canonical discharge (Q) timeseries."""
        q_series = await self._load_q_series()

        resp = await self._get(
            self._KIWIS_PATH,
            params={
                "service": "kisters",
                "type": "queryServices",
                "request": "getStationList",
                "datasource": "0",
                "format": "json",
                "returnFields": self._STATION_FIELDS,
            },
        )
        return self._parse_stations(resp.json(), q_series)

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
                "type": "queryServices",
                "service": "kisters",
                "request": "getTimeseriesvalues",
                "ts_id": ts_id,
                "format": "json",
                "datasource": "0",
                "returnfields": "Timestamp,Value,Quality Code",
                "from": start.isoformat(),
                "to": end.isoformat(),
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

    def _parse_stations(
        self, data: list, q_series: dict[str, dict[str, str]],
    ) -> list[Station]:
        """Parse the KiWIS station list response.

        The response is a list where the first element is a column-header
        array and subsequent elements are data rows (positional arrays).
        Only stations that have a canonical discharge (Q) timeseries are
        returned — VMM hosts ~1,872 stations but only ~195 measure discharge.
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
                if not native_id or native_id not in q_series:
                    continue

                lat = (
                    float(str(row[idx_lat]))
                    if row[idx_lat] not in (None, "")
                    else 0.0
                )
                lon = (
                    float(str(row[idx_lon]))
                    if row[idx_lon] not in (None, "")
                    else 0.0
                )
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
        """Parse the KiWIS getTimeseriesvalues response.

        The response is a list containing a single dict with a ``data``
        key whose value is a list of [timestamp, value, quality_code]
        arrays.
        """
        observations: list[Observation] = []

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
                discharge = (
                    float(str(raw_value))
                    if raw_value is not None
                    else None
                )
            except (ValueError, TypeError):
                discharge = None

            quality = _map_quality(
                str(quality_code) if quality_code is not None else None,
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

    async def _load_q_series(self) -> dict[str, dict[str, str]]:
        """Load and cache the map of discharge (Q) timeseries per station.

        A single filtered ``getTimeseriesList`` call (``stationparameter_name=Q``)
        returns every canonical discharge series across all stations — far
        cheaper than per-station queries and avoids selecting a non-discharge
        series by accident. Uses an extended timeout: this response is large
        (~9k rows). The ``coverage`` field is deliberately omitted because it
        forces an expensive server-side span computation that times out.
        """
        if self._q_series is not None:
            return self._q_series

        resp = await self._get(
            self._KIWIS_PATH,
            params={
                "service": "kisters",
                "type": "queryServices",
                "request": "getTimeseriesList",
                "datasource": "0",
                "format": "json",
                "stationparameter_name": "Q",
                "returnFields": "station_no,ts_id,ts_name",
            },
            timeout=180.0,
        )
        self._q_series = self._parse_q_series(resp.json())
        return self._q_series

    def _parse_q_series(self, data: list) -> dict[str, dict[str, str]]:
        """Parse the filtered getTimeseriesList response into station->series."""
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
                self.slug,
                f"Unexpected column layout in timeseries list: {columns}",
            ) from exc

        for row in data[1:]:
            try:
                station_no = str(row[idx_no])
                ts_id = str(row[idx_ts_id])
                ts_name = str(row[idx_name])
                if station_no and ts_id:
                    result.setdefault(station_no, {})[ts_name] = ts_id
            except (IndexError, TypeError) as exc:
                logger.warning(
                    "ts_list_parse_failed",
                    provider=self.slug,
                    row=row,
                    error=str(exc),
                )
                continue
        return result

    async def _resolve_ts_id(self, native_id: str) -> str:
        """Return the KiWIS ts_id for a station's preferred discharge series."""
        q_series = await self._load_q_series()
        series = q_series.get(native_id)
        if series:
            for ts_name in self._TS_PREFERENCE:
                if ts_name in series:
                    return series[ts_name]
            # No preferred cadence matched; fall back to any discharge series.
            return next(iter(series.values()))

        raise ConnectorError(
            self.slug,
            f"No discharge (Q) timeseries found for station '{native_id}'",
        )
