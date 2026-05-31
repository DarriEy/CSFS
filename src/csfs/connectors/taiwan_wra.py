# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""WRA connector — Taiwan Water Resources Agency open-data API (v2, keyless).

Taiwan's WRA moved to a keyless v2 open-data platform
(https://opendata.wra.gov.tw/api/v2). That platform exposes the river-flow
gauge network only as *station status* (metadata, no values); the actual
gauge discharge remains on the old v1 API, which now requires an undocumented
key. The only discharge **values** available keyless are reservoir/weir inflow
from the "Reservoir hydrological data" dataset — and several of those points
are run-of-river weirs (Shigang Dam, Jiji Weir, Jiaxian Weir) whose inflow is
river discharge.

This connector therefore serves reservoir/weir **inflow discharge** for the
major mainland reservoirs. The dataset has no coordinates, so a curated seed of
WGS84 locations is used (the reservoirs are large, well-known features).
Timestamps are Taiwan local time (UTC+8).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

_TAIPEI = timezone(timedelta(hours=8))

# Reservoir hydrological data (inflow discharge, hourly, last ~24 h).
_RESERVOIR_DATASET = "2be9044c-6e44-4856-aad5-dd108c2e6679"

# Curated WGS84 seed for the major reservoirs/weirs that publish inflow
# discharge (the API carries no coordinates). reservoiridentifier -> (name,
# river, lat, lon). Weirs are run-of-river structures (inflow == river flow).
_RESERVOIR_SEED: dict[str, tuple[str, str | None, float, float]] = {
    "10201": ("Shimen Reservoir", "Dahan River", 24.812, 121.243),
    "10205": ("Feitsui Reservoir", "Beishi River", 24.910, 121.572),
    "10501": ("Yongheshan Reservoir", "Zhonggang River", 24.620, 120.920),
    "20101": ("Liyutan Reservoir", "Jing River", 24.323, 120.778),
    "20201": ("Deji Reservoir", "Dajia River", 24.252, 121.165),
    "20202": ("Shigang Dam", "Dajia River", 24.255, 120.770),
    "20503": ("Jiji Weir", "Zhuoshui River", 23.847, 120.783),
    "20509": ("Hushan Reservoir", "Qingshui River", 23.667, 120.560),
    "30301": ("Renyitan Reservoir", "Bazhang River", 23.517, 120.430),
    "30302": ("Lantan Reservoir", "Bazhang River", 23.475, 120.490),
    "30502": ("Zengwen Reservoir", "Zengwen River", 23.230, 120.530),
    "30802": ("Agongdian Reservoir", "Agongdian River", 22.781, 120.397),
    "31002": ("Jiaxian Weir", "Qishan River", 23.077, 120.591),
    "31201": ("Mudan Reservoir", "Sichongxi River", 22.203, 120.781),
}


@register("taiwan_wra")
class TaiwanWRAConnector(BaseConnector):
    """Connector for Taiwan WRA reservoir/weir inflow discharge (keyless v2)."""

    slug = "taiwan_wra"
    display_name = "WRA (Taiwan)"
    base_url = "https://opendata.wra.gov.tw/api/v2"
    country_codes = ["TW"]

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        self._records: list[dict] | None = None

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return the seeded reservoirs/weirs that are present in the feed."""
        records = await self._load_records()
        present = {str(r.get("reservoiridentifier", "")) for r in records}
        stations = [
            self._to_station(rid, *meta)
            for rid, meta in _RESERVOIR_SEED.items()
            if rid in present
        ]
        logger.info("stations_fetched", provider=self.slug, count=len(stations))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch inflow-discharge observations for one reservoir/weir."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        records = await self._load_records()

        observations: list[Observation] = []
        for rec in records:
            if str(rec.get("reservoiridentifier", "")) != native_id:
                continue
            ts = _parse_local(rec.get("observationtime"))
            if ts is None or not (start <= ts <= end):
                continue
            discharge = _to_float(rec.get("inflowdischarge"))
            if discharge is None:
                continue
            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=discharge,
                quality=QualityFlag.RAW,
            ))

        observations.sort(key=lambda o: o.timestamp)
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent inflow observations (last 24 h)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id, start=now - timedelta(hours=24), end=now,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _load_records(self) -> list[dict]:
        """Fetch and cache the reservoir hydrological dataset."""
        if self._records is None:
            resp = await self._get(f"/{_RESERVOIR_DATASET}")
            data = resp.json()
            if not isinstance(data, list):
                raise DataFormatError(
                    self.slug, "Reservoir dataset did not return a list",
                )
            self._records = data
        return self._records

    def _to_station(
        self, rid: str, name: str, river: str | None, lat: float, lon: float,
    ) -> Station:
        return Station(
            id=self._station_id(rid),
            provider=self.slug,
            native_id=rid,
            name=name,
            latitude=lat,
            longitude=lon,
            country_code="TW",
            river=river,
        )


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    text = str(value).strip()
    if text in ("", "-", "--"):
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_local(value: object) -> datetime | None:
    """Parse a WRA local timestamp (UTC+8) into a UTC datetime."""
    if not isinstance(value, str) or not value.strip():
        return None
    text = value.strip()
    for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M"):
        try:
            naive = datetime.strptime(text, fmt)
        except ValueError:
            continue
        return naive.replace(tzinfo=_TAIPEI).astimezone(UTC)
    return None
