"""CAMELSH connector -- hourly US hydrometeorological dataset (Zenodo).

CAMELSH provides hourly streamflow and water level for 9,008 catchments
across the CONUS (1980-2024). Data is distributed on Zenodo across several
records (primary: 15413207 for hourly time series).

This connector supports:

1. **Station catalogue** -- seed list of representative gauges using USGS IDs.

2. **Observations from local files** -- CAMELSH distributes CSVs with
   columns ``date``, ``streamflow_m3s``, and ``water_level_m``.

References
----------
- DOI: 10.5281/zenodo.15413207
- Paper: Tran et al. (2025) – CAMELSH
"""

from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import (
    Observation,
    QualityFlag,
    Station,
    TimeSeriesChunk,
)
from csfs.core.registry import register

logger = structlog.get_logger()

# Zenodo record for CAMELSH Hourly Time Series
_ZENODO_RECORD_ID = "15413207"
_ZENODO_DOWNLOAD_URL = f"https://zenodo.org/records/{_ZENODO_RECORD_ID}"

_SEED_STATIONS: list[dict] = [
    {
        "id": '01013500',
        "name": 'Fish River near Fort Kent',
        "lat": 47.24,
        "lon": -68.58,
        "country": 'US',
        "river": 'Fish River',
    },
    {
        "id": '01137500',
        "name": 'Youghiogheny River at Friendsville',
        "lat": 39.65,
        "lon": -79.4,
        "country": 'US',
        "river": 'Youghiogheny River',
    },
]

@register("camelsh")
class CAMELSHConnector(BaseConnector):
    """Connector for CAMELSH (hourly US dataset)."""

    slug = "camelsh"
    display_name = "CAMELSH (Hourly US)"
    base_url = "https://zenodo.org/api"
    country_codes = ["US"]

    async def fetch_stations(self) -> list[Station]:
        stations: list[Station] = []
        for entry in _SEED_STATIONS:
            stations.append(
                Station(
                    id=self._station_id(entry["id"]),
                    provider=self.slug,
                    native_id=entry["id"],
                    name=entry["name"],
                    latitude=entry["lat"],
                    longitude=entry["lon"],
                    country_code=entry["country"],
                    river=entry.get("river"),
                )
            )
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = self.config.get("data_dir")

        if not data_dir:
            logger.info("camelsh_no_data_dir", station=native_id)
            return self._empty_chunk(station_id)

        data_path = Path(data_dir)
        # CAMELSH often organizes by folder or flat list
        file_path = data_path / f"{native_id}.csv"
        if not file_path.exists():
            return self._empty_chunk(station_id)

        observations: list[Observation] = []
        try:
            with open(file_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    # Expected format: YYYY-MM-DD HH:MM:SS
                    ts_str = row.get("date") or row.get("timestamp")
                    if not ts_str:
                        continue
                        
                    try:
                        ts = datetime.fromisoformat(ts_str.replace(" ", "T")).replace(tzinfo=UTC)
                    except ValueError:
                        continue

                    if start <= ts <= end:
                        q = row.get("streamflow_m3s") or row.get("q")
                        # The Observation model only tracks discharge; water level is
                        # ignored (CSFS is a streamflow service).
                        observations.append(
                            Observation(
                                station_id=station_id,
                                timestamp=ts,
                                discharge_m3s=float(q) if q else None,
                                quality=QualityFlag.RAW,
                            )
                        )
        except Exception as exc:
            raise ConnectorError(self.slug, f"Failed to parse {file_path}: {exc}") from exc

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    def _empty_chunk(self, station_id: str) -> TimeSeriesChunk:
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )
