"""CAMELS-AUS connector — Australian large-sample hydrology (Zenodo, daily).

CAMELS-AUS v2 (Fowler et al.) provides daily streamflow for Australian
catchments, keyed by the AWRC station id (e.g. ``912101A``).

A published, DOI-pinned dataset artifact (Zenodo 13350616, CC-BY-4.0). The
streamflow is a single WIDE MATRIX in **ML/day** (megalitres per day) with
separate year/month/day columns; it is converted to m³/s on read. Two resources
are auto-downloaded + checksum-verified via
:func:`csfs.core.downloads.ensure_dataset`:

* ``camels_aus`` — ``03_streamflow.zip`` → ``streamflow_MLd.csv`` (comma-sep;
  ``year,month,day`` then one column per station id, ML/day; missing = -99.99);
* ``camels_aus_attributes`` — the bare ``CAMELS_AUS_Attributes&Indices_Master
  Table.csv`` (row per station; ``station_id,...,lat_outlet,long_outlet``).
"""

from __future__ import annotations

import csv
from datetime import UTC, datetime
from pathlib import Path

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.downloads import ensure_dataset
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

_ZENODO_URL = "https://zenodo.org/records/13350616"
_STREAMFLOW_SLUG = "camels_aus"
_ATTRIBUTES_SLUG = "camels_aus_attributes"
#: 1 ML/day = 1000 m³ / 86400 s.
_MLD_TO_M3S = 1000.0 / 86400.0
_MISSING = -99.99


@register("camels_aus")
class CAMELSAUSConnector(BaseConnector):
    """Connector for CAMELS-AUS (Australia)."""

    slug = "camels_aus"
    display_name = "CAMELS-AUS (Australia)"
    base_url = "https://zenodo.org/api"  # data via ensure_dataset
    country_codes = ["AU"]

    async def fetch_stations(self) -> list[Station]:
        """Catalogue from the master attributes table (real AWRC outlet coords)."""
        data_dir = await ensure_dataset(_ATTRIBUTES_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_aus_no_attributes", hint=f"Download from {_ZENODO_URL}")
            return []
        master = self._find_one(Path(data_dir), "MasterTable")
        if master is None:
            return []
        stations: list[Station] = []
        with open(master, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                gid = (row.get("station_id") or row.get("gauge_id") or "").strip()
                try:
                    lat = float(row["lat_outlet"])
                    lon = float(row["long_outlet"])
                except (KeyError, TypeError, ValueError):
                    continue
                if not gid:
                    continue
                stations.append(Station(
                    id=self._station_id(gid),
                    provider=self.slug,
                    native_id=gid,
                    name=row.get("station_name") or gid,
                    latitude=lat,
                    longitude=lon,
                    country_code="AU",
                ))
        logger.info("camels_aus_stations_loaded", count=len(stations))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Read one station's ML/day column from the matrix, converted to m³/s."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = await ensure_dataset(_STREAMFLOW_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_aus_no_data", station=native_id, hint=f"Download from {_ZENODO_URL}")
            return self._empty_chunk(station_id)
        matrix = self._find_one(Path(data_dir), "streamflow_MLd.csv")
        if matrix is None:
            logger.info("camels_aus_file_not_found", station=native_id, data_dir=str(data_dir))
            return self._empty_chunk(station_id)

        start_aware = start if start.tzinfo else start.replace(tzinfo=UTC)
        end_aware = end if end.tzinfo else end.replace(tzinfo=UTC)
        observations = self._parse_matrix(matrix, native_id, station_id, start_aware, end_aware)
        logger.info(
            "camels_aus_observations_loaded",
            station=native_id, count=len(observations), file=str(matrix),
        )
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # ------------------------------------------------------------------

    def _empty_chunk(self, station_id: str) -> TimeSeriesChunk:
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )

    @staticmethod
    def _find_one(data_dir: Path, name_fragment: str) -> Path | None:
        hits = [p for p in data_dir.rglob("*") if p.is_file() and name_fragment in p.name]
        return hits[0] if hits else None

    @staticmethod
    def _parse_matrix(
        path: Path, native_id: str, station_id: str, start: datetime, end: datetime,
    ) -> list[Observation]:
        observations: list[Observation] = []
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.reader(fh)
            header = next(reader, None)
            if header is None:
                return observations
            try:
                col = header.index(native_id)
                iy, im, idd = header.index("year"), header.index("month"), header.index("day")
            except ValueError:
                return observations
            for cols in reader:
                if col >= len(cols):
                    continue
                try:
                    ts = datetime(int(cols[iy]), int(cols[im]), int(cols[idd]), tzinfo=UTC)
                except (ValueError, IndexError):
                    continue
                if not (start <= ts <= end):
                    continue
                raw = cols[col].strip()
                discharge: float | None
                quality: QualityFlag
                try:
                    mld = float(raw)
                    if mld <= _MISSING or mld < 0:  # -99.99 sentinel (and any negative)
                        discharge, quality = None, QualityFlag.MISSING
                    else:
                        discharge, quality = mld * _MLD_TO_M3S, QualityFlag.RAW
                except ValueError:
                    discharge, quality = None, QualityFlag.MISSING
                observations.append(Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=quality,
                ))
        return observations
