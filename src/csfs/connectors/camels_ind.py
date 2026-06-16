"""CAMELS-IND connector — Indian large-sample hydrology (Zenodo, daily).

CAMELS-IND (v2.2, Mangukiya et al.) provides daily streamflow for Indian
catchments, keyed by the CWC gauge code (e.g. ``3002``).

A published, DOI-pinned dataset artifact (Zenodo 14999580, CC-BY-4.0). The
streamflow is a single WIDE MATRIX with separate year/month/day columns; the
authoritative standalone slug is ``camels_ind`` (distinct from the
Caravan-derived ``camels_in`` India alias). Auto-downloaded + checksum-verified
via :func:`csfs.core.downloads.ensure_dataset`:

* observations — ``streamflow_timeseries/streamflow_observed.csv`` (comma-sep;
  ``year,month,day`` then one column per gauge id, m³/s; missing = empty cell);
* catalogue — ``attributes_csv/camels_ind_topo.csv`` (row per gauge;
  ``gauge_id,cwc_lat,cwc_lon,...``).
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

_ZENODO_URL = "https://zenodo.org/records/14999580"
_SLUG = "camels_ind"  # single archive holds streamflow + attributes


@register("camels_ind")
class CAMELSINDConnector(BaseConnector):
    """Connector for CAMELS-IND (India) — authoritative standalone."""

    slug = "camels_ind"
    display_name = "CAMELS-IND (India)"
    base_url = "https://zenodo.org/api"  # data via ensure_dataset
    country_codes = ["IN"]

    async def fetch_stations(self) -> list[Station]:
        """Catalogue from ``camels_ind_topo.csv`` (row per gauge, CWC coords)."""
        data_dir = await ensure_dataset(_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_ind_no_data", hint=f"Download from {_ZENODO_URL}")
            return []
        topo = self._find_file(Path(data_dir), "camels_ind_topo.csv")
        if topo is None:
            return []
        stations: list[Station] = []
        with open(topo, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                gid = (row.get("gauge_id") or "").strip()
                try:
                    lat = float(row["cwc_lat"])
                    lon = float(row["cwc_lon"])
                except (KeyError, TypeError, ValueError):
                    continue
                if not gid:
                    continue
                stations.append(Station(
                    id=self._station_id(gid),
                    provider=self.slug,
                    native_id=gid,
                    name=gid,
                    latitude=lat,
                    longitude=lon,
                    country_code="IN",
                ))
        logger.info("camels_ind_stations_loaded", count=len(stations))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Read one gauge's daily discharge column out of the wide matrix."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = await ensure_dataset(_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_ind_no_data", station=native_id, hint=f"Download from {_ZENODO_URL}")
            return self._empty_chunk(station_id)
        matrix = self._find_file(Path(data_dir), "streamflow_observed.csv")
        if matrix is None:
            logger.info("camels_ind_file_not_found", station=native_id, data_dir=str(data_dir))
            return self._empty_chunk(station_id)

        start_aware = start if start.tzinfo else start.replace(tzinfo=UTC)
        end_aware = end if end.tzinfo else end.replace(tzinfo=UTC)
        observations = self._parse_matrix(matrix, native_id, station_id, start_aware, end_aware)
        logger.info(
            "camels_ind_observations_loaded",
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
    def _find_file(data_dir: Path, name: str) -> Path | None:
        hits = list(data_dir.rglob(name))
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
                col = header.index(native_id)  # gauge column for this station
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
                if not raw:  # empty cell = missing
                    discharge, quality = None, QualityFlag.MISSING
                else:
                    try:
                        discharge = float(raw)
                        quality = QualityFlag.RAW
                        if discharge < 0:
                            discharge, quality = None, QualityFlag.MISSING
                    except ValueError:
                        discharge, quality = None, QualityFlag.MISSING
                observations.append(Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=quality,
                ))
        return observations
