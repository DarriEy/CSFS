"""CAMELS-COL connector — Colombian large-sample hydrology (manual / access-gated).

CAMELS-COL (Jimenez et al. 2025) provides daily observed streamflow for 347
Colombian (IDEAM) catchments, keyed by the IDEAM station code (e.g.
``13077030``).

ACCESS-GATED. Despite a CC-BY-4.0 licence, the Zenodo files (record
10.5281/zenodo.15554735) are published ``access_right: restricted`` — the files
endpoint returns HTTP 403 and requires a manual "Request access". The dataset
therefore cannot be auto-downloaded or checksum-verified, so it is NOT part of
the provenance-gated dataset-artifact tier. It is wired in as a MANUAL connector:
point ``config['data_dir']`` at a locally obtained copy and it will read it.

.. warning::
   The streamflow filename pattern and column names below follow the documented
   CAMELS-BR/CL-style layout but are **UNVERIFIED against the real archive**
   (no access at build time). Confirm and adjust once access is granted.
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

_LANDING = "https://doi.org/10.5281/zenodo.15554735"
_SLUG = "camels_col"
# Documented-but-unverified conventions (see module warning).
_STREAMFLOW_COLS = ("streamflow", "q_obs", "discharge", "q")
_DATE_COLS = ("date", "time", "datetime")
_ATTRS_GLOB = "*attributes*.csv"


@register("camels_col")
class CAMELSCOLConnector(BaseConnector):
    """Connector for CAMELS-COL (Colombia) — manual / access-gated standalone."""

    slug = "camels_col"
    display_name = "CAMELS-COL (Colombia)"
    base_url = "https://zenodo.org"  # access-gated; data via local data_dir
    country_codes = ["CO"]

    async def fetch_stations(self) -> list[Station]:
        """Catalogue from the attributes table (gauge_lat/gauge_lon, WGS84)."""
        data_dir = await ensure_dataset(_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_col_no_data", hint=f"Request access + download from {_LANDING}")
            return []
        attrs = self._find_one(Path(data_dir), _ATTRS_GLOB)
        if attrs is None:
            logger.info("camels_col_attributes_not_found", data_dir=str(data_dir))
            return []
        stations: list[Station] = []
        with open(attrs, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                gid = (row.get("gauge_id") or "").strip()
                try:
                    lat = float(row["gauge_lat"])
                    lon = float(row["gauge_lon"])
                except (KeyError, TypeError, ValueError):
                    continue
                if not gid:
                    continue
                stations.append(Station(
                    id=self._station_id(gid),
                    provider=self.slug,
                    native_id=gid,
                    name=(row.get("gauge_name") or gid).strip(),
                    latitude=lat,
                    longitude=lon,
                    country_code="CO",
                ))
        logger.info("camels_col_stations_loaded", count=len(stations))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Read daily observed streamflow (m³/s) for one IDEAM station."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = await ensure_dataset(_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_col_no_data", station=native_id,
                        hint=f"Request access + download from {_LANDING}")
            return self._empty_chunk(station_id)
        f = self._find_one(Path(data_dir), f"*{native_id}*.csv", exclude_glob=_ATTRS_GLOB)
        if f is None:
            logger.info("camels_col_file_not_found", station=native_id, data_dir=str(data_dir))
            return self._empty_chunk(station_id)

        start_aware = start if start.tzinfo else start.replace(tzinfo=UTC)
        end_aware = end if end.tzinfo else end.replace(tzinfo=UTC)
        observations = self._parse_timeseries(f, station_id, start_aware, end_aware)
        logger.info(
            "camels_col_observations_loaded",
            station=native_id, count=len(observations), file=str(f),
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
    def _find_one(data_dir: Path, pattern: str, exclude_glob: str | None = None) -> Path | None:
        import fnmatch
        for p in sorted(data_dir.rglob(pattern)):
            if exclude_glob and fnmatch.fnmatch(p.name, exclude_glob):
                continue
            return p
        return None

    @classmethod
    def _parse_timeseries(
        cls, path: Path, station_id: str, start: datetime, end: datetime,
    ) -> list[Observation]:
        observations: list[Observation] = []
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            cols = reader.fieldnames or []
            date_col = next((c for c in _DATE_COLS if c in cols), None)
            q_col = next((c for c in _STREAMFLOW_COLS if c in cols), None)
            if date_col is None or q_col is None:
                return observations
            for row in reader:
                raw_date = (row.get(date_col) or "").strip()
                ts = _parse_date(raw_date)
                if ts is None or not (start <= ts <= end):
                    continue
                raw = (row.get(q_col) or "").strip()
                discharge: float | None
                quality: QualityFlag
                if not raw or raw.lower() in ("nan", "na"):
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


def _parse_date(raw: str) -> datetime | None:
    if not raw:
        return None
    for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%d/%m/%Y"):
        try:
            return datetime.strptime(raw[:10], fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    return None
