"""CAMELS-PE connector — Peruvian large-sample hydrology (Zenodo, daily).

CAMELS-PE (Llauca et al. 2026) provides daily observed streamflow for 136
Peruvian catchments (Pacific / Atlantic / Titicaca regions), keyed by the gauge
id (e.g. ``PE_211408``).

A published, DOI-pinned dataset artifact (Zenodo, CC-BY-4.0). A single bundle is
auto-downloaded + checksum-verified via :func:`csfs.core.downloads.ensure_dataset`
(slug ``camels_pe``):

* per-catchment ``CAMELS-PE/03_timeseries/by_catchment/{gauge_id}.csv`` with
  ``date,prec,...,flow_obs,flow_sim,...``. **``flow_obs`` is OBSERVED streamflow
  in mm/day** (``flow_sim`` is PISCO-ARNOVIC simulation and is ignored); ``NA`` =
  missing. mm/day is converted to m³/s using the catchment area:
  ``Q[m³/s] = flow_obs[mm/day] × area[km²] / 86.4``;
* catchment areas from ``02_attributes/topographic_attributes.csv`` (``area``,
  km²) and gauge coordinates from ``01_metadata/stations.csv``
  (``gauge_lat`` / ``gauge_lon``, already WGS84).
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

_LANDING = "https://doi.org/10.5281/zenodo.20058778"
_SLUG = "camels_pe"
_STATIONS_CSV = "stations.csv"
_TOPO_CSV = "topographic_attributes.csv"
# mm/day over a km² catchment -> m³/s: 1 mm/day · 1 km² = 1e3 m³/day = 1e3/86400 m³/s.
_MM_KM2_PER_DAY_TO_M3S = 1.0 / 86.4


@register("camels_pe")
class CAMELSPEConnector(BaseConnector):
    """Connector for CAMELS-PE (Peru) — authoritative standalone."""

    slug = "camels_pe"
    display_name = "CAMELS-PE (Peru)"
    base_url = "https://zenodo.org"  # data via ensure_dataset
    country_codes = ["PE"]

    async def fetch_stations(self) -> list[Station]:
        """Catalogue from stations.csv (gauge_lat/gauge_lon, already WGS84)."""
        data_dir = await ensure_dataset(_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_pe_no_data", hint=f"Download from {_LANDING}")
            return []
        stations_csv = self._find_one(Path(data_dir), _STATIONS_CSV)
        if stations_csv is None:
            logger.info("camels_pe_stations_not_found", data_dir=str(data_dir))
            return []
        stations: list[Station] = []
        with open(stations_csv, newline="", encoding="utf-8") as fh:
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
                    country_code="PE",
                ))
        logger.info("camels_pe_stations_loaded", count=len(stations))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Read daily observed streamflow (flow_obs, mm/day → m³/s) for one gauge."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = await ensure_dataset(_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_pe_no_data", station=native_id, hint=f"Download from {_LANDING}")
            return self._empty_chunk(station_id)
        f = self._find_one(Path(data_dir), f"{native_id}.csv")
        if f is None:
            logger.info("camels_pe_file_not_found", station=native_id, data_dir=str(data_dir))
            return self._empty_chunk(station_id)
        area_km2 = self._area_for(Path(data_dir), native_id)
        if area_km2 is None:
            logger.info("camels_pe_no_area", station=native_id)
            return self._empty_chunk(station_id)

        start_aware = start if start.tzinfo else start.replace(tzinfo=UTC)
        end_aware = end if end.tzinfo else end.replace(tzinfo=UTC)
        observations = self._parse_timeseries(f, station_id, area_km2, start_aware, end_aware)
        logger.info(
            "camels_pe_observations_loaded",
            station=native_id, count=len(observations), file=str(f),
        )
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # ------------------------------------------------------------------

    def _area_for(self, data_dir: Path, gauge_id: str) -> float | None:
        """Catchment area (km²) for one gauge from topographic_attributes.csv."""
        topo = self._find_one(data_dir, _TOPO_CSV)
        if topo is None:
            return None
        with open(topo, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                if (row.get("gauge_id") or "").strip() == gauge_id:
                    try:
                        return float(row["area"])
                    except (KeyError, TypeError, ValueError):
                        return None
        return None

    def _empty_chunk(self, station_id: str) -> TimeSeriesChunk:
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )

    @staticmethod
    def _find_one(data_dir: Path, name: str) -> Path | None:
        hits = list(data_dir.rglob(name))
        return hits[0] if hits else None

    @staticmethod
    def _parse_timeseries(
        path: Path, station_id: str, area_km2: float, start: datetime, end: datetime,
    ) -> list[Observation]:
        observations: list[Observation] = []
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            cols = reader.fieldnames or []
            if not {"date", "flow_obs"} <= set(cols):
                return observations
            for row in reader:
                raw_date = (row.get("date") or "").strip()
                if not raw_date:
                    continue
                try:
                    ts = datetime.strptime(raw_date[:10], "%Y-%m-%d").replace(tzinfo=UTC)
                except ValueError:
                    continue
                if not (start <= ts <= end):
                    continue
                raw = (row.get("flow_obs") or "").strip()
                discharge: float | None
                quality: QualityFlag
                if not raw or raw.upper() in ("NA", "NAN"):
                    discharge, quality = None, QualityFlag.MISSING
                else:
                    try:
                        mm_per_day = float(raw)
                        if mm_per_day < 0:
                            discharge, quality = None, QualityFlag.MISSING
                        else:
                            discharge = mm_per_day * area_km2 * _MM_KM2_PER_DAY_TO_M3S
                            quality = QualityFlag.RAW
                    except ValueError:
                        discharge, quality = None, QualityFlag.MISSING
                observations.append(Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=quality,
                ))
        return observations
