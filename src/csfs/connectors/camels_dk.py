"""CAMELS-DK connector — Danish large-sample hydrology (GEUS Dataverse, daily).

CAMELS-DK (Liu et al. 2024) provides daily streamflow for 3330 Danish
catchments, keyed by the catchment id (e.g. ``12410011``).

A published, DOI-pinned dataset artifact (GEUS Dataverse, CC0-1.0). Two
resources are auto-downloaded + checksum-verified via
:func:`csfs.core.downloads.ensure_dataset`:

* ``camels_dk`` — ``Gauged_catchments.zip`` → per-catchment
  ``CAMELS_DK_obs_based_{id}.csv`` (comma-sep; ``time`` ISO, observed discharge
  ``Qobs`` in m³/s; blank = missing);
* ``camels_dk_attributes`` — the bare ``CAMELS_DK_topography.csv``
  (``catch_id,catch_outlet_lon,catch_outlet_lat,...``). The outlet coordinates
  are **easting/northing in ETRS89 / UTM 32N (EPSG:25832)** despite the
  ``lon/lat`` column names, and are reprojected to WGS84 on read.
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
from csfs.core.reproject import to_wgs84

logger = structlog.get_logger()

_DATAVERSE_URL = "https://dataverse.geus.dk/dataset.xhtml?persistentId=doi:10.22008/FK2/AZXSYP"
_STREAMFLOW_SLUG = "camels_dk"
_ATTRIBUTES_SLUG = "camels_dk_attributes"
_EPSG_DK = 25832  # ETRS89 / UTM zone 32N


@register("camels_dk")
class CAMELSDKConnector(BaseConnector):
    """Connector for CAMELS-DK (Denmark) — authoritative standalone."""

    slug = "camels_dk"
    display_name = "CAMELS-DK (Denmark)"
    base_url = "https://dataverse.geus.dk"  # data via ensure_dataset
    country_codes = ["DK"]

    async def fetch_stations(self) -> list[Station]:
        """Catalogue from ``CAMELS_DK_topography.csv`` (UTM 32N → WGS84)."""
        data_dir = await ensure_dataset(_ATTRIBUTES_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_dk_no_attributes", hint=f"Download from {_DATAVERSE_URL}")
            return []
        topo = self._find_one(Path(data_dir), "CAMELS_DK_topography.csv")
        if topo is None:
            return []
        stations: list[Station] = []
        with open(topo, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                gid = (row.get("catch_id") or "").strip()
                try:
                    easting = float(row["catch_outlet_lon"])
                    northing = float(row["catch_outlet_lat"])
                except (KeyError, TypeError, ValueError):
                    continue
                if not gid:
                    continue
                lat, lon = to_wgs84(easting, northing, _EPSG_DK)
                stations.append(Station(
                    id=self._station_id(gid),
                    provider=self.slug,
                    native_id=gid,
                    name=gid,
                    latitude=lat,
                    longitude=lon,
                    country_code="DK",
                ))
        logger.info("camels_dk_stations_loaded", count=len(stations))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Read daily observed discharge (Qobs) for one catchment."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = await ensure_dataset(_STREAMFLOW_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_dk_no_data", station=native_id, hint=f"Download from {_DATAVERSE_URL}")
            return self._empty_chunk(station_id)
        f = self._find_one(Path(data_dir), f"CAMELS_DK_obs_based_{native_id}.csv")
        if f is None:
            logger.info("camels_dk_file_not_found", station=native_id, data_dir=str(data_dir))
            return self._empty_chunk(station_id)

        start_aware = start if start.tzinfo else start.replace(tzinfo=UTC)
        end_aware = end if end.tzinfo else end.replace(tzinfo=UTC)
        observations = self._parse_timeseries(f, station_id, start_aware, end_aware)
        logger.info(
            "camels_dk_observations_loaded",
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
    def _find_one(data_dir: Path, name: str) -> Path | None:
        hits = list(data_dir.rglob(name))
        return hits[0] if hits else None

    @staticmethod
    def _parse_timeseries(
        path: Path, station_id: str, start: datetime, end: datetime,
    ) -> list[Observation]:
        observations: list[Observation] = []
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames is None or "Qobs" not in reader.fieldnames:
                return observations
            for row in reader:
                raw_date = (row.get("time") or "").strip()
                if not raw_date:
                    continue
                try:
                    ts = datetime.strptime(raw_date[:10], "%Y-%m-%d").replace(tzinfo=UTC)
                except ValueError:
                    continue
                if not (start <= ts <= end):
                    continue
                raw = (row.get("Qobs") or "").strip()
                discharge: float | None
                quality: QualityFlag
                if not raw or raw.lower() == "nan":
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
