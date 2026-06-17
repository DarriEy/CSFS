"""CAMELS-SE connector — Swedish large-sample hydrology (SND, daily).

CAMELS-SE (Teutschbein et al. 2024) provides daily observed discharge for 50
Swedish catchments, keyed by the SMHI catchment id (e.g. ``1069``).

A published, DOI-pinned dataset artifact (Swedish National Data Service, SND
2023-173, CC-BY-4.0). Two resources are auto-downloaded + checksum-verified via
:func:`csfs.core.downloads.ensure_dataset`:

* ``camels_se`` — ``catchment time series.zip`` → per-catchment
  ``catchment time series/catchment_id_{id}_{NAME}.csv`` (comma-sep;
  ``Year,Month,Day,Qobs_m3s,...``; observed discharge ``Qobs_m3s`` in m³/s).
  Catchment names carry Swedish characters (Latin-1 in the archive filenames),
  but the numeric id in the filename is ASCII so a glob on the id is exact;
* ``camels_se_gis`` — ``catchment_GIS_shapefiles.zip`` → the gauge point
  shapefile ``Sweden_catchments_50_stations_WGS84.shp`` (already WGS84; the
  ``id`` attribute is the catchment id, the Point geometry is the gauge).

SND publishes no archive checksum; the recorded md5s are self-computed on the
fetched bytes and pinned so re-downloads are integrity-checked.
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

_LANDING = "https://researchdata.se/en/catalogue/dataset/2023-173"
_STREAMFLOW_SLUG = "camels_se"
_GIS_SLUG = "camels_se_gis"
_STATIONS_SHP = "Sweden_catchments_50_stations_WGS84.shp"


@register("camels_se")
class CAMELSSEConnector(BaseConnector):
    """Connector for CAMELS-SE (Sweden) — authoritative standalone."""

    slug = "camels_se"
    display_name = "CAMELS-SE (Sweden)"
    base_url = "https://researchdata.se"  # data via ensure_dataset
    country_codes = ["SE"]

    async def fetch_stations(self) -> list[Station]:
        """Catalogue from the WGS84 gauge point shapefile."""
        data_dir = await ensure_dataset(_GIS_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_se_no_gis", hint=f"Download from {_LANDING}")
            return []
        shp = self._find_one(Path(data_dir), _STATIONS_SHP)
        if shp is None:
            logger.info("camels_se_shapefile_not_found", data_dir=str(data_dir))
            return []

        import fiona

        stations: list[Station] = []
        with fiona.open(shp) as src:
            for feat in src:
                props = feat["properties"]
                gid = str(props.get("id") or "").strip()
                geom = feat["geometry"]
                if not gid or geom is None or geom["type"] != "Point":
                    continue
                lon, lat = geom["coordinates"][0], geom["coordinates"][1]
                stations.append(Station(
                    id=self._station_id(gid),
                    provider=self.slug,
                    native_id=gid,
                    name=str(props.get("name") or gid),
                    latitude=float(lat),
                    longitude=float(lon),
                    country_code="SE",
                ))
        logger.info("camels_se_stations_loaded", count=len(stations))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Read daily observed discharge (Qobs_m3s) for one catchment."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = await ensure_dataset(_STREAMFLOW_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_se_no_data", station=native_id, hint=f"Download from {_LANDING}")
            return self._empty_chunk(station_id)
        # Catchment names hold Swedish characters; the id is ASCII so glob on it.
        f = self._find_one(Path(data_dir), f"catchment_id_{native_id}_*.csv")
        if f is None:
            logger.info("camels_se_file_not_found", station=native_id, data_dir=str(data_dir))
            return self._empty_chunk(station_id)

        start_aware = start if start.tzinfo else start.replace(tzinfo=UTC)
        end_aware = end if end.tzinfo else end.replace(tzinfo=UTC)
        observations = self._parse_timeseries(f, station_id, start_aware, end_aware)
        logger.info(
            "camels_se_observations_loaded",
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
    def _find_one(data_dir: Path, pattern: str) -> Path | None:
        hits = sorted(data_dir.rglob(pattern))
        return hits[0] if hits else None

    @staticmethod
    def _parse_timeseries(
        path: Path, station_id: str, start: datetime, end: datetime,
    ) -> list[Observation]:
        observations: list[Observation] = []
        with open(path, newline="", encoding="latin-1") as fh:
            reader = csv.DictReader(fh)
            cols = reader.fieldnames or []
            if not {"Year", "Month", "Day", "Qobs_m3s"} <= set(cols):
                return observations
            for row in reader:
                try:
                    ts = datetime(
                        int(row["Year"]), int(row["Month"]), int(row["Day"]), tzinfo=UTC,
                    )
                except (KeyError, TypeError, ValueError):
                    continue
                if not (start <= ts <= end):
                    continue
                raw = (row.get("Qobs_m3s") or "").strip()
                discharge: float | None
                quality: QualityFlag
                if not raw or raw.lower() in ("nan", "na", "-9999"):
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
