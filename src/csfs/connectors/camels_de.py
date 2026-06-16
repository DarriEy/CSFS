"""CAMELS-DE connector — German large-sample hydrology (Zenodo, daily).

CAMELS-DE (Loritz et al. 2024) provides daily hydrometeorological time series and
attributes for 1582 German catchments, keyed by the CAMELS-DE gauge id (e.g.
``DE210480``).

A published, DOI-pinned dataset artifact (Zenodo record 16755906, v1.1.0,
CC-BY-4.0), distributed as a single ``camels_de.zip`` (~2.2 GB) that is
auto-downloaded and checksum-verified on first use via
:func:`csfs.core.downloads.ensure_dataset`:

* observations — ``timeseries/CAMELS_DE_hydromet_timeseries_{gauge}.csv``
  (comma-separated; ``date`` ISO, ``discharge_vol_obs`` in m³/s);
* catalogue — ``CAMELS_DE_topographic_attributes.csv``
  (``gauge_id,...,gauge_lat,gauge_lon,...``).
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

_ZENODO_URL = "https://zenodo.org/records/16755906"
_SLUG = "camels_de"  # single archive holds both timeseries and attributes


@register("camels_de")
class CAMELSDEConnector(BaseConnector):
    """Connector for CAMELS-DE (Germany)."""

    slug = "camels_de"
    display_name = "CAMELS-DE (Germany)"
    base_url = "https://zenodo.org/api"  # data comes from the archive via ensure_dataset
    country_codes = ["DE"]

    async def fetch_stations(self) -> list[Station]:
        """Catalogue from ``CAMELS_DE_topographic_attributes.csv`` (real coords)."""
        data_dir = await ensure_dataset(_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_de_no_data", hint=f"Download from {_ZENODO_URL}")
            return []
        attr = self._find_file(Path(data_dir), "CAMELS_DE_topographic_attributes.csv")
        if attr is None:
            return []

        stations: list[Station] = []
        with open(attr, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                gid = row.get("gauge_id")
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
                    name=row.get("gauge_name") or gid,
                    latitude=lat,
                    longitude=lon,
                    country_code="DE",
                ))
        logger.info("camels_de_stations_loaded", count=len(stations))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Read daily discharge for one gauge from its hydromet timeseries CSV."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = await ensure_dataset(_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_de_no_data", station=native_id, hint=f"Download from {_ZENODO_URL}")
            return self._empty_chunk(station_id)

        file_path = self._find_file(
            Path(data_dir), f"CAMELS_DE_hydromet_timeseries_{native_id}.csv"
        )
        if file_path is None:
            logger.info("camels_de_file_not_found", station=native_id, data_dir=str(data_dir))
            return self._empty_chunk(station_id)

        start_aware = start if start.tzinfo else start.replace(tzinfo=UTC)
        end_aware = end if end.tzinfo else end.replace(tzinfo=UTC)
        observations = self._parse_timeseries(file_path, station_id, start_aware, end_aware)
        logger.info(
            "camels_de_observations_loaded",
            station=native_id, count=len(observations), file=str(file_path),
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
    def _parse_timeseries(
        path: Path, station_id: str, start: datetime, end: datetime,
    ) -> list[Observation]:
        """Parse ``date,discharge_vol_obs,...``; blank discharge = missing."""
        observations: list[Observation] = []
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames is None or "discharge_vol_obs" not in reader.fieldnames:
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
                raw_q = (row.get("discharge_vol_obs") or "").strip()
                discharge: float | None
                quality: QualityFlag
                try:
                    discharge = float(raw_q)
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
