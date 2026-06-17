"""CAMELS-GB connector — Great Britain large-sample hydrology (CEH, daily).

CAMELS-GB (Coxon et al. 2020) provides daily hydrometeorological time series and
attributes for 671 British catchments, keyed by the NRFA gauge id (e.g.
``41004``).

A published dataset artifact (CEH EIDC, Open Government Licence), distributed as
a single zip auto-downloaded via :func:`csfs.core.downloads.ensure_dataset`.
CEH's data-package server regenerates the zip per request, so the ARCHIVE bytes
are non-reproducible; integrity is enforced by a CONTENT checksum over the
extracted data (the volatile ``readme.html`` generation-timestamp is excluded).

* observations — ``timeseries/CAMELS_GB_hydromet_timeseries_{gauge}_{range}.csv``
  (comma-sep; ``date`` ISO, ``discharge_vol`` in m³/s);
* catalogue — ``CAMELS_GB_topographic_attributes.csv``
  (row per gauge; ``gauge_id,gauge_name,gauge_lat,gauge_lon,...``).
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

_EIDC_URL = "https://catalogue.ceh.ac.uk/documents/8344e4f3-d2ea-44f5-8afa-86d2987543a9"
_SLUG = "camels_gb"


@register("camels_gb")
class CAMELSGBConnector(BaseConnector):
    """Connector for CAMELS-GB (Great Britain)."""

    slug = "camels_gb"
    display_name = "CAMELS-GB (Great Britain)"
    base_url = "https://data-package.ceh.ac.uk"  # data via ensure_dataset
    country_codes = ["GB"]

    async def fetch_stations(self) -> list[Station]:
        """Catalogue from ``CAMELS_GB_topographic_attributes.csv`` (NRFA coords)."""
        data_dir = await ensure_dataset(_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_gb_no_data", hint=f"Download from {_EIDC_URL}")
            return []
        topo = self._find_one(Path(data_dir), "CAMELS_GB_topographic_attributes.csv")
        if topo is None:
            return []
        stations: list[Station] = []
        with open(topo, newline="", encoding="utf-8") as fh:
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
                    name=row.get("gauge_name") or gid,
                    latitude=lat,
                    longitude=lon,
                    country_code="GB",
                ))
        logger.info("camels_gb_stations_loaded", count=len(stations))
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
            logger.info("camels_gb_no_data", station=native_id, hint=f"Download from {_EIDC_URL}")
            return self._empty_chunk(station_id)
        # Filenames carry a record-range suffix: ..._{gauge}_{start}-{end}.csv.
        hits = list(Path(data_dir).rglob(f"CAMELS_GB_hydromet_timeseries_{native_id}_*.csv"))
        if not hits:
            logger.info("camels_gb_file_not_found", station=native_id, data_dir=str(data_dir))
            return self._empty_chunk(station_id)

        start_aware = start if start.tzinfo else start.replace(tzinfo=UTC)
        end_aware = end if end.tzinfo else end.replace(tzinfo=UTC)
        observations = self._parse_timeseries(hits[0], station_id, start_aware, end_aware)
        logger.info(
            "camels_gb_observations_loaded",
            station=native_id, count=len(observations), file=str(hits[0]),
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
            if reader.fieldnames is None or "discharge_vol" not in reader.fieldnames:
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
                raw = (row.get("discharge_vol") or "").strip()
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
