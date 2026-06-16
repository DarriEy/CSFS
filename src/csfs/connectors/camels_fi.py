"""CAMELS-FI connector — Finnish large-sample hydrology (Zenodo, daily).

CAMELS-FI (Seppä et al. 2025) provides daily observed discharge for 320 Finnish
catchments, keyed by the SYKE gauge id (e.g. ``896``; combined/virtual gauges
use a hyphenated id such as ``1181-3743``).

A published, DOI-pinned dataset artifact (Zenodo, CC-BY-4.0), still an ESSD
preprint under review. A single bundle is auto-downloaded + checksum-verified
via :func:`csfs.core.downloads.ensure_dataset` (slug ``camels_fi``):

* per-gauge ``CAMELS-FI/data/timeseries/CAMELS_FI_hydromet_timeseries_{gauge}_{range}.csv``
  (CAMELS-GB layout; ``date,discharge_vol,discharge_spec,...``; observed
  discharge ``discharge_vol`` in m³/s; blank = missing);
* gauge coordinates from ``CAMELS_FI_meta_attributes.csv``. The published
  ``gauge_lat`` / ``gauge_lon`` columns have a documented metadata-description
  swap, so the unambiguous projected ``gauge_easting`` / ``gauge_northing``
  (ETRS-TM35FIN, EPSG:3067) are used and reprojected to WGS84.
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

_LANDING = "https://doi.org/10.5281/zenodo.15853357"
_SLUG = "camels_fi"
_META_CSV = "CAMELS_FI_meta_attributes.csv"
_EPSG_FI = 3067  # ETRS89 / ETRS-TM35FIN


@register("camels_fi")
class CAMELSFIConnector(BaseConnector):
    """Connector for CAMELS-FI (Finland) — authoritative standalone."""

    slug = "camels_fi"
    display_name = "CAMELS-FI (Finland)"
    base_url = "https://zenodo.org"  # data via ensure_dataset
    country_codes = ["FI"]

    async def fetch_stations(self) -> list[Station]:
        """Catalogue from the meta attributes CSV (TM35FIN → WGS84)."""
        data_dir = await ensure_dataset(_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_fi_no_data", hint=f"Download from {_LANDING}")
            return []
        meta = self._find_one(Path(data_dir), _META_CSV)
        if meta is None:
            logger.info("camels_fi_meta_not_found", data_dir=str(data_dir))
            return []
        stations: list[Station] = []
        with open(meta, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                gid = (row.get("gauge_id") or "").strip()
                try:
                    easting = float(row["gauge_easting"])
                    northing = float(row["gauge_northing"])
                except (KeyError, TypeError, ValueError):
                    continue
                if not gid:
                    continue
                lat, lon = to_wgs84(easting, northing, _EPSG_FI)
                stations.append(Station(
                    id=self._station_id(gid),
                    provider=self.slug,
                    native_id=gid,
                    name=(row.get("gauge_name") or gid).strip(),
                    latitude=lat,
                    longitude=lon,
                    country_code="FI",
                ))
        logger.info("camels_fi_stations_loaded", count=len(stations))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Read daily observed discharge (discharge_vol, m³/s) for one gauge."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = await ensure_dataset(_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_fi_no_data", station=native_id, hint=f"Download from {_LANDING}")
            return self._empty_chunk(station_id)
        # The filename embeds the gauge id then the record date range.
        f = self._find_one(Path(data_dir), f"CAMELS_FI_hydromet_timeseries_{native_id}_*.csv")
        if f is None:
            logger.info("camels_fi_file_not_found", station=native_id, data_dir=str(data_dir))
            return self._empty_chunk(station_id)

        start_aware = start if start.tzinfo else start.replace(tzinfo=UTC)
        end_aware = end if end.tzinfo else end.replace(tzinfo=UTC)
        observations = self._parse_timeseries(f, station_id, start_aware, end_aware)
        logger.info(
            "camels_fi_observations_loaded",
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
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            cols = reader.fieldnames or []
            if not {"date", "discharge_vol"} <= set(cols):
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
