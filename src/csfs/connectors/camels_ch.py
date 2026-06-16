"""CAMELS-CH connector — Swiss large-sample hydrology (Zenodo, daily).

CAMELS-CH (Höge et al.) provides daily observation-based hydrometeorological
time series and attributes for Swiss catchments, keyed by the BAFU gauge id
(e.g. ``2004``).

A published, DOI-pinned dataset artifact (Zenodo 15025258, CC-BY-4.0),
distributed as a single ``camels_ch.zip`` auto-downloaded + checksum-verified
via :func:`csfs.core.downloads.ensure_dataset`:

* observations — ``timeseries/observation_based/CAMELS_CH_obs_based_{gauge}.csv``
  (comma-sep; ``date`` ISO, ``discharge_vol(m3/s)``; missing = ``NaN``);
* catalogue — ``static_attributes/CAMELS_CH_topographic_attributes.csv``
  (a leading ``#`` comment line, then ``gauge_id,...,gauge_lon,gauge_lat,...``).
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

_ZENODO_URL = "https://zenodo.org/records/15025258"
_SLUG = "camels_ch"
_DISCHARGE_COL = "discharge_vol(m3/s)"


def _rows_skipping_comments(fh):
    """Yield CSV rows, skipping leading ``#`` comment lines (CAMELS-CH attrs)."""
    return csv.DictReader(line for line in fh if not line.lstrip().startswith("#"))


@register("camels_ch")
class CAMELSCHConnector(BaseConnector):
    """Connector for CAMELS-CH (Switzerland)."""

    slug = "camels_ch"
    display_name = "CAMELS-CH (Switzerland)"
    base_url = "https://zenodo.org/api"  # data via ensure_dataset
    country_codes = ["CH"]

    async def fetch_stations(self) -> list[Station]:
        """Catalogue from the topographic attributes CSV (real BAFU coords)."""
        data_dir = await ensure_dataset(_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_ch_no_data", hint=f"Download from {_ZENODO_URL}")
            return []
        topo = self._find_file(Path(data_dir), "CAMELS_CH_topographic_attributes.csv")
        if topo is None:
            return []
        stations: list[Station] = []
        with open(topo, newline="", encoding="utf-8") as fh:
            for row in _rows_skipping_comments(fh):
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
                    country_code="CH",
                ))
        logger.info("camels_ch_stations_loaded", count=len(stations))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Read daily observed discharge for one gauge from its obs-based CSV."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = await ensure_dataset(_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_ch_no_data", station=native_id, hint=f"Download from {_ZENODO_URL}")
            return self._empty_chunk(station_id)
        file_path = self._find_file(Path(data_dir), f"CAMELS_CH_obs_based_{native_id}.csv")
        if file_path is None:
            logger.info("camels_ch_file_not_found", station=native_id, data_dir=str(data_dir))
            return self._empty_chunk(station_id)

        start_aware = start if start.tzinfo else start.replace(tzinfo=UTC)
        end_aware = end if end.tzinfo else end.replace(tzinfo=UTC)
        observations = self._parse_timeseries(file_path, station_id, start_aware, end_aware)
        logger.info(
            "camels_ch_observations_loaded",
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
        observations: list[Observation] = []
        with open(path, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            if reader.fieldnames is None or _DISCHARGE_COL not in reader.fieldnames:
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
                raw = (row.get(_DISCHARGE_COL) or "").strip()
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
