"""CAMELS-NZ connector — New Zealand large-sample hydrology (figshare, daily).

CAMELS-NZ (Bushra et al. 2025) provides daily observed streamflow for 369 New
Zealand catchments, keyed by the hydrological station id (e.g. ``29605``).

A published, DOI-pinned dataset artifact (University of Canterbury Data
Repository, figshare 28827644, CC-BY-4.0). Two resources are auto-downloaded +
checksum-verified via :func:`csfs.core.downloads.ensure_dataset`:

* ``camels_nz`` — ``CAMELS_NZ_daily_Streamflow.zip`` → per-station
  ``daily_flow_station_id_{id}.csv`` (``time,flow``; ``flow`` in m³/s, ``NA`` =
  missing). 14 of the 369 stations are permission-gated by the data owner and
  ship ALL-``NA`` files, which surface here as missing observations;
* ``camels_nz_attributes`` — ``CAMELS_NZ_Catchment_Atrributes.zip`` (the upstream
  filename misspelling is preserved) → ``1.CAMELS_NZ_Catchment_information.csv``
  with ``Station_ID`` and already-WGS84 ``Latitude (WGS 84)`` / ``Longitude(WGS
  84)`` columns (no reprojection).

The figshare access URLs (``ndownloader.figshare.com/files/<id>``) carry no
filename and 30x-redirect to short-lived signed S3 links; the download layer
content-sniffs the archive and httpx follows the redirect.
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

_LANDING = "https://doi.org/10.26021/canterburynz.28827644"
_STREAMFLOW_SLUG = "camels_nz"
_ATTRIBUTES_SLUG = "camels_nz_attributes"
_INFO_CSV = "CAMELS_NZ_Catchment_information.csv"
_LAT_COL = "Latitude (WGS 84)"
_LON_COL = "Longitude(WGS 84)"


@register("camels_nz")
class CAMELSNZConnector(BaseConnector):
    """Connector for CAMELS-NZ (New Zealand) — authoritative standalone."""

    slug = "camels_nz"
    display_name = "CAMELS-NZ (New Zealand)"
    base_url = "https://figshare.canterbury.ac.nz"  # data via ensure_dataset
    country_codes = ["NZ"]

    async def fetch_stations(self) -> list[Station]:
        """Catalogue from the catchment information CSV (already WGS84)."""
        data_dir = await ensure_dataset(_ATTRIBUTES_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_nz_no_attributes", hint=f"Download from {_LANDING}")
            return []
        # The file ships with a numeric ordering prefix ("1.<name>").
        info = self._find_one(Path(data_dir), f"*{_INFO_CSV}")
        if info is None:
            logger.info("camels_nz_info_not_found", data_dir=str(data_dir))
            return []
        stations: list[Station] = []
        # The file carries a UTF-8 BOM; utf-8-sig strips it so the first header
        # name ('Station_ID') is clean.
        with open(info, newline="", encoding="utf-8-sig") as fh:
            for row in csv.DictReader(fh):
                gid = (row.get("Station_ID") or "").strip()
                try:
                    lat = float(row[_LAT_COL])
                    lon = float(row[_LON_COL])
                except (KeyError, TypeError, ValueError):
                    continue
                if not gid:
                    continue
                stations.append(Station(
                    id=self._station_id(gid),
                    provider=self.slug,
                    native_id=gid,
                    name=(row.get("Station Name") or gid).strip(),
                    latitude=lat,
                    longitude=lon,
                    country_code="NZ",
                ))
        logger.info("camels_nz_stations_loaded", count=len(stations))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Read daily observed streamflow (flow, m³/s) for one station."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = await ensure_dataset(_STREAMFLOW_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_nz_no_data", station=native_id, hint=f"Download from {_LANDING}")
            return self._empty_chunk(station_id)
        f = self._find_one(Path(data_dir), f"daily_flow_station_id_{native_id}.csv")
        if f is None:
            logger.info("camels_nz_file_not_found", station=native_id, data_dir=str(data_dir))
            return self._empty_chunk(station_id)

        start_aware = start if start.tzinfo else start.replace(tzinfo=UTC)
        end_aware = end if end.tzinfo else end.replace(tzinfo=UTC)
        observations = self._parse_timeseries(f, station_id, start_aware, end_aware)
        logger.info(
            "camels_nz_observations_loaded",
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
            cols = reader.fieldnames or []
            if not {"time", "flow"} <= set(cols):
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
                raw = (row.get("flow") or "").strip()
                discharge: float | None
                quality: QualityFlag
                if not raw or raw.upper() in ("NA", "NAN"):
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
