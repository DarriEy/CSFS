"""CAMELS-US connector — US large-sample hydrology (Zenodo/NCAR, daily).

CAMELS-US (Newman et al. 2015 / Addor et al. 2017) provides daily USGS
streamflow for 671 US catchments, keyed by the 8-digit USGS gauge id.

A published, DOI-pinned dataset artifact (Zenodo 15529996, mirror of NCAR
10.5065/D6MW2F4D, CC-BY-4.0), distributed as a single ~3.4 GB bundle
auto-downloaded + checksum-verified via
:func:`csfs.core.downloads.ensure_dataset`:

* observations — per-basin ``usgs_streamflow/<HUC>/<id>_streamflow_qc.txt``
  (whitespace ``gaugeID YYYY MM DD discharge_cfs qc``; cfs converted to m³/s;
  -999 = missing);
* catalogue — ``basin_metadata/gauge_information.txt`` (tab-separated
  ``HUC_02 GAGE_ID GAGE_NAME LAT LONG DRAINAGE_AREA``).
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.downloads import ensure_dataset
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

_ZENODO_URL = "https://zenodo.org/records/15529996"
_SLUG = "camels_us"
#: 1 cubic foot per second = 0.028316846592 m³/s.
_CFS_TO_M3S = 0.028316846592


@register("camels_us")
class CAMELSUSConnector(BaseConnector):
    """Connector for CAMELS-US (USA)."""

    slug = "camels_us"
    display_name = "CAMELS-US (USA)"
    base_url = "https://zenodo.org/api"  # data via ensure_dataset
    country_codes = ["US"]

    async def fetch_stations(self) -> list[Station]:
        """Catalogue from ``gauge_information.txt`` (USGS gauge coords)."""
        data_dir = await ensure_dataset(_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_us_no_data", hint=f"Download from {_ZENODO_URL}")
            return []
        info = self._find_one(Path(data_dir), "gauge_information.txt")
        if info is None:
            return []
        stations: list[Station] = []
        with open(info, encoding="utf-8", errors="replace") as fh:
            header = fh.readline().split("\t")
            hl = [h.strip().upper() for h in header]

            def idx(*names: str) -> int | None:
                for n in names:
                    if n in hl:
                        return hl.index(n)
                return None

            i_id, i_name = idx("GAGE_ID"), idx("GAGE_NAME")
            i_lat, i_lon = idx("LAT"), idx("LONG", "LON")
            if i_id is None or i_lat is None or i_lon is None:
                return stations
            for line in fh:
                cols = line.rstrip("\n").split("\t")
                if max(i_id, i_lat, i_lon) >= len(cols):
                    continue
                gid = cols[i_id].strip()
                try:
                    lat, lon = float(cols[i_lat]), float(cols[i_lon])
                except ValueError:
                    continue
                if not gid:
                    continue
                stations.append(Station(
                    id=self._station_id(gid),
                    provider=self.slug,
                    native_id=gid,
                    name=(cols[i_name].strip() if i_name is not None and i_name < len(cols) else gid),
                    latitude=lat,
                    longitude=lon,
                    country_code="US",
                ))
        logger.info("camels_us_stations_loaded", count=len(stations))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Read one gauge's daily discharge (cfs->m³/s) from its qc txt file."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = await ensure_dataset(_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_us_no_data", station=native_id, hint=f"Download from {_ZENODO_URL}")
            return self._empty_chunk(station_id)
        f = self._find_one(Path(data_dir), f"{native_id}_streamflow_qc.txt")
        if f is None:
            logger.info("camels_us_file_not_found", station=native_id, data_dir=str(data_dir))
            return self._empty_chunk(station_id)

        start_aware = start if start.tzinfo else start.replace(tzinfo=UTC)
        end_aware = end if end.tzinfo else end.replace(tzinfo=UTC)
        observations = self._parse_streamflow(f, station_id, start_aware, end_aware)
        logger.info(
            "camels_us_observations_loaded",
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
    def _parse_streamflow(
        path: Path, station_id: str, start: datetime, end: datetime,
    ) -> list[Observation]:
        """Whitespace ``gaugeID YYYY MM DD cfs qc``; -999 (or negative) = missing."""
        observations: list[Observation] = []
        with open(path, encoding="utf-8", errors="replace") as fh:
            for line in fh:
                cols = line.split()
                if len(cols) < 5:
                    continue
                try:
                    ts = datetime(int(cols[1]), int(cols[2]), int(cols[3]), tzinfo=UTC)
                    cfs = float(cols[4])
                except (ValueError, IndexError):
                    continue
                if not (start <= ts <= end):
                    continue
                if cfs < 0:  # CAMELS-US uses -999 for missing
                    discharge, quality = None, QualityFlag.MISSING
                else:
                    discharge, quality = cfs * _CFS_TO_M3S, QualityFlag.RAW
                observations.append(Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=quality,
                ))
        return observations
