"""CAMELS-BR connector — Brazilian large-sample hydrology (Zenodo, daily).

CAMELS-BR (Chagas et al. 2020, ESSD) provides daily streamflow and landscape
attributes for 897 Brazilian catchments, keyed by the ANA gauge code.

A published, DOI-pinned dataset artifact (Zenodo record 3964745, CC-BY-4.0),
distributed as separate zips. Two are auto-downloaded and checksum-verified on
first use via :func:`csfs.core.downloads.ensure_dataset`:

* ``camels_br`` — ``02_CAMELS_BR_streamflow_m3s.zip`` → per-gauge daily
  discharge ``02_CAMELS_BR_streamflow_m3s/{gauge}_streamflow_m3s.txt``
  (whitespace-separated ``year month day streamflow_m3s qual_control_by_ana
  qual_flag``); this is the observation source.
* ``camels_br_attributes`` — ``01_CAMELS_BR_attributes.zip`` →
  ``camels_br_location.txt`` (``gauge_id gauge_name gauge_region gauge_lat
  gauge_lon ...``) for the station catalogue.
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

_ZENODO_URL = "https://zenodo.org/records/3964745"
_STREAMFLOW_SLUG = "camels_br"               # primary archive: streamflow_m3s
_ATTRIBUTES_SLUG = "camels_br_attributes"    # secondary archive: location/coords


@register("camels_br")
class CAMELSBRConnector(BaseConnector):
    """Connector for CAMELS-BR (Brazil)."""

    slug = "camels_br"
    display_name = "CAMELS-BR (Brazil)"
    base_url = "https://zenodo.org/api"  # data comes from the archive via ensure_dataset
    country_codes = ["BR"]

    async def fetch_stations(self) -> list[Station]:
        """Catalogue from ``camels_br_location.txt`` (real ANA gauge coords)."""
        data_dir = await ensure_dataset(_ATTRIBUTES_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_br_no_attributes", hint=f"Download from {_ZENODO_URL}")
            return []
        loc = self._find_file(Path(data_dir), "camels_br_location.txt")
        if loc is None:
            return []

        stations: list[Station] = []
        with open(loc, encoding="utf-8") as fh:
            header = fh.readline().split()
            idx = {name: i for i, name in enumerate(header)}
            for line in fh:
                cols = line.split()
                if len(cols) <= idx.get("gauge_lon", -1):
                    continue
                try:
                    gid = cols[idx["gauge_id"]]
                    lat = float(cols[idx["gauge_lat"]])
                    lon = float(cols[idx["gauge_lon"]])
                except (KeyError, ValueError):
                    continue
                name = cols[idx["gauge_name"]] if "gauge_name" in idx else gid
                stations.append(Station(
                    id=self._station_id(gid),
                    provider=self.slug,
                    native_id=gid,
                    name=name,
                    latitude=lat,
                    longitude=lon,
                    country_code="BR",
                ))
        logger.info("camels_br_stations_loaded", count=len(stations))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Read daily discharge for one ANA gauge from its streamflow_m3s file."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = await ensure_dataset(_STREAMFLOW_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_br_no_data", station=native_id, hint=f"Download from {_ZENODO_URL}")
            return self._empty_chunk(station_id)

        file_path = self._find_file(
            Path(data_dir), f"{native_id}_streamflow_m3s.txt"
        )
        if file_path is None:
            logger.info("camels_br_file_not_found", station=native_id, data_dir=str(data_dir))
            return self._empty_chunk(station_id)

        start_aware = start if start.tzinfo else start.replace(tzinfo=UTC)
        end_aware = end if end.tzinfo else end.replace(tzinfo=UTC)
        observations = self._parse_streamflow(file_path, station_id, start_aware, end_aware)
        logger.info(
            "camels_br_observations_loaded",
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
        """Return an empty TimeSeriesChunk for a station."""
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )

    @staticmethod
    def _find_file(data_dir: Path, name: str) -> Path | None:
        direct = list(data_dir.rglob(name))
        return direct[0] if direct else None

    @staticmethod
    def _parse_streamflow(
        path: Path, station_id: str, start: datetime, end: datetime,
    ) -> list[Observation]:
        """Parse ``year month day streamflow_m3s ...`` (whitespace-separated).

        Negative discharge encodes missing data in CAMELS-BR and is dropped.
        """
        observations: list[Observation] = []
        with open(path, encoding="utf-8") as fh:
            header = fh.readline().split()
            try:
                iy, im, idd = header.index("year"), header.index("month"), header.index("day")
                iq = header.index("streamflow_m3s")
            except ValueError:
                return observations
            for line in fh:
                cols = line.split()
                if len(cols) <= iq:
                    continue
                try:
                    ts = datetime(int(cols[iy]), int(cols[im]), int(cols[idd]), tzinfo=UTC)
                    q = float(cols[iq])
                except (ValueError, IndexError):
                    continue
                if q < 0:  # CAMELS-BR missing-data sentinel
                    continue
                if start <= ts <= end:
                    observations.append(Observation(
                        station_id=station_id,
                        timestamp=ts,
                        discharge_m3s=q,
                        quality=QualityFlag.RAW,
                    ))
        return observations
