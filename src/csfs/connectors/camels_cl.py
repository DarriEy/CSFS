"""CAMELS-CL connector — Chilean large-sample hydrology (PANGAEA, daily).

CAMELS-CL (Alvarez-Garreton et al. 2018) provides daily streamflow for 516
Chilean catchments, keyed by the DGA gauge code (e.g. ``1001001``).

A published, DOI-pinned dataset artifact (PANGAEA 10.1594/PANGAEA.894885,
CC-BY). PANGAEA serves it as direct ``store.pangaea.de`` zips (no auth, no bot
protection); two are auto-downloaded and checksum-verified on first use via
:func:`csfs.core.downloads.ensure_dataset`:

* ``camels_cl`` — ``2_CAMELScl_streamflow_m3s.zip`` → a single WIDE MATRIX
  ``2_CAMELScl_streamflow_m3s.txt`` (tab-separated, every field double-quoted):
  row 0 is ``gauge_id`` + the 516 gauge IDs; column 0 of each later row is the
  ISO date; missing values are a quoted single space ``" "``.
* ``camels_cl_attributes`` — ``1_CAMELScl_attributes.zip`` → a TRANSPOSED table
  ``1_CAMELScl_attributes.txt`` (attribute per row, gauge per column) with
  ``gauge_id`` / ``gauge_name`` / ``gauge_lat`` / ``gauge_lon`` rows, for the
  station catalogue.
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

_PANGAEA_URL = "https://doi.pangaea.de/10.1594/PANGAEA.894885"
_STREAMFLOW_SLUG = "camels_cl"
_ATTRIBUTES_SLUG = "camels_cl_attributes"


def _unq(field: str) -> str:
    """Strip CAMELS-CL's surrounding double-quotes and whitespace."""
    return field.strip().strip('"').strip()


@register("camels_cl")
class CAMELSCLConnector(BaseConnector):
    """Connector for CAMELS-CL (Chile)."""

    slug = "camels_cl"
    display_name = "CAMELS-CL (Chile)"
    base_url = "https://store.pangaea.de"  # data via ensure_dataset
    country_codes = ["CL"]

    async def fetch_stations(self) -> list[Station]:
        """Catalogue from the transposed attributes table (real DGA coords)."""
        data_dir = await ensure_dataset(_ATTRIBUTES_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_cl_no_attributes", hint=f"Download from {_PANGAEA_URL}")
            return []
        attr = self._find_file(Path(data_dir), "1_CAMELScl_attributes.txt")
        if attr is None:
            return []

        rows: dict[str, list[str]] = {}
        with open(attr, encoding="utf-8") as fh:
            for line in fh:
                fields = line.rstrip("\n").split("\t")
                if not fields:
                    continue
                rows[_unq(fields[0])] = [_unq(f) for f in fields[1:]]
        ids = rows.get("gauge_id", [])
        lats = rows.get("gauge_lat", [])
        lons = rows.get("gauge_lon", [])
        names = rows.get("gauge_name", [])

        stations: list[Station] = []
        for i, gid in enumerate(ids):
            if not gid:
                continue
            try:
                lat = float(lats[i])
                lon = float(lons[i])
            except (IndexError, ValueError):
                continue
            stations.append(Station(
                id=self._station_id(gid),
                provider=self.slug,
                native_id=gid,
                name=(names[i] if i < len(names) and names[i] else gid),
                latitude=lat,
                longitude=lon,
                country_code="CL",
            ))
        logger.info("camels_cl_stations_loaded", count=len(stations))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Read one gauge's daily discharge column out of the wide matrix."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = await ensure_dataset(_STREAMFLOW_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_cl_no_data", station=native_id, hint=f"Download from {_PANGAEA_URL}")
            return self._empty_chunk(station_id)
        matrix = self._find_file(Path(data_dir), "2_CAMELScl_streamflow_m3s.txt")
        if matrix is None:
            logger.info("camels_cl_file_not_found", station=native_id, data_dir=str(data_dir))
            return self._empty_chunk(station_id)

        start_aware = start if start.tzinfo else start.replace(tzinfo=UTC)
        end_aware = end if end.tzinfo else end.replace(tzinfo=UTC)
        observations = self._parse_matrix(matrix, native_id, station_id, start_aware, end_aware)
        logger.info(
            "camels_cl_observations_loaded",
            station=native_id, count=len(observations), file=str(matrix),
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
    def _parse_matrix(
        path: Path, native_id: str, station_id: str, start: datetime, end: datetime,
    ) -> list[Observation]:
        observations: list[Observation] = []
        with open(path, encoding="utf-8") as fh:
            header = [_unq(f) for f in fh.readline().split("\t")]
            try:
                col = header.index(native_id)  # gauge_id column for this station
            except ValueError:
                return observations  # gauge not in this dataset
            for line in fh:
                fields = line.split("\t")
                if col >= len(fields):
                    continue
                raw_date = _unq(fields[0])
                if not raw_date:
                    continue
                try:
                    ts = datetime.strptime(raw_date[:10], "%Y-%m-%d").replace(tzinfo=UTC)
                except ValueError:
                    continue
                if not (start <= ts <= end):
                    continue
                raw_q = _unq(fields[col])
                discharge: float | None
                quality: QualityFlag
                try:
                    discharge = float(raw_q)
                    quality = QualityFlag.RAW
                    if discharge < 0:
                        discharge, quality = None, QualityFlag.MISSING
                except ValueError:
                    # CAMELS-CL missing-data sentinel is a quoted single space.
                    discharge, quality = None, QualityFlag.MISSING
                observations.append(Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=quality,
                ))
        return observations
