"""CAMELS-LUX connector — Luxembourg large-sample hydrology (Zenodo, daily).

CAMELS-LUX (Nijzink et al. 2025) provides daily streamflow for 56 nested
Luxembourg catchments, keyed by a zero-padded id (``ID_01`` … ``ID_56``).

A published, DOI-pinned dataset artifact (Zenodo, CC-BY-4.0), still an ESSD
preprint under review. Two resources are auto-downloaded + checksum-verified via
:func:`csfs.core.downloads.ensure_dataset`:

* ``camels_lux`` — the ``CAMELS-LUX.zip`` bundle → per-gauge
  ``timeseries/daily/CAMELS_LUX_hydromet_timeseries__daily_ID_{NN}.csv`` (note
  the double underscore). Discharge ``Q`` is in m³/s with a companion ``Qflag``
  (0 = original observation, anything else = gap-filled/interpolated); ``NaN`` =
  missing. Filled values are surfaced with an ``estimated`` quality flag;
* ``camels_lux_shapefiles`` — ``CAMELS-LUX_shapefiles.zip`` → the gauge point
  shapefile ``stream-gauges_CAMELS-LUX.shp`` (already WGS84; ``gauge_id``
  attribute, Point geometry).
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

_LANDING = "https://doi.org/10.5281/zenodo.13846619"
_STREAMFLOW_SLUG = "camels_lux"
_SHAPEFILE_SLUG = "camels_lux_shapefiles"
_GAUGES_SHP = "stream-gauges_CAMELS-LUX.shp"


@register("camels_lux")
class CAMELSLUXConnector(BaseConnector):
    """Connector for CAMELS-LUX (Luxembourg) — authoritative standalone."""

    slug = "camels_lux"
    display_name = "CAMELS-LUX (Luxembourg)"
    base_url = "https://zenodo.org"  # data via ensure_dataset
    country_codes = ["LU"]

    async def fetch_stations(self) -> list[Station]:
        """Catalogue from the WGS84 gauge point shapefile."""
        data_dir = await ensure_dataset(_SHAPEFILE_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_lux_no_shapefiles", hint=f"Download from {_LANDING}")
            return []
        shp = self._find_one(Path(data_dir), _GAUGES_SHP)
        if shp is None:
            logger.info("camels_lux_shapefile_not_found", data_dir=str(data_dir))
            return []

        import fiona

        stations: list[Station] = []
        with fiona.open(shp) as src:
            for feat in src:
                props = feat["properties"]
                gid = str(props.get("gauge_id") or "").strip()
                geom = feat["geometry"]
                if not gid or geom is None or geom["type"] != "Point":
                    continue
                lon, lat = geom["coordinates"][0], geom["coordinates"][1]
                stations.append(Station(
                    id=self._station_id(gid),
                    provider=self.slug,
                    native_id=gid,
                    name=str(props.get("Station") or gid),
                    latitude=float(lat),
                    longitude=float(lon),
                    country_code="LU",
                ))
        logger.info("camels_lux_stations_loaded", count=len(stations))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Read daily discharge (Q, m³/s) for one gauge; flag gap-filled values."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = await ensure_dataset(_STREAMFLOW_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_lux_no_data", station=native_id, hint=f"Download from {_LANDING}")
            return self._empty_chunk(station_id)
        f = self._find_one(Path(data_dir), f"*daily_{native_id}.csv")
        if f is None:
            logger.info("camels_lux_file_not_found", station=native_id, data_dir=str(data_dir))
            return self._empty_chunk(station_id)

        start_aware = start if start.tzinfo else start.replace(tzinfo=UTC)
        end_aware = end if end.tzinfo else end.replace(tzinfo=UTC)
        observations = self._parse_timeseries(f, station_id, start_aware, end_aware)
        logger.info(
            "camels_lux_observations_loaded",
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
            if not {"Date", "Q"} <= set(cols):
                return observations
            has_flag = "Qflag" in cols
            for row in reader:
                raw_date = (row.get("Date") or "").strip()
                if not raw_date:
                    continue
                try:
                    ts = datetime.strptime(raw_date[:10], "%Y-%m-%d").replace(tzinfo=UTC)
                except ValueError:
                    continue
                if not (start <= ts <= end):
                    continue
                raw = (row.get("Q") or "").strip()
                discharge: float | None
                quality: QualityFlag
                if not raw or raw.lower() in ("nan", "na"):
                    discharge, quality = None, QualityFlag.MISSING
                else:
                    try:
                        discharge = float(raw)
                        if discharge < 0:
                            discharge, quality = None, QualityFlag.MISSING
                        else:
                            # Qflag 0 = original observation; non-zero = gap-filled.
                            flag = (row.get("Qflag") or "0").strip() if has_flag else "0"
                            quality = QualityFlag.RAW if flag in ("0", "0.0", "") else QualityFlag.ESTIMATED
                    except ValueError:
                        discharge, quality = None, QualityFlag.MISSING
                observations.append(Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=quality,
                ))
        return observations
