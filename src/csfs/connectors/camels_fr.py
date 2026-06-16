"""CAMELS-FR connector — French large-sample hydrology (Recherche Data Gouv, daily).

CAMELS-FR (Delaigue et al. 2024) provides daily observed streamflow for 654
French catchments, keyed by the Hydro3 station code (``sta_code_h3``, e.g.
``A105003001``).

A published, DOI-pinned dataset artifact (INRAE / Recherche Data Gouv,
doi:10.57745/WH7FJR, CC-BY-4.0). Two resources are auto-downloaded +
checksum-verified via :func:`csfs.core.downloads.ensure_dataset`:

* ``camels_fr`` — ``CAMELS_FR_time_series.zip`` → per-station
  ``CAMELS_FR_time_series/daily/CAMELS_FR_tsd_{code}.csv``. Each file opens with
  a ``#``-prefixed comment block, then a semicolon-separated header. The date is
  ``tsd_date`` (``YYYYMMDD``); **observed streamflow is ``tsd_q_l`` in litres per
  second** and is divided by 1000 to obtain m³/s;
* ``camels_fr_geography`` — ``CAMELS_FR_geography.zip`` → the gauge outlet
  GeoPackage ``CAMELS_FR_gauge_outlet.gpkg`` (Point geometry, ``sta_code_h3``
  attribute). Coordinates are in **NTF Lambert II (EPSG:27572)** and are
  reprojected to WGS84 on read.
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

_LANDING = "https://doi.org/10.57745/WH7FJR"
_STREAMFLOW_SLUG = "camels_fr"
_GEOGRAPHY_SLUG = "camels_fr_geography"
_OUTLET_GPKG = "CAMELS_FR_gauge_outlet.gpkg"
_CODE_FIELD = "sta_code_h3"
_EPSG_FR = 27572  # NTF (Paris) / Lambert zone II
_LPS_TO_M3S = 1.0 / 1000.0


@register("camels_fr")
class CAMELSFRConnector(BaseConnector):
    """Connector for CAMELS-FR (France) — authoritative standalone."""

    slug = "camels_fr"
    display_name = "CAMELS-FR (France)"
    base_url = "https://entrepot.recherche.data.gouv.fr"  # data via ensure_dataset
    country_codes = ["FR"]

    async def fetch_stations(self) -> list[Station]:
        """Catalogue from the gauge outlet GeoPackage (Lambert II → WGS84)."""
        data_dir = await ensure_dataset(_GEOGRAPHY_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_fr_no_geography", hint=f"Download from {_LANDING}")
            return []
        gpkg = self._find_one(Path(data_dir), _OUTLET_GPKG)
        if gpkg is None:
            logger.info("camels_fr_gpkg_not_found", data_dir=str(data_dir))
            return []

        import fiona

        stations: list[Station] = []
        with fiona.open(gpkg) as src:
            for feat in src:
                props = feat["properties"]
                code = str(props.get(_CODE_FIELD) or "").strip()
                geom = feat["geometry"]
                if not code or geom is None or geom["type"] != "Point":
                    continue
                easting, northing = geom["coordinates"][0], geom["coordinates"][1]
                lat, lon = to_wgs84(easting, northing, _EPSG_FR)
                stations.append(Station(
                    id=self._station_id(code),
                    provider=self.slug,
                    native_id=code,
                    name=code,
                    latitude=lat,
                    longitude=lon,
                    country_code="FR",
                ))
        logger.info("camels_fr_stations_loaded", count=len(stations))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Read daily observed streamflow (tsd_q_l, L/s → m³/s) for one station."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = await ensure_dataset(_STREAMFLOW_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_fr_no_data", station=native_id, hint=f"Download from {_LANDING}")
            return self._empty_chunk(station_id)
        f = self._find_one(Path(data_dir), f"CAMELS_FR_tsd_{native_id}.csv")
        if f is None:
            logger.info("camels_fr_file_not_found", station=native_id, data_dir=str(data_dir))
            return self._empty_chunk(station_id)

        start_aware = start if start.tzinfo else start.replace(tzinfo=UTC)
        end_aware = end if end.tzinfo else end.replace(tzinfo=UTC)
        observations = self._parse_timeseries(f, station_id, start_aware, end_aware)
        logger.info(
            "camels_fr_observations_loaded",
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
            # A '#'-prefixed comment block precedes the semicolon-separated header.
            data_lines = [ln for ln in fh if not ln.lstrip().startswith("#")]
        reader = csv.DictReader(data_lines, delimiter=";")
        cols = reader.fieldnames or []
        if not {"tsd_date", "tsd_q_l"} <= set(cols):
            return observations
        for row in reader:
            raw_date = (row.get("tsd_date") or "").strip()
            if not raw_date:
                continue
            try:
                ts = datetime.strptime(raw_date[:8], "%Y%m%d").replace(tzinfo=UTC)
            except ValueError:
                continue
            if not (start <= ts <= end):
                continue
            raw = (row.get("tsd_q_l") or "").strip()
            discharge: float | None
            quality: QualityFlag
            if not raw or raw.lower() in ("nan", "na"):
                discharge, quality = None, QualityFlag.MISSING
            else:
                try:
                    value = float(raw)
                    if value < 0:
                        discharge, quality = None, QualityFlag.MISSING
                    else:
                        discharge, quality = value * _LPS_TO_M3S, QualityFlag.RAW
                except ValueError:
                    discharge, quality = None, QualityFlag.MISSING
            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=discharge,
                quality=quality,
            ))
        return observations
