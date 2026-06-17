"""CAMELS-SPAT connector — North American spatial hydrology (manual / Globus-only).

CAMELS-SPAT (Knoben et al. 2025, HESS 29:5791) provides daily (and hourly)
observed streamflow for 1,426 basins (~713 US USGS + ~713 Canadian WSC), keyed
by the agency gauge id. The "spatially distributed" CAMELS for North America.

DISTRIBUTION-GATED. The FRDR dataset (DOI 10.20383/103.01306) is published via
**Globus Transfer only** — there is no direct HTTPS download endpoint (the
"Download as Zip" option is intermittently unavailable), and Globus needs an
account + client software. It therefore cannot be auto-downloaded or checksum-
verified here, so it is NOT part of the provenance-gated dataset-artifact tier.
It is wired in as a MANUAL connector: Globus-transfer the data locally, point
``config['data_dir']`` at it, and this connector reads it.

.. warning::
   The per-basin NetCDF filename pattern, the ``streamflow`` variable name, and
   the metadata-table columns below follow the documented layout but are
   **UNVERIFIED against the real archive** (Globus-only at build time). Confirm
   and adjust once a local copy is available.
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

_LANDING = "https://www.frdr-dfdr.ca/repo/dataset/9ca63670-9e40-477c-a8a8-30f61205d668"
_SLUG = "camels_spat"
# Documented-but-unverified conventions (see module warning).
_STREAMFLOW_VARS = ("streamflow", "q_obs", "discharge", "obs_streamflow")
_META_GLOB = "*metadata*.csv"
_LAT_COLS = ("lat", "latitude", "gauge_lat", "Hydrometric_station_latitude")
_LON_COLS = ("lon", "longitude", "gauge_lon", "Hydrometric_station_longitude")
_ID_COLS = ("gauge_id", "station_id", "Official_ID", "id")
_COUNTRY_COLS = ("country", "country_code", "Country")
_SOURCE_COLS = ("source", "Source", "agency", "provider")


@register("camels_spat")
class CAMELSSPATConnector(BaseConnector):
    """Connector for CAMELS-SPAT (US + Canada) — manual / Globus-only standalone."""

    slug = "camels_spat"
    display_name = "CAMELS-SPAT (North America)"
    base_url = "https://www.frdr-dfdr.ca"  # Globus-only; data via local data_dir
    country_codes = ["US", "CA"]

    async def fetch_stations(self) -> list[Station]:
        """Catalogue from the metadata table (gauge id + WGS84 coordinates)."""
        data_dir = await ensure_dataset(_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_spat_no_data", hint=f"Globus-transfer from {_LANDING}")
            return []
        meta = self._find_one(Path(data_dir), _META_GLOB)
        if meta is None:
            logger.info("camels_spat_metadata_not_found", data_dir=str(data_dir))
            return []
        stations: list[Station] = []
        with open(meta, newline="", encoding="utf-8") as fh:
            reader = csv.DictReader(fh)
            cols = reader.fieldnames or []
            id_col = next((c for c in _ID_COLS if c in cols), None)
            lat_col = next((c for c in _LAT_COLS if c in cols), None)
            lon_col = next((c for c in _LON_COLS if c in cols), None)
            country_col = next((c for c in _COUNTRY_COLS if c in cols), None)
            source_col = next((c for c in _SOURCE_COLS if c in cols), None)
            if not (id_col and lat_col and lon_col):
                return stations
            for row in reader:
                gid = (row.get(id_col) or "").strip()
                try:
                    lat = float(row[lat_col])
                    lon = float(row[lon_col])
                except (KeyError, TypeError, ValueError):
                    continue
                if not gid:
                    continue
                stations.append(Station(
                    id=self._station_id(gid),
                    provider=self.slug,
                    native_id=gid,
                    name=gid,
                    latitude=lat,
                    longitude=lon,
                    country_code=_country(
                        row.get(country_col), row.get(source_col),
                    ),
                ))
        logger.info("camels_spat_stations_loaded", count=len(stations))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Read daily observed streamflow (m³/s) for one basin from its NetCDF."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = await ensure_dataset(_SLUG, self.config)
        if data_dir is None:
            logger.info("camels_spat_no_data", station=native_id,
                        hint=f"Globus-transfer from {_LANDING}")
            return self._empty_chunk(station_id)
        nc = self._find_one(Path(data_dir), f"*{native_id}*.nc")
        if nc is None:
            logger.info("camels_spat_file_not_found", station=native_id, data_dir=str(data_dir))
            return self._empty_chunk(station_id)

        start_aware = start if start.tzinfo else start.replace(tzinfo=UTC)
        end_aware = end if end.tzinfo else end.replace(tzinfo=UTC)
        observations = self._read_streamflow(nc, station_id, start_aware, end_aware)
        logger.info(
            "camels_spat_observations_loaded",
            station=native_id, count=len(observations), file=str(nc),
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

    # ------------------------------------------------------------------

    @staticmethod
    def _read_streamflow(
        nc: Path, station_id: str, start: datetime, end: datetime,
    ) -> list[Observation]:
        import numpy as np
        import xarray as xr

        observations: list[Observation] = []
        with xr.open_dataset(nc) as d:
            var = next((v for v in _STREAMFLOW_VARS if v in d.variables), None)
            if var is None or "time" not in d.coords:
                return observations
            times = d["time"].values
            lo = np.datetime64(start.replace(tzinfo=None))
            hi = np.datetime64(end.replace(tzinfo=None))
            mask = (times >= lo) & (times <= hi)
            if not mask.any():
                return observations
            series = np.asarray(d[var].values).reshape(len(times))[mask]
            sel_times = times[mask]
        for t, raw in zip(sel_times, series, strict=True):
            ts = datetime.fromtimestamp(t.astype("datetime64[s]").astype(int), tz=UTC)
            discharge: float | None
            quality: QualityFlag
            if raw is None or np.isnan(raw):
                discharge, quality = None, QualityFlag.MISSING
            else:
                discharge = float(raw)
                quality = QualityFlag.RAW
                if discharge < 0:
                    discharge, quality = None, QualityFlag.MISSING
            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=discharge,
                quality=quality,
            ))
        return observations


def _country(country: str | None, source: str | None) -> str:
    """Best-effort country: explicit country/source column, else default US.

    USGS and Water Survey of Canada gauge ids are not reliably distinguishable
    by prefix, so an explicit column is used when present.
    """
    c = (country or "").strip().upper()
    if c in ("US", "USA", "UNITED STATES"):
        return "US"
    if c in ("CA", "CAN", "CANADA"):
        return "CA"
    s = (source or "").strip().upper()
    if "WSC" in s or "HYDAT" in s or "CANADA" in s:
        return "CA"
    if "USGS" in s or "NWIS" in s:
        return "US"
    return "US"
