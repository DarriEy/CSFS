"""HYSETS connector — North American large-sample hydrology (OSF, daily).

HYSETS (Arsenault et al. 2020, Sci. Data 7:243) is a multisource database for
14,425 North American watersheds (Canada / US / Mexico), keyed by the gauge's
agency ``Official_ID`` (HYDAT e.g. ``01AD002``; USGS e.g. ``01646500``).

A published, DOI-pinned dataset artifact (OSF ``rpc3w``, CC-BY-4.0). Unlike the
CAMELS connectors, observed streamflow is a variable INSIDE a large multi-
variable NetCDF, not per-gauge CSVs. Two resources are auto-downloaded +
checksum-verified via :func:`csfs.core.downloads.ensure_dataset`:

* ``hysets`` — ``HYSETS_2023_update_QC_stations.nc`` (~3 GB): a
  ``discharge(watershed, time)`` array in m³/s of quality-controlled observed
  flow (1950–2023 daily; NaN = missing). The ``watershedID`` variable maps each
  ``watershed`` index to a ``Watershed_ID``;
* ``hysets_properties`` — the bare ``HYSETS_watershed_properties.txt`` (comma-
  separated) mapping ``Official_ID`` → ``Watershed_ID`` and carrying the WGS84
  gauge coordinates (``Hydrometric_station_latitude/longitude``).

The OSF download URLs are filename-less (``osf.io/download/<key>/``); the
DATASETS entries pin the real filename so the bare files keep their names.
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

_LANDING = "https://osf.io/rpc3w/"
_STREAMFLOW_SLUG = "hysets"
_PROPERTIES_SLUG = "hysets_properties"
_PROPERTIES_TXT = "HYSETS_watershed_properties.txt"
_QC_NC = "HYSETS_2023_update_QC_stations.nc"


@register("hysets")
class HYSETSConnector(BaseConnector):
    """Connector for HYSETS (Canada/US/Mexico) — authoritative standalone."""

    slug = "hysets"
    display_name = "HYSETS (North America)"
    base_url = "https://osf.io"  # data via ensure_dataset
    country_codes = ["CA", "US", "MX"]

    async def fetch_stations(self) -> list[Station]:
        """Catalogue from the watershed properties table (already WGS84)."""
        props = await self._load_properties()
        if props is None:
            return []
        stations: list[Station] = []
        for oid, row in props.items():
            try:
                lat = float(row["Hydrometric_station_latitude"])
                lon = float(row["Hydrometric_station_longitude"])
            except (KeyError, TypeError, ValueError):
                continue
            stations.append(Station(
                id=self._station_id(oid),
                provider=self.slug,
                native_id=oid,
                name=(row.get("Name") or oid).strip().strip('"'),
                latitude=lat,
                longitude=lon,
                country_code=_country_of(row.get("Source", "")),
            ))
        logger.info("hysets_stations_loaded", count=len(stations))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Read daily observed discharge (m³/s) for one gauge from the NetCDF."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        props = await self._load_properties()
        if props is None or native_id not in props:
            logger.info("hysets_station_unknown", station=native_id)
            return self._empty_chunk(station_id)
        try:
            watershed_id = int(float(props[native_id]["Watershed_ID"]))
        except (KeyError, TypeError, ValueError):
            return self._empty_chunk(station_id)

        data_dir = await ensure_dataset(_STREAMFLOW_SLUG, self.config)
        if data_dir is None:
            logger.info("hysets_no_data", station=native_id, hint=f"Download from {_LANDING}")
            return self._empty_chunk(station_id)
        nc = self._find_one(Path(data_dir), _QC_NC) or self._find_one(Path(data_dir), "*.nc")
        if nc is None:
            logger.info("hysets_nc_not_found", data_dir=str(data_dir))
            return self._empty_chunk(station_id)

        start_aware = start if start.tzinfo else start.replace(tzinfo=UTC)
        end_aware = end if end.tzinfo else end.replace(tzinfo=UTC)
        observations = self._read_discharge(nc, watershed_id, station_id, start_aware, end_aware)
        logger.info(
            "hysets_observations_loaded",
            station=native_id, watershed_id=watershed_id, count=len(observations),
        )
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # ------------------------------------------------------------------

    async def _load_properties(self) -> dict[str, dict[str, str]] | None:
        """Official_ID → row dict from the watershed properties table."""
        data_dir = await ensure_dataset(_PROPERTIES_SLUG, self.config)
        if data_dir is None:
            logger.info("hysets_no_properties", hint=f"Download from {_LANDING}")
            return None
        txt = self._find_one(Path(data_dir), _PROPERTIES_TXT)
        if txt is None:
            logger.info("hysets_properties_not_found", data_dir=str(data_dir))
            return None
        out: dict[str, dict[str, str]] = {}
        with open(txt, newline="", encoding="utf-8") as fh:
            for row in csv.DictReader(fh):
                oid = (row.get("Official_ID") or "").strip()
                if oid:
                    out.setdefault(oid, row)
        return out

    @staticmethod
    def _read_discharge(
        nc: Path, watershed_id: int, station_id: str, start: datetime, end: datetime,
    ) -> list[Observation]:
        import numpy as np
        import xarray as xr

        observations: list[Observation] = []
        with xr.open_dataset(nc) as d:
            wids = d["watershedID"].values
            hits = np.where(wids == float(watershed_id))[0]
            if len(hits) == 0:
                return observations
            pos = int(hits[0])
            times = d["time"].values  # datetime64[ns]
            lo = np.datetime64(start.replace(tzinfo=None))
            hi = np.datetime64(end.replace(tzinfo=None))
            mask = (times >= lo) & (times <= hi)
            if not mask.any():
                return observations
            values = d["discharge"].isel(watershed=pos).values[mask]
            sel_times = times[mask]
        for t, raw in zip(sel_times, values, strict=True):
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


def _country_of(source: str) -> str:
    """Map a HYSETS data source to an ISO country code (HYDAT→CA, USGS→US, …)."""
    s = source.strip().upper()
    if s == "HYDAT":
        return "CA"
    if s in ("USGS", "USGS_NWIS"):
        return "US"
    if s in ("BANDAS", "CONAGUA"):
        return "MX"
    return "CA"  # remaining HYSETS sources are Canadian provincial networks
