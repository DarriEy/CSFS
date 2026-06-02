# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""GloFAS connector -- Global Flood Awareness System (Copernicus EMS / ECMWF).

GloFAS is a global, *gridded* hydrological reanalysis + forecast model (river
discharge on a ~0.05 deg grid). It is NOT a gauge/station network. Its native
distribution channels do not fit CSFS's station-based connector contract:

- Copernicus Climate Data Store (``cdsapi``): gridded GRIB/NetCDF, requires a
  personal access token + dataset-license acceptance, and downloads are large
  (multi-GB). There is no per-station streaming time-series endpoint.
- The GloFAS web viewer "reporting points" (``mapserver:ReportingPoints``) are
  served as authenticated WMS layers; the per-point hydrographs come from WMS
  ``GetFeatureInfo`` behind a login session. There is no open station roster or
  open per-point JSON time-series API.

The only *open, keyless* way to pull GloFAS river discharge as a per-coordinate
JSON time series is the Open-Meteo Flood API, which redistributes GloFAS v4
daily river discharge (m3/s) and accepts a latitude/longitude. Because GloFAS is
gridded, there is no inherent station list to discover; instead this connector
treats a configurable set of *reporting points* (coordinates at major river
outlets) as virtual stations and samples the GloFAS grid at each via Open-Meteo.

Values are GloFAS **model output** (reanalysis/forecast), not gauge readings, so
observations are flagged ``ESTIMATED``.

References
----------
- Portal: https://global-flood.emergency.copernicus.eu/
- CDS dataset: https://ewds.climate.copernicus.eu/datasets/cems-glofas-historical
- Open-Meteo Flood API (keyless GloFAS v4): https://open-meteo.com/en/docs/flood-api
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import (
    Observation,
    QualityFlag,
    Station,
    TimeSeriesChunk,
)
from csfs.core.registry import register

logger = structlog.get_logger()

# Built-in default "reporting points": coordinates near the discharge maximum of
# major world rivers, where GloFAS (sampled by Open-Meteo as "largest river in a
# 5 km radius") yields a meaningful mainstem time series. Each is a virtual
# station. Operators can override/extend via config ``virtual_stations``.
# Fields: id, name, lat, lon, country, river.
_DEFAULT_REPORTING_POINTS: list[dict] = [
    {"id": "amazon_obidos", "name": "Amazon near Obidos",
     "lat": -1.95, "lon": -55.51, "country": "BR", "river": "Amazon"},
    {"id": "congo_lower", "name": "Congo (lower mainstem)",
     "lat": -4.88, "lon": 14.42, "country": "CD", "river": "Congo"},
    {"id": "mississippi_neworleans", "name": "Mississippi near New Orleans",
     "lat": 29.95, "lon": -90.06, "country": "US", "river": "Mississippi"},
    {"id": "nile_khartoum", "name": "Nile near Khartoum",
     "lat": 15.62, "lon": 32.50, "country": "SD", "river": "Nile"},
    {"id": "yangtze_datong", "name": "Yangtze at Datong",
     "lat": 30.77, "lon": 117.62, "country": "CN", "river": "Yangtze"},
    {"id": "ganges_padma", "name": "Ganges-Padma (Bangladesh)",
     "lat": 23.50, "lon": 90.50, "country": "BD", "river": "Ganges"},
    {"id": "danube_ceatal", "name": "Danube at Ceatal Izmail",
     "lat": 45.22, "lon": 28.73, "country": "RO", "river": "Danube"},
    {"id": "mekong_stungtreng", "name": "Mekong near Stung Treng",
     "lat": 13.53, "lon": 105.97, "country": "KH", "river": "Mekong"},
    {"id": "parana_lower", "name": "Parana (lower mainstem)",
     "lat": -33.70, "lon": -59.30, "country": "AR", "river": "Parana"},
    {"id": "lena_upper", "name": "Lena (upper mainstem)",
     "lat": 60.73, "lon": 114.88, "country": "RU", "river": "Lena"},
    {"id": "ob_khanty", "name": "Ob near Khanty-Mansiysk",
     "lat": 60.95, "lon": 69.05, "country": "RU", "river": "Ob"},
    {"id": "yenisei_igarka", "name": "Yenisei near Igarka",
     "lat": 67.43, "lon": 86.48, "country": "RU", "river": "Yenisei"},
    {"id": "niger_delta", "name": "Niger (lower delta)",
     "lat": 5.32, "lon": 6.47, "country": "NG", "river": "Niger"},
    {"id": "zambezi_tete", "name": "Zambezi at Tete",
     "lat": -16.17, "lon": 33.59, "country": "MZ", "river": "Zambezi"},
    {"id": "mackenzie_arctic", "name": "Mackenzie at Arctic Red River",
     "lat": 67.46, "lon": -133.74, "country": "CA", "river": "Mackenzie"},
]

# Open-Meteo Flood API (keyless), redistributing GloFAS v4 daily river discharge.
_FLOOD_API_BASE = "https://flood-api.open-meteo.com"
_FLOOD_API_PATH = "/v1/flood"


@register("glofas")
class GloFASConnector(BaseConnector):
    """Connector for GloFAS gridded river discharge, sampled at reporting points.

    GloFAS is a gridded model, not a gauge network, so this connector samples the
    GloFAS grid (via the keyless Open-Meteo Flood API) at a configurable set of
    coordinate "reporting points" that act as virtual stations. Discharge is
    GloFAS v4 daily mean in m3/s and is flagged ``ESTIMATED`` (model output).

    Configuration options:
        virtual_stations : list[dict]
            Override the built-in reporting points. Each item must provide
            ``id``, ``lat``, ``lon`` and may provide ``name``, ``country``,
            ``river``.
    """

    slug = "glofas"
    display_name = "GloFAS (Copernicus EMS / ECMWF, via Open-Meteo)"
    base_url = _FLOOD_API_BASE
    country_codes: list[str] = ["global"]

    def _reporting_points(self) -> list[dict]:
        """Return the active reporting-point definitions (config or built-in)."""
        configured = self.config.get("virtual_stations")
        return configured if configured else _DEFAULT_REPORTING_POINTS

    def _point_by_native_id(self, native_id: str) -> dict | None:
        for point in self._reporting_points():
            if str(point["id"]) == native_id:
                return point
        return None

    async def fetch_stations(self) -> list[Station]:
        """Return the configured GloFAS reporting points as virtual stations."""
        stations: list[Station] = []
        for point in self._reporting_points():
            try:
                native_id = str(point["id"])
                stations.append(
                    Station(
                        id=self._station_id(native_id),
                        provider=self.slug,
                        native_id=native_id,
                        name=point.get("name", native_id),
                        latitude=float(point["lat"]),
                        longitude=float(point["lon"]),
                        country_code=point.get("country", "global"),
                        river=point.get("river"),
                        is_active=True,
                    )
                )
            except (KeyError, ValueError, TypeError) as exc:
                logger.warning(
                    "glofas_invalid_reporting_point",
                    provider=self.slug,
                    point=point,
                    error=str(exc),
                )
                continue

        logger.info("stations_fetched", provider=self.slug, count=len(stations))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch GloFAS daily river discharge for one reporting point."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        point = self._point_by_native_id(native_id)
        if point is None:
            raise ConnectorError(
                self.slug,
                f"Unknown GloFAS reporting point: {native_id}",
            )

        params = {
            "latitude": float(point["lat"]),
            "longitude": float(point["lon"]),
            "daily": "river_discharge",
            "start_date": start.date().isoformat(),
            "end_date": end.date().isoformat(),
        }

        try:
            resp = await self._get(_FLOOD_API_PATH, params=params)
            payload = resp.json()
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch GloFAS discharge for {native_id}: {exc}",
            ) from exc

        daily = payload.get("daily")
        if not isinstance(daily, dict):
            raise DataFormatError(
                self.slug, f"Unexpected Open-Meteo Flood API response for {native_id}"
            )

        times = daily.get("time") or []
        values = daily.get("river_discharge") or []

        observations: list[Observation] = []
        for raw_time, raw_val in zip(times, values, strict=False):
            try:
                ts = datetime.fromisoformat(raw_time).replace(tzinfo=UTC)
            except (ValueError, TypeError):
                continue
            discharge = None if raw_val is None else float(raw_val)
            observations.append(
                Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    # GloFAS reanalysis/forecast model output, not a gauge reading.
                    quality=QualityFlag.ESTIMATED,
                )
            )

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )
