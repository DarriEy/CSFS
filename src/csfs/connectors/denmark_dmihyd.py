# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Denmark Hydrological connector — VanDa (Danmarks Miljøportal) API.

Provides real-time and historical river discharge data for ~450 stations.
Previously associated with DMI, the data is officially served via the
VanDa Hydro-API.

Primary source: https://vandah.miljoeportal.dk/api
Documentation: https://github.com/danmarksmiljoeportal/VanDa/wiki/Hydro-API
"""

from __future__ import annotations

import functools
from datetime import UTC, datetime, timedelta

import structlog
from pyproj import Transformer

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

_BASE_URL = "https://vandah.miljoeportal.dk/api"
_PARAM_DISCHARGE = "Vandføring"
# VanDa station coordinates are ETRS89 / UTM zone 32N (EPSG:25832), in metres.
_UTM32N_EPSG = 25832


@functools.lru_cache(maxsize=1)
def _utm32n_to_wgs84() -> Transformer:
    """Cached EPSG:25832 (easting, northing) -> WGS84 (lon, lat) transformer."""
    return Transformer.from_crs(_UTM32N_EPSG, 4326, always_xy=True)


def _to_wgs84(location: dict | None) -> tuple[float, float]:
    """Convert a VanDa UTM-32N location object to (latitude, longitude)."""
    if not location:
        return 0.0, 0.0
    try:
        lon, lat = _utm32n_to_wgs84().transform(
            float(location["x"]), float(location["y"]),
        )
        return lat, lon
    except (KeyError, TypeError, ValueError):
        return 0.0, 0.0


@register("denmark_dmihyd")
class DenmarkHydroConnector(BaseConnector):
    """Connector for Danish river discharge via Danmarks Miljøportal (VanDa)."""

    slug = "denmark_dmihyd"
    display_name = "VanDa Hydro (Denmark)"
    base_url = _BASE_URL
    country_codes = ["DK"]

    async def fetch_stations(self) -> list[Station]:
        """Fetch all stations with discharge data."""
        try:
            resp = await self._get("/stations")
            data = resp.json()
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch station list: {exc}",
            ) from exc

        if not isinstance(data, list):
            raise DataFormatError(self.slug, "Expected list of stations from VanDa")

        stations: list[Station] = []
        for entry in data:
            # Check if any measurement point has discharge (Vandføring)
            has_discharge = False
            for mp in entry.get("measurementPoints", []):
                for exam in mp.get("examinations", []):
                    if exam.get("parameter") == _PARAM_DISCHARGE:
                        has_discharge = True
                        break
                if has_discharge:
                    break
            
            if not has_discharge:
                continue

            native_id = str(entry.get("stationId", ""))
            if not native_id:
                continue

            # VanDa serves coordinates only as ETRS89 / UTM zone 32N (metres)
            # in the station 'location' object; convert to WGS84 lat/lon.
            latitude, longitude = _to_wgs84(entry.get("location"))
            stations.append(Station(
                id=self._station_id(native_id),
                provider=self.slug,
                native_id=native_id,
                name=entry.get("name", "Unknown"),
                latitude=latitude,
                longitude=longitude,
                country_code="DK",
                river=None, # VanDa doesn't explicitly name the river in root
                catchment_area_km2=None,
                is_active=True, # Active by default in VanDa real-time feed
            ))

        logger.info(
            "stations_fetched",
            provider=self.slug,
            count=len(stations),
        )
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for a station and range."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        
        # VanDa expects RFC 3339 without seconds: YYYY-MM-DDTHH:mmZ
        fmt = "%Y-%m-%dT%H:%MZ"
        params = {
            "stationId": native_id,
            "from": start.strftime(fmt),
            "to": end.strftime(fmt),
        }

        try:
            resp = await self._get("/water-flows", params=params)
            data = resp.json()
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch water flows for station {native_id}: {exc}",
            ) from exc

        # /water-flows returns a list of per-station objects, each wrapping the
        # actual readings in a nested 'results' array -- the timestamp/value
        # live there, NOT on the top-level object.
        observations: list[Observation] = []
        for station_obj in data:
            for row in station_obj.get("results", []):
                try:
                    if row.get("parameter") != _PARAM_DISCHARGE:
                        continue
                    raw_time = row.get("measurementDateTime")
                    if not raw_time:
                        continue
                    ts = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))

                    # VanDa discharge is in l/s; convert to m3/s.
                    raw_val = row.get("result")
                    if raw_val is None:
                        discharge = None
                        quality = QualityFlag.MISSING
                    else:
                        discharge = float(raw_val) / 1000.0
                        quality = QualityFlag.GOOD

                    observations.append(Observation(
                        station_id=station_id,
                        timestamp=ts,
                        discharge_m3s=discharge,
                        quality=quality,
                    ))
                except (ValueError, TypeError):
                    continue

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent 24 hours of observations."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )
