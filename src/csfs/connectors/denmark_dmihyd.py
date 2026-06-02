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

from datetime import UTC, datetime, timedelta

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

_BASE_URL = "https://vandah.miljoeportal.dk/api"
_PARAM_DISCHARGE = "Vandføring"

def _map_quality(raw: str | None) -> QualityFlag:
    """Map VanDa quality marks to CSFS flags."""
    # VanDa uses quality marks; for now, we treat standard results as GOOD
    # if they have a numeric value.
    return QualityFlag.GOOD


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

            # Coordinates are in ETRS89 / UTM zone 32N (SRID 25832) in metadata,
            # but we need WGS84 for the Station model.
            # For now, if metadata doesn't provide lat/lng in WGS84, we keep 0.0
            # or extract from another source if possible.
            # VanDa metadata usually only provides UTM 'location' objects.
            stations.append(Station(
                id=self._station_id(native_id),
                provider=self.slug,
                native_id=native_id,
                name=entry.get("name", "Unknown"),
                latitude=0.0, # TODO: UTM to WGS84 conversion if needed
                longitude=0.0,
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

        observations: list[Observation] = []
        for row in data:
            try:
                raw_time = row.get("measurementDateTime")
                if not raw_time:
                    continue
                ts = datetime.fromisoformat(raw_time.replace("Z", "+00:00"))
                
                # result is usually in l/s (liters per second), convert to m3/s
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
