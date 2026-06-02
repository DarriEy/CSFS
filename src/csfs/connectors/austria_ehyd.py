# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""eHYD Austria connector via the official BMLUK WFS service.

Provides real-time and metadata for ~800 surface water stations in Austria.
Data source: https://ehyd.gv.at/
WFS Service: https://gis.lfrz.gv.at/api/geodata/i000501/wfs
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


@register("austria_ehyd")
class AustriaEhydConnector(BaseConnector):
    """Connector for the Austrian eHYD (Hydrographischer Dienst) service."""

    slug = "austria_ehyd"
    display_name = "eHYD (Austria)"
    # NOTE: trailing slash is required so httpx joins "wfs" cleanly; the server
    # returns 404 for ".../wfs/?..." but 200 for ".../wfs?...".
    base_url = "https://gis.lfrz.gv.at/api/geodata/i000501/"
    country_codes = ["AT"]

    # WFS Layer Names
    _STATION_LAYER = "i000501:messstellen_owf"
    _DATA_LAYER = "i000501:pegel_aktuell"

    async def fetch_stations(self) -> list[Station]:
        """Fetch all surface water stations from the eHYD WFS service."""
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": self._STATION_LAYER,
            "outputFormat": "application/json",
        }

        try:
            resp = await self._get("wfs", params=params)
            data = resp.json()
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch station list from WFS: {exc}",
            ) from exc

        if not isinstance(data, dict) or "features" not in data:
            raise DataFormatError(
                self.slug,
                "WFS response is not a valid GeoJSON FeatureCollection",
            )

        stations: list[Station] = []
        for feature in data["features"]:
            props = feature.get("properties", {})
            hzbnr = props.get("hzbnr")
            if not hzbnr:
                continue

            # Only include stations that measure discharge (Durchfluss)
            art = props.get("messstellenart", "").lower()
            if "durchfluss" not in art:
                continue

            geom = feature.get("geometry", {})
            coords = geom.get("coordinates", [0.0, 0.0])

            stations.append(Station(
                id=self._station_id(str(hzbnr)),
                provider=self.slug,
                native_id=str(hzbnr),
                name=props.get("name", "Unknown"),
                latitude=coords[1],
                longitude=coords[0],
                country_code="AT",
                river=props.get("gewaesser"),
                # WFS properties don't usually include catchment area
                catchment_area_km2=None,
                is_active=props.get("aufgelassen") is None,
            ))

        logger.info(
            "stations_fetched",
            provider=self.slug,
            count=len(stations),
        )
        return stations

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the latest real-time observations for a station.
        
        Note: The eHYD WFS 'pegel_aktuell' layer typically provides the most
        recent single measurement.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")
        
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeNames": self._DATA_LAYER,
            "outputFormat": "application/json",
            "cql_filter": f"hzbnr='{native_id}'",
        }

        try:
            resp = await self._get("wfs", params=params)
            data = resp.json()
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch latest data for station {native_id}: {exc}",
            ) from exc

        features = data.get("features", [])
        if not features:
            logger.warning(
                "no_latest_data",
                provider=self.slug,
                station=native_id,
            )
            return self._empty_chunk(station_id)

        observations: list[Observation] = []
        for feature in features:
            props = feature.get("properties", {})
            
            # Check if this feature is actually discharge (Q)
            param = props.get("parameter", "").upper()
            if param != "Q":
                continue
                
            raw_value = props.get("wert")
            raw_time = props.get("zeitpunkt")
            
            if raw_value is None or not raw_time:
                continue
                
            try:
                ts = datetime.fromisoformat(raw_time)
                # Ensure UTC
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=UTC)
            except ValueError:
                continue
                
            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=float(raw_value),
                quality=QualityFlag.GOOD, # WFS 'aktuell' values are screened
            ))

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch observations for a time range.
        
        Currently, the WFS only provides the 'aktuell' (latest) value.
        Historical data via WFS is not supported.
        """
        # If the requested range includes 'now', try to get the latest value.
        now = datetime.now(UTC)
        if start <= now <= (end + timedelta(minutes=15)):
            return await self.fetch_latest(station_id)
            
        logger.warning(
            "historical_data_not_supported_via_wfs",
            provider=self.slug,
            station=station_id,
        )
        return self._empty_chunk(station_id)

    def _empty_chunk(self, station_id: str) -> TimeSeriesChunk:
        """Return an empty TimeSeriesChunk for a station."""
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )
