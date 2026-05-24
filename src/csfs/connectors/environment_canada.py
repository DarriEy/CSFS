"""Environment Canada Hydrometric Data connector (OGC API Features)."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# Maps DISCHARGE_SYMBOL_EN values to CSFS quality flags.
# See: https://wateroffice.ec.gc.ca/contactus/faq_e.html
EC_QUALITY_MAP: dict[str | None, QualityFlag] = {
    None: QualityFlag.RAW,
    "": QualityFlag.RAW,
    "A": QualityFlag.GOOD,          # Approved / Partial Day
    "B": QualityFlag.ESTIMATED,     # Ice conditions — estimated
    "D": QualityFlag.SUSPECT,       # Dry
    "E": QualityFlag.ESTIMATED,     # Estimated
    "R": QualityFlag.SUSPECT,       # Revised
    "S": QualityFlag.SUSPECT,       # Sample(s) collected this day
}


@register("environment_canada")
class EnvironmentCanadaConnector(BaseConnector):
    slug = "environment_canada"
    display_name = "Environment Canada Hydrometric"
    base_url = "https://api.weather.gc.ca"
    country_codes = ["CA"]

    # Canadian province/territory codes for reference
    PROVINCES = [
        "AB", "BC", "MB", "NB", "NL", "NS", "NT", "NU",
        "ON", "PE", "QC", "SK", "YT",
    ]

    PAGE_SIZE = 500

    async def fetch_stations(
        self,
        provinces: list[str] | None = None,
    ) -> list[Station]:
        """Return all hydrometric stations from the OGC API.

        Uses offset-based pagination to walk through the full station list.
        Optionally filter by province codes.
        """
        target_provinces = (
            provinces
            or self.config.get("provinces")
            or None  # None means no filter — fetch all
        )

        all_stations: list[Station] = []
        offset = 0

        while True:
            params: dict = {
                "f": "json",
                "limit": self.PAGE_SIZE,
                "offset": offset,
            }
            if target_provinces:
                # OGC API supports PROV_TERR_STATE_LOC filter
                params["PROV_TERR_STATE_LOC"] = ",".join(target_provinces)

            resp = await self._get(
                "/collections/hydrometric-stations/items",
                params=params,
            )
            data = resp.json()
            features = data.get("features", [])
            if not features:
                break

            for feat in features:
                station = self._parse_station_feature(feat)
                if station is not None:
                    all_stations.append(station)

            if len(features) < self.PAGE_SIZE:
                break
            offset += self.PAGE_SIZE

        logger.info(
            "stations_fetched",
            provider=self.slug,
            count=len(all_stations),
        )
        return all_stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch daily mean discharge from the OGC API for a station and time range."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        observations: list[Observation] = []
        offset = 0
        page_size = 1000

        while True:
            params: dict = {
                "f": "json",
                "STATION_NUMBER": native_id,
                "limit": page_size,
                "offset": offset,
                "sortby": "DATE",
            }
            # Add date range filters if the API supports datetime parameter
            if start:
                params["datetime"] = (
                    f"{start.strftime('%Y-%m-%dT%H:%M:%SZ')}"
                    f"/{end.strftime('%Y-%m-%dT%H:%M:%SZ')}"
                )

            resp = await self._get(
                "/collections/hydrometric-daily-mean/items",
                params=params,
            )
            data = resp.json()
            features = data.get("features", [])
            if not features:
                break

            for feat in features:
                obs = self._parse_daily_mean_feature(feat, station_id)
                if obs is not None:
                    observations.append(obs)

            if len(features) < page_size:
                break
            offset += page_size

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent daily mean observations."""
        from datetime import timedelta

        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(days=7),
            end=now,
        )

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    def _parse_station_feature(self, feature: dict) -> Station | None:
        """Parse a single GeoJSON Feature into a Station model."""
        props = feature.get("properties", {})
        geom = feature.get("geometry", {})

        native_id = props.get("STATION_NUMBER")
        coords = geom.get("coordinates", [])

        if not native_id or len(coords) < 2:
            return None

        lon, lat = coords[0], coords[1]

        drainage = props.get("DRAINAGE_AREA_GROSS")
        catchment_km2 = float(drainage) if drainage is not None else None

        return Station(
            id=self._station_id(native_id),
            provider=self.slug,
            native_id=native_id,
            name=props.get("STATION_NAME", native_id),
            latitude=float(lat),
            longitude=float(lon),
            country_code="CA",
            river=props.get("STATION_NAME"),  # EC doesn't separate river name
            catchment_area_km2=catchment_km2,
        )

    def _parse_daily_mean_feature(
        self,
        feature: dict,
        station_id: str,
    ) -> Observation | None:
        """Parse a single daily-mean GeoJSON Feature into an Observation."""
        props = feature.get("properties", {})
        date_str = props.get("DATE")
        if not date_str:
            return None

        discharge_raw = props.get("DISCHARGE")
        discharge = float(discharge_raw) if discharge_raw is not None else None

        symbol = props.get("DISCHARGE_SYMBOL_EN", "")
        quality = EC_QUALITY_MAP.get(symbol, QualityFlag.RAW)
        if discharge is None:
            quality = QualityFlag.MISSING

        return Observation(
            station_id=station_id,
            timestamp=datetime.fromisoformat(date_str),
            discharge_m3s=discharge,
            quality=quality,
        )
