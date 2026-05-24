"""Environment Canada Hydrometric Data connector (OGC API Features).

Uses two OGC collections on api.weather.gc.ca:
- hydrometric-stations: station metadata
- hydrometric-realtime: 5-min telemetry (last ~30 days, near-real-time)
- hydrometric-daily-mean: historical daily mean discharge (months of lag)

fetch_observations uses the realtime collection by default, falling back
to daily-mean for date ranges older than 30 days.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

EC_QUALITY_MAP: dict[str | None, QualityFlag] = {
    None: QualityFlag.RAW,
    "": QualityFlag.RAW,
    "A": QualityFlag.GOOD,
    "B": QualityFlag.ESTIMATED,
    "D": QualityFlag.SUSPECT,
    "E": QualityFlag.ESTIMATED,
    "R": QualityFlag.SUSPECT,
    "S": QualityFlag.SUSPECT,
    "Ice Conditions": QualityFlag.ESTIMATED,
}


@register("environment_canada")
class EnvironmentCanadaConnector(BaseConnector):
    slug = "environment_canada"
    display_name = "Environment Canada Hydrometric"
    base_url = "https://api.weather.gc.ca"
    country_codes = ["CA"]

    PAGE_SIZE = 500
    REALTIME_WINDOW_DAYS = 30

    async def fetch_stations(
        self,
        provinces: list[str] | None = None,
    ) -> list[Station]:
        target_provinces = provinces or self.config.get("provinces")
        all_stations: list[Station] = []
        offset = 0

        while True:
            params: dict = {
                "f": "json",
                "limit": self.PAGE_SIZE,
                "offset": offset,
            }
            if target_provinces:
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

        logger.info("stations_fetched", provider=self.slug, count=len(all_stations))
        return all_stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        native_id = station_id.removeprefix(f"{self.slug}:")
        cutoff = datetime.now(UTC) - timedelta(days=self.REALTIME_WINDOW_DAYS)
        start_aware = start if start.tzinfo else start.replace(tzinfo=UTC)
        end_aware = end if end.tzinfo else end.replace(tzinfo=UTC)

        if start_aware >= cutoff:
            return await self._fetch_realtime(native_id, station_id, start_aware, end_aware)

        if end_aware >= cutoff:
            hist = await self._fetch_daily_mean(native_id, station_id, start_aware, cutoff)
            rt = await self._fetch_realtime(native_id, station_id, cutoff, end_aware)
            return TimeSeriesChunk(
                station_id=station_id,
                provider=self.slug,
                observations=hist.observations + rt.observations,
                fetched_at=datetime.now(UTC),
            )

        return await self._fetch_daily_mean(native_id, station_id, start_aware, end_aware)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    async def _fetch_realtime(
        self, native_id: str, station_id: str, start: datetime, end: datetime,
    ) -> TimeSeriesChunk:
        observations: list[Observation] = []
        offset = 0

        while True:
            resp = await self._get(
                "/collections/hydrometric-realtime/items",
                params={
                    "f": "json",
                    "STATION_NUMBER": native_id,
                    "datetime": f"{start.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end.strftime('%Y-%m-%dT%H:%M:%SZ')}",
                    "sortby": "DATETIME",
                    "limit": 2000,
                    "offset": offset,
                },
            )
            data = resp.json()
            features = data.get("features", [])
            if not features:
                break

            for feat in features:
                obs = self._parse_realtime_feature(feat, station_id)
                if obs is not None:
                    observations.append(obs)

            if len(features) < 2000:
                break
            offset += 2000

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    async def _fetch_daily_mean(
        self, native_id: str, station_id: str, start: datetime, end: datetime,
    ) -> TimeSeriesChunk:
        observations: list[Observation] = []
        offset = 0

        while True:
            resp = await self._get(
                "/collections/hydrometric-daily-mean/items",
                params={
                    "f": "json",
                    "STATION_NUMBER": native_id,
                    "datetime": f"{start.strftime('%Y-%m-%dT%H:%M:%SZ')}/{end.strftime('%Y-%m-%dT%H:%M:%SZ')}",
                    "sortby": "DATE",
                    "limit": 1000,
                    "offset": offset,
                },
            )
            data = resp.json()
            features = data.get("features", [])
            if not features:
                break

            for feat in features:
                obs = self._parse_daily_mean_feature(feat, station_id)
                if obs is not None:
                    observations.append(obs)

            if len(features) < 1000:
                break
            offset += 1000

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    def _parse_station_feature(self, feature: dict) -> Station | None:
        props = feature.get("properties", {})
        geom = feature.get("geometry", {})
        native_id = props.get("STATION_NUMBER")
        coords = geom.get("coordinates", [])

        if not native_id or len(coords) < 2:
            return None

        lon, lat = coords[0], coords[1]
        drainage = props.get("DRAINAGE_AREA_GROSS")

        return Station(
            id=self._station_id(native_id),
            provider=self.slug,
            native_id=native_id,
            name=props.get("STATION_NAME", native_id),
            latitude=float(lat),
            longitude=float(lon),
            country_code="CA",
            river=props.get("STATION_NAME"),
            catchment_area_km2=float(drainage) if drainage is not None else None,
        )

    def _parse_realtime_feature(self, feature: dict, station_id: str) -> Observation | None:
        props = feature.get("properties", {})
        dt_str = props.get("DATETIME")
        if not dt_str:
            return None

        discharge = props.get("DISCHARGE")
        symbol = props.get("DISCHARGE_SYMBOL_EN")
        quality = EC_QUALITY_MAP.get(symbol, QualityFlag.RAW)
        if discharge is None:
            quality = QualityFlag.MISSING

        return Observation(
            station_id=station_id,
            timestamp=datetime.fromisoformat(dt_str),
            discharge_m3s=float(discharge) if discharge is not None else None,
            quality=quality,
        )

    def _parse_daily_mean_feature(self, feature: dict, station_id: str) -> Observation | None:
        props = feature.get("properties", {})
        date_str = props.get("DATE")
        if not date_str:
            return None

        discharge = props.get("DISCHARGE")
        symbol = props.get("DISCHARGE_SYMBOL_EN", "")
        quality = EC_QUALITY_MAP.get(symbol, QualityFlag.RAW)
        if discharge is None:
            quality = QualityFlag.MISSING

        return Observation(
            station_id=station_id,
            timestamp=datetime.fromisoformat(date_str),
            discharge_m3s=float(discharge) if discharge is not None else None,
            quality=quality,
        )
