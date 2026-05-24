"""UK Environment Agency Hydrology API connector."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()


@register("uk_ea")
class UKEnvironmentAgencyConnector(BaseConnector):
    slug = "uk_ea"
    display_name = "UK Environment Agency"
    base_url = "https://environment.data.gov.uk/hydrology"
    country_codes = ["GB"]

    async def fetch_stations(self) -> list[Station]:
        stations = []
        url = "/id/stations"
        params: dict | None = {"observedProperty": "waterFlow", "_limit": 500}

        while url:
            resp = await self._get(url, params=params)
            data = resp.json()
            for item in data.get("items", []):
                native_id = item.get("notation", item.get("stationReference", ""))
                lat = item.get("lat")
                lon = item.get("long")
                if not (native_id and lat and lon):
                    continue
                river = item.get("riverName")
                if isinstance(river, list):
                    river = river[0] if river else None
                area = item.get("catchmentArea")
                if isinstance(area, list):
                    area = area[0] if area else None
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=item.get("label", native_id) if isinstance(item.get("label"), str) else native_id,
                    latitude=float(lat),
                    longitude=float(lon),
                    country_code="GB",
                    river=river,
                    catchment_area_km2=float(area) if area else None,
                ))
            next_link = None
            for link in data.get("links", []):
                if link.get("rel") == "next":
                    next_link = link.get("href")
                    break
            url = next_link
            params = None

        return stations

    # Preferred measure suffixes in priority order: mean daily, instantaneous 15-min
    _MEASURE_PREFS = ["-flow-m-86400-m3s-qualified", "-flow-i-900-m3s-qualified"]

    async def _find_flow_measure(self, native_id: str) -> str | None:
        """Discover the best flow measure notation for a station."""
        try:
            resp = await self._get(f"/id/stations/{native_id}/measures")
            data = resp.json()
            measures = [
                item.get("notation", "")
                for item in data.get("items", [])
                if "flow" in item.get("parameterName", "").lower()
            ]
            for pref in self._MEASURE_PREFS:
                for m in measures:
                    if m.endswith(pref):
                        return m
            return measures[0] if measures else None
        except Exception:
            pass
        return None

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        native_id = station_id.removeprefix(f"{self.slug}:")
        measure = await self._find_flow_measure(native_id)
        if not measure:
            raise ConnectorError(self.slug, f"No flow measure found for station {native_id}")

        resp = await self._get(
            f"/id/measures/{measure}/readings",
            params={
                "min-date": start.strftime("%Y-%m-%d"),
                "max-date": end.strftime("%Y-%m-%d"),
                "_limit": 10000,
            },
        )
        return self._parse_readings(resp.json(), station_id)

    def _parse_readings(self, data: dict, station_id: str) -> TimeSeriesChunk:
        observations = []
        for item in data.get("items", []):
            try:
                observations.append(Observation(
                    station_id=station_id,
                    timestamp=datetime.fromisoformat(item["dateTime"]),
                    discharge_m3s=float(item["value"]),
                    quality=self._map_quality(item.get("quality", "")),
                ))
            except (KeyError, ValueError, TypeError):
                continue

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    @staticmethod
    def _map_quality(flag: str) -> QualityFlag:
        flag_lower = flag.lower()
        if "good" in flag_lower:
            return QualityFlag.GOOD
        if "suspect" in flag_lower:
            return QualityFlag.SUSPECT
        if "estimated" in flag_lower:
            return QualityFlag.ESTIMATED
        return QualityFlag.RAW
