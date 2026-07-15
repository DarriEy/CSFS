"""UK Environment Agency Hydrology API connector."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import (
    Observation,
    QualityFlag,
    Resolution,
    Station,
    TimeSeriesChunk,
    Variable,
)
from csfs.core.registry import register

logger = structlog.get_logger()


@register("uk_ea")
class UKEnvironmentAgencyConnector(BaseConnector):
    slug = "uk_ea"
    display_name = "UK Environment Agency"
    base_url = "https://environment.data.gov.uk/hydrology"
    country_codes = ["GB"]
    supported_variables = ("discharge", "stage")

    async def fetch_stations(self) -> list[Station]:
        stations = []
        url: str | None = "/id/stations"
        params: dict | None = {"observedProperty": "waterFlow", "_limit": 10000}

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
            next_link: str | None = None
            for link in data.get("links", []):
                if link.get("rel") == "next":
                    next_link = str(link["href"])
                    break
            url = next_link
            params = None

        return stations

    # Preferred measure suffixes in priority order: instantaneous 15-min, mean daily
    _MEASURE_PREFS = ["-flow-i-900-m3s-qualified", "-flow-m-86400-m3s-qualified"]
    # Level measure notations vary in their unit token (m / mAOD / mASD, all
    # metres), so prefer by period substring: instantaneous 15-min, mean daily.
    _LEVEL_MEASURE_PREFS = ["-level-i-900-", "-level-m-86400-"]

    async def _station_measures(self, native_id: str) -> list[dict]:
        """All measure items for a station, across both EA id schemes.

        EA migrated station ids to GUIDs (2026-07). The two lookups are
        complementary and mutually exclusive:
        - GUID stations resolve via the path form ``/id/stations/<guid>/measures``
          but return nothing for ``?station.stationReference=<guid>``;
        - legacy WISKI-reference stations (e.g. ``3400TH``) are the reverse.
        Try the path form first (the roster is now overwhelmingly GUIDs),
        then fall back to the stationReference query.
        """
        for fetch in (
            lambda: self._get(f"/id/stations/{native_id}/measures"),
            lambda: self._get(
                "/id/measures", params={"station.stationReference": native_id}
            ),
        ):
            try:
                items = list((await fetch()).json().get("items", []))
            except Exception:
                items = []
            if items:
                return items
        return []

    @staticmethod
    def _ordered(measures: list[str], prefs: list[str], *, suffix: bool) -> list[str]:
        """Order candidate notations by preference, best first, no dupes."""
        ranked: list[str] = []
        for pref in prefs:
            for m in measures:
                match = m.endswith(pref) if suffix else (pref in m)
                if match and m not in ranked:
                    ranked.append(m)
        ranked.extend(m for m in measures if m not in ranked)
        return ranked

    def _flow_candidates(self, items: list[dict]) -> list[str]:
        measures = [
            item.get("notation", "")
            for item in items
            if "flow" in item.get("parameterName", "").lower()
        ]
        return self._ordered(measures, self._MEASURE_PREFS, suffix=True)

    async def _find_flow_measure(self, native_id: str) -> str | None:
        """Best flow measure notation for a station (compat helper)."""
        candidates = self._flow_candidates(await self._station_measures(native_id))
        return candidates[0] if candidates else None

    def _level_candidates(self, items: list[dict]) -> list[str]:
        measures = [
            item.get("notation", "")
            for item in items
            if "level" in item.get("parameterName", "").lower()
        ]
        return self._ordered(measures, self._LEVEL_MEASURE_PREFS, suffix=False)

    async def _find_level_measure(self, native_id: str) -> str | None:
        """Best water-level measure notation for a station (compat helper)."""
        candidates = self._level_candidates(await self._station_measures(native_id))
        return candidates[0] if candidates else None

    @staticmethod
    def _measure_resolution(measure: str) -> Resolution:
        """Infer temporal resolution from an EA measure notation.

        ``i-900`` measures are instantaneous 15-min readings; ``m-86400``
        measures are daily means.
        """
        if "-i-900-" in measure:
            return Resolution.INSTANTANEOUS
        if "-m-86400-" in measure:
            return Resolution.DAILY_MEAN
        return Resolution.UNKNOWN

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        native_id = station_id.removeprefix(f"{self.slug}:")
        items = await self._station_measures(native_id)
        flow_candidates = self._flow_candidates(items)
        level_candidates = self._level_candidates(items)
        if not flow_candidates and not level_candidates:
            raise ConnectorError(
                self.slug, f"No flow or level measure found for station {native_id}"
            )

        all_observations: list[Observation] = []
        # Try candidates in preference order until one yields readings:
        # individual EA feeds go stale (the 15-min flow product froze on
        # 2026-06-15 while the daily-mean product stayed current), so an
        # empty preferred measure must not mask a healthy fallback.
        for measure in flow_candidates[:2]:
            flow_obs = await self._fetch_readings(
                measure, station_id, start, end,
                Variable.DISCHARGE, self._measure_resolution(measure),
            )
            if flow_obs:
                all_observations.extend(flow_obs)
                break
        for measure in level_candidates[:2]:
            # EA levels are already in metres (m / mAOD / mASD).
            level_obs = await self._fetch_readings(
                measure, station_id, start, end,
                Variable.STAGE, self._measure_resolution(measure),
            )
            if level_obs:
                all_observations.extend(level_obs)
                break

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=all_observations,
            fetched_at=datetime.now(UTC),
        )

    async def _fetch_readings(
        self,
        measure: str,
        station_id: str,
        start: datetime,
        end: datetime,
        variable: Variable,
        resolution: Resolution,
    ) -> list[Observation]:
        """Fetch and parse all readings pages for one measure."""
        observations: list[Observation] = []
        url: str | None = f"/id/measures/{measure}/readings"
        params: dict | None = {
            "min-date": start.strftime("%Y-%m-%d"),
            "max-date": end.strftime("%Y-%m-%d"),
            "_limit": 10000,
        }

        while url:
            resp = await self._get(url, params=params)
            data = resp.json()
            observations.extend(
                self._parse_readings(data, station_id, variable, resolution)
            )

            next_link: str | None = None
            for link in data.get("links", []):
                if link.get("rel") == "next":
                    next_link = str(link["href"])
                    break
            url = next_link
            params = None

        return observations

    def _parse_readings(
        self,
        data: dict,
        station_id: str,
        variable: Variable,
        resolution: Resolution,
    ) -> list[Observation]:
        observations = []
        for item in data.get("items", []):
            try:
                observations.append(Observation(
                    station_id=station_id,
                    timestamp=datetime.fromisoformat(item["dateTime"]),
                    variable=variable,
                    resolution=resolution,
                    value=float(item["value"]),
                    quality=self._map_quality(item.get("quality", "")),
                ))
            except (KeyError, ValueError, TypeError):
                continue

        return observations

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
