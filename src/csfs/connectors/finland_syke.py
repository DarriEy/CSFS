"""SYKE connector — Finnish Environment Institute hydrology data.

Uses the Hertta/OIVA open data API for hydrological observations from Finland.
Base URL: https://rajapinnat.ymparisto.fi/api/hydrology/v1

Note: The exact API schema may evolve. This connector tries two endpoint patterns:
  - Primary: /stations + /observations
  - Fallback: /sites + /values
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()


def _quality_from_syke(raw: str | None) -> QualityFlag:
    """Map SYKE quality codes to CSFS quality flags.

    SYKE quality codes (known patterns):
        "good", "verified", "2"  -> GOOD
        "suspect", "1"           -> SUSPECT
        "estimated"              -> ESTIMATED
        None / ""                -> RAW (no quality info provided)
    """
    if raw is None:
        return QualityFlag.RAW
    code = raw.strip().lower()
    if code in ("good", "verified", "2", "approved"):
        return QualityFlag.GOOD
    if code in ("suspect", "1", "uncertain"):
        return QualityFlag.SUSPECT
    if code in ("estimated", "3"):
        return QualityFlag.ESTIMATED
    if code == "":
        return QualityFlag.RAW
    return QualityFlag.RAW


@register("finland_syke")
class FinlandSYKEConnector(BaseConnector):
    slug = "finland_syke"
    display_name = "SYKE Hydrology (Finland)"
    base_url = "https://rajapinnat.ymparisto.fi/api/hydrology/v1"
    country_codes = ["FI"]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all hydrological stations from SYKE.

        Tries the /stations endpoint first; falls back to /sites if that fails.
        """
        try:
            resp = await self._get("/stations", params={"format": "json"})
            data = resp.json()
            return self._parse_stations_primary(data)
        except DataFormatError:
            raise
        except (httpx.HTTPStatusError, ConnectorError, KeyError, TypeError) as exc:
            logger.info(
                "primary_stations_endpoint_failed_trying_fallback",
                provider=self.slug,
                error=str(exc),
            )

        try:
            resp = await self._get("/sites", params={"format": "json"})
            data = resp.json()
            return self._parse_stations_fallback(data)
        except (httpx.HTTPStatusError, ConnectorError) as exc:
            raise ConnectorError(
                self.slug, f"Failed to fetch stations from both endpoints: {exc}"
            ) from exc

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for a station over [start, end].

        Tries the /observations endpoint first; falls back to /values.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        # Ensure start/end are timezone-aware (UTC) for comparison
        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)
        if end.tzinfo is None:
            end = end.replace(tzinfo=UTC)

        try:
            return await self._fetch_observations_primary(native_id, station_id, start, end)
        except DataFormatError:
            raise
        except (httpx.HTTPStatusError, ConnectorError, KeyError, TypeError) as exc:
            logger.info(
                "primary_observations_endpoint_failed_trying_fallback",
                provider=self.slug,
                station=native_id,
                error=str(exc),
            )

        try:
            return await self._fetch_observations_fallback(native_id, station_id, start, end)
        except (httpx.HTTPStatusError, ConnectorError) as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch observations for station {native_id}: {exc}",
            ) from exc

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent observations (last 24 h)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # ------------------------------------------------------------------
    # Primary endpoint helpers (/stations, /observations)
    # ------------------------------------------------------------------

    async def _fetch_observations_primary(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch from /observations endpoint."""
        params = {
            "stationId": native_id,
            "startTime": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "endTime": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "variable": "discharge",
            "format": "json",
        }
        resp = await self._get("/observations", params=params)
        data = resp.json()
        return self._parse_observations_primary(data, station_id, start, end)

    def _parse_stations_primary(self, data: list | dict) -> list[Station]:
        """Parse the /stations response (expected: JSON array of station objects)."""
        entries = data if isinstance(data, list) else data.get("stations", data.get("data", []))
        if not isinstance(entries, list):
            raise DataFormatError(self.slug, "Expected list of stations from /stations endpoint")

        stations: list[Station] = []
        for entry in entries:
            native_id = str(entry.get("id", entry.get("stationId", "")))
            if not native_id:
                continue

            try:
                lat = float(entry.get("lat", entry.get("latitude", 0.0)))
                lon = float(entry.get("lon", entry.get("longitude", 0.0)))
            except (ValueError, TypeError) as exc:
                logger.warning(
                    "station_coordinate_parse_failed",
                    provider=self.slug,
                    station=native_id,
                    error=str(exc),
                )
                continue

            river = entry.get("river", entry.get("waterBodyName"))
            catchment = entry.get("catchmentArea")
            catchment_km2 = float(catchment) if catchment is not None else None

            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=entry.get("name", entry.get("stationName", "")),
                    latitude=lat,
                    longitude=lon,
                    country_code="FI",
                    river=river,
                    catchment_area_km2=catchment_km2,
                    is_active=entry.get("active", True),
                ))
            except (ValueError, KeyError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    station=native_id,
                    error=str(exc),
                )
                continue
        return stations

    def _parse_observations_primary(
        self,
        data: list | dict,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Parse the /observations response."""
        entries = data if isinstance(data, list) else data.get("observations", data.get("data", []))
        if not isinstance(entries, list):
            raise DataFormatError(self.slug, "Expected list of observations from /observations")

        observations: list[Observation] = []
        for entry in entries:
            time_str = entry.get("time", entry.get("timestamp", entry.get("dateTime")))
            if time_str is None:
                continue

            try:
                ts = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            except (ValueError, AttributeError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid timestamp in observation: {time_str}",
                ) from exc

            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)

            # Client-side date range filter
            if ts < start or ts > end:
                continue

            raw_value = entry.get("value", entry.get("discharge"))
            discharge = float(raw_value) if raw_value is not None else None
            quality_code = entry.get("quality", entry.get("qualityCode"))
            quality = QualityFlag.MISSING if discharge is None else _quality_from_syke(quality_code)

            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=discharge,
                quality=quality,
            ))

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # ------------------------------------------------------------------
    # Fallback endpoint helpers (/sites, /values)
    # ------------------------------------------------------------------

    def _parse_stations_fallback(self, data: list | dict) -> list[Station]:
        """Parse the /sites response (alternative endpoint)."""
        entries = data if isinstance(data, list) else data.get("sites", data.get("data", []))
        if not isinstance(entries, list):
            raise DataFormatError(self.slug, "Expected list of sites from /sites endpoint")

        stations: list[Station] = []
        for entry in entries:
            native_id = str(entry.get("siteId", entry.get("id", "")))
            if not native_id:
                continue

            try:
                lat = float(entry.get("latitude", entry.get("lat", 0.0)))
                lon = float(entry.get("longitude", entry.get("lon", 0.0)))
            except (ValueError, TypeError) as exc:
                logger.warning(
                    "station_coordinate_parse_failed",
                    provider=self.slug,
                    station=native_id,
                    error=str(exc),
                )
                continue

            catchment = entry.get("catchmentArea")
            catchment_km2 = float(catchment) if catchment is not None else None

            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=entry.get("siteName", entry.get("name", "")),
                    latitude=lat,
                    longitude=lon,
                    country_code="FI",
                    catchment_area_km2=catchment_km2,
                    is_active=True,
                ))
            except (ValueError, KeyError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    station=native_id,
                    error=str(exc),
                )
                continue
        return stations

    async def _fetch_observations_fallback(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch from /values endpoint (alternative pattern)."""
        params = {
            "siteId": native_id,
            "startDate": start.strftime("%Y-%m-%d"),
            "endDate": end.strftime("%Y-%m-%d"),
            "param": "Q",
            "format": "json",
        }
        resp = await self._get("/values", params=params)
        data = resp.json()
        return self._parse_observations_fallback(data, station_id, start, end)

    def _parse_observations_fallback(
        self,
        data: list | dict,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Parse the /values response (alternative endpoint)."""
        entries = data if isinstance(data, list) else data.get("values", data.get("data", []))
        if not isinstance(entries, list):
            raise DataFormatError(self.slug, "Expected list of values from /values endpoint")

        observations: list[Observation] = []
        for entry in entries:
            time_str = entry.get("dateTime", entry.get("time", entry.get("timestamp")))
            if time_str is None:
                continue

            try:
                ts = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
            except (ValueError, AttributeError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid timestamp in observation: {time_str}",
                ) from exc

            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)

            # Client-side date range filter
            if ts < start or ts > end:
                continue

            raw_value = entry.get("value")
            discharge = float(raw_value) if raw_value is not None else None
            quality_code = entry.get("quality", entry.get("qualityCode"))
            quality = QualityFlag.MISSING if discharge is None else _quality_from_syke(quality_code)

            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=discharge,
                quality=quality,
            ))

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )
