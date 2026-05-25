"""BAFU Hydrodaten connector — Swiss federal hydrological gauging stations.

Uses the third-party api.existenz.ch API which proxies Swiss FOEN/BAFU data.
The official hydrodaten.admin.ch site blocks programmatic API access.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()


@register("switzerland_bafu")
class SwitzerlandBafuConnector(BaseConnector):
    slug = "switzerland_bafu"
    display_name = "BAFU Hydrodaten (Switzerland)"
    base_url = "https://api.existenz.ch"
    country_codes = ["CH"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all stations that report flow data via api.existenz.ch."""
        try:
            resp = await self._get(
                "/apiv1/hydro/latest",
                params={"parameters": "flow"},
            )
        except Exception as exc:
            raise ConnectorError(
                self.slug, f"Failed to fetch station listing: {exc}"
            ) from exc

        try:
            data = resp.json()
        except ValueError as exc:
            raise DataFormatError(
                self.slug, "Station listing response is not valid JSON"
            ) from exc

        return self._parse_stations(data)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge measurements for a station over a time range.

        api.existenz.ch returns recent data.  The *start* / *end* window
        is used to filter the returned records client-side.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        try:
            resp = await self._get(
                "/apiv1/hydro/latest",
                params={"parameters": "flow", "locations": native_id},
            )
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch observations for station {native_id}: {exc}",
            ) from exc

        try:
            data = resp.json()
        except ValueError as exc:
            raise DataFormatError(
                self.slug, "Observations response is not valid JSON"
            ) from exc

        observations = self._parse_observations(data, station_id)

        # Client-side date filtering
        filtered = [
            obs for obs in observations
            if start <= obs.timestamp <= end
        ]

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=filtered,
            fetched_at=datetime.now(UTC),
        )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent discharge observations."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_stations(self, data: dict) -> list[Station]:
        """Parse the api.existenz.ch /hydro/latest response into stations.

        Response shape: {"source": "Swiss FOEN/BAFU", "payload": [{...}, ...]}
        Each payload entry has: timestamp, loc, par, val (and possibly others).
        We extract unique station locations from the payload.
        """
        if not isinstance(data, dict):
            logger.warning(
                "unexpected_station_format",
                provider=self.slug,
                type=type(data).__name__,
            )
            return []

        payload = data.get("payload", [])
        if not isinstance(payload, list):
            logger.warning(
                "unexpected_payload_format",
                provider=self.slug,
                type=type(payload).__name__,
            )
            return []

        # Deduplicate by location ID
        seen: dict[str, dict] = {}
        for entry in payload:
            if not isinstance(entry, dict):
                continue
            loc = entry.get("loc")
            if loc is not None and str(loc) not in seen:
                seen[str(loc)] = entry

        stations: list[Station] = []
        for native_id, entry in seen.items():
            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=entry.get("name", native_id),
                    latitude=float(entry.get("lat", 0.0)),
                    longitude=float(entry.get("lon", 0.0)),
                    country_code="CH",
                    river=entry.get("river"),
                ))
            except (ValueError, KeyError, TypeError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    station=native_id,
                    error=str(exc),
                )
                continue

        return stations

    def _parse_observations(
        self, data: dict, station_id: str
    ) -> list[Observation]:
        """Parse api.existenz.ch response payload into Observations.

        Each payload entry has: timestamp, loc, par, val.
        """
        if not isinstance(data, dict):
            raise DataFormatError(
                self.slug,
                f"Unexpected response type: {type(data).__name__}",
            )

        payload = data.get("payload", [])
        if not isinstance(payload, list):
            raise DataFormatError(
                self.slug,
                f"Unexpected payload type: {type(payload).__name__}",
            )

        observations: list[Observation] = []
        for entry in payload:
            if not isinstance(entry, dict):
                continue

            raw_ts = entry.get("timestamp")
            if raw_ts is None:
                continue

            try:
                if isinstance(raw_ts, (int, float)):
                    ts = datetime.fromtimestamp(raw_ts, tz=UTC)
                else:
                    ts = datetime.fromisoformat(str(raw_ts))
            except (ValueError, TypeError, OverflowError) as exc:
                logger.debug(
                    "observation_parse_skipped",
                    provider=self.slug,
                    record=str(entry)[:200],
                    error=str(exc),
                )
                continue

            raw_val = entry.get("val")
            discharge: float | None = None
            if raw_val is not None:
                try:
                    discharge = float(raw_val)
                except (ValueError, TypeError):
                    discharge = None

            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=discharge,
                quality=QualityFlag.RAW if discharge is not None else QualityFlag.MISSING,
            ))

        return observations
