"""Greece OpenHI connector — Open Hydrosystem Information Network.

OpenHI (https://openhi.net) provides open hydrological data for Greece,
including discharge time series from gauging stations.

Endpoints used
--------------
* Station listing:
  GET /api/stations?variable=discharge&format=json
  Returns a JSON array of gauging stations.

* Observations:
  GET /api/timeseries/{id}?start={ISO}&end={ISO}&format=json
  Returns ``[{timestamp, value, flag}, ...]``.

Both endpoints may evolve; the connector is written defensively with
fallback parsing and clear error messages.
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


def _flag_to_quality(flag: str | None) -> QualityFlag:
    """Map an OpenHI quality flag to a CSFS quality flag.

    Known flags: "VALIDATED", "RAW", "SUSPECT", "ESTIMATED", "MISSING".
    """
    if flag is None:
        return QualityFlag.RAW
    flag_upper = str(flag).upper().strip()
    mapping: dict[str, QualityFlag] = {
        "VALIDATED": QualityFlag.GOOD,
        "GOOD": QualityFlag.GOOD,
        "RAW": QualityFlag.RAW,
        "SUSPECT": QualityFlag.SUSPECT,
        "ESTIMATED": QualityFlag.ESTIMATED,
        "MISSING": QualityFlag.MISSING,
    }
    return mapping.get(flag_upper, QualityFlag.RAW)


@register("greece_openhi")
class GreeceOpenhiConnector(BaseConnector):
    """Connector for Greece's OpenHI hydrological data."""

    slug = "greece_openhi"
    display_name = "OpenHI (Greece)"
    base_url = "https://openhi.net"
    country_codes = ["GR"]

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all discharge gauging stations from OpenHI."""
        try:
            resp = await self._get(
                "/api/stations",
                params={"variable": "discharge", "format": "json"},
            )
        except httpx.HTTPStatusError as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch station list: "
                f"HTTP {exc.response.status_code}",
            ) from exc

        return self._parse_stations(resp.json())

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for *station_id* over [start, end]."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        params: dict[str, str] = {
            "start": start.isoformat(),
            "end": end.isoformat(),
            "format": "json",
        }

        try:
            resp = await self._get(
                f"/api/timeseries/{native_id}",
                params=params,
            )
        except httpx.HTTPStatusError as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch observations for {native_id}: "
                f"HTTP {exc.response.status_code}",
            ) from exc

        return self._parse_observations(resp.json(), station_id)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent discharge observations (last 24 h)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # -----------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------

    def _parse_stations(self, data: list[dict] | dict) -> list[Station]:
        """Parse the OpenHI station-list JSON into ``Station`` models.

        The API may return a bare list or wrap it under a key.
        Both forms are handled defensively.
        """
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("stations", data.get("results", []))
        else:
            return []

        stations: list[Station] = []
        for entry in items:
            native_id = str(entry.get("id", "")).strip()
            if not native_id:
                continue

            lat = entry.get("latitude") or entry.get("lat")
            lon = entry.get("longitude") or entry.get("lon")
            if lat is None or lon is None:
                logger.warning(
                    "station_missing_coords",
                    provider=self.slug,
                    station=native_id,
                )
                continue

            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=entry.get("name", native_id),
                    latitude=float(lat),
                    longitude=float(lon),
                    country_code="GR",
                    river=entry.get("river"),
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

    def _parse_observations(
        self,
        data: dict | list,
        station_id: str,
    ) -> TimeSeriesChunk:
        """Parse the OpenHI observations response into a ``TimeSeriesChunk``.

        Expected shape::

            [
                {"timestamp": "2024-06-01T12:00:00Z", "value": 34.5,
                 "flag": "VALIDATED"},
                ...
            ]

        The response may also be wrapped in a dict.
        """
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("data", data.get("timeseries", []))
        else:
            raise DataFormatError(
                self.slug,
                f"Unexpected response type: {type(data).__name__}",
            )

        observations: list[Observation] = []
        for entry in items:
            try:
                ts = datetime.fromisoformat(entry["timestamp"])
            except (KeyError, ValueError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid or missing timestamp: {exc}",
                ) from exc

            value = entry.get("value")
            discharge = float(value) if value is not None else None
            flag = entry.get("flag")
            quality = (
                QualityFlag.MISSING
                if discharge is None
                else _flag_to_quality(flag)
            )

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
