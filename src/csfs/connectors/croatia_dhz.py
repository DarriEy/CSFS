"""Croatia DHZ connector — Državni hidrometeorološki zavod.

The Croatian Hydrological and Meteorological Service (DHZ / DHMZ) operates
Croatia's river monitoring network via https://hidro.dhz.hr.

Endpoints used
--------------
* Station listing:
  GET /api/stations?format=json
  Returns a JSON array of gauging stations.

* Observations:
  GET /api/data?station={sifra}&variable=protok
       &from={YYYY-MM-DD}&to={YYYY-MM-DD}&format=json
  Returns ``[{datum, vrijednost}, ...]``.

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


@register("croatia_dhz")
class CroatiaDhzConnector(BaseConnector):
    """Connector for Croatia's DHZ hydrological data."""

    slug = "croatia_dhz"
    display_name = "DHZ Hidro (Croatia)"
    base_url = "https://hidro.dhz.hr"
    country_codes = ["HR"]

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all gauging stations from DHZ."""
        try:
            resp = await self._get(
                "/api/stations",
                params={"format": "json"},
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
            "station": native_id,
            "variable": "protok",
            "from": start.strftime("%Y-%m-%d"),
            "to": end.strftime("%Y-%m-%d"),
            "format": "json",
        }

        try:
            resp = await self._get("/api/data", params=params)
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
        """Parse the DHZ station-list JSON into ``Station`` models.

        The API may return a bare list or wrap it under a key.
        Both forms are handled defensively.
        """
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("stations", data.get("stanice", []))
        else:
            return []

        stations: list[Station] = []
        for entry in items:
            native_id = str(entry.get("sifra", "")).strip()
            if not native_id:
                continue

            lat = entry.get("lat") or entry.get("latitude")
            lon = entry.get("lon") or entry.get("longitude")
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
                    name=entry.get("naziv", native_id),
                    latitude=float(lat),
                    longitude=float(lon),
                    country_code="HR",
                    river=entry.get("rijeka"),
                    catchment_area_km2=entry.get("sliv"),
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
        """Parse the DHZ observations response into a ``TimeSeriesChunk``.

        Expected shape::

            [
                {"datum": "2024-06-01T12:00:00", "vrijednost": 34.5},
                ...
            ]

        The response may also be wrapped in a dict.
        """
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("podaci", data.get("data", []))
        else:
            raise DataFormatError(
                self.slug,
                f"Unexpected response type: {type(data).__name__}",
            )

        observations: list[Observation] = []
        for entry in items:
            try:
                ts = datetime.fromisoformat(entry["datum"])
            except (KeyError, ValueError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid or missing timestamp: {exc}",
                ) from exc

            value = entry.get("vrijednost")
            discharge = float(value) if value is not None else None
            quality = (
                QualityFlag.MISSING
                if discharge is None
                else QualityFlag.RAW
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
