"""Lithuania meteo.lt connector -- hydrological observation data.

The Lithuanian Hydrometeorological Service provides hydro-station data
through a public JSON API at https://api.meteo.lt/v1.  No authentication
is required.

Endpoints used
--------------
* Hydro station listing:
  GET https://api.meteo.lt/v1/hydro-stations
  Returns a JSON array of station objects with ``code``, ``name``,
  ``coordinates`` (latitude/longitude), and ``waterBody``.

* Measured observations (one day at a time):
  GET https://api.meteo.lt/v1/hydro-stations/{code}/observations/measured/{YYYY-MM-DD}
  Returns JSON ``{observations: [{observationTimeUtc, waterLevel, waterTemperature}]}``.
  Hourly readings (up to 24 per day).

**Important:** This API provides *water level* (cm), **not** discharge.
The ``discharge_m3s`` field in the returned observations stores water level
values for downstream mapping convenience.
"""

from __future__ import annotations

import contextlib
from datetime import UTC, date, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import (
    Observation,
    QualityFlag,
    Station,
    TimeSeriesChunk,
)
from csfs.core.registry import register

logger = structlog.get_logger()


@register("lithuania_lhmt")
class LithuaniaLhmtConnector(BaseConnector):
    """Connector for Lithuanian Hydrometeorological Service (meteo.lt).

    Observations are *water level* (cm), not discharge (m3/s).
    Values are stored in ``discharge_m3s`` for interface compatibility.
    """

    slug = "lithuania_lhmt"
    display_name = "LHMT (Lithuania)"
    base_url = "https://api.meteo.lt"
    country_codes = ["LT"]

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all hydro-stations from the meteo.lt API."""
        try:
            resp = await self._get("/v1/hydro-stations")
        except httpx.HTTPStatusError as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch station list: "
                f"HTTP {exc.response.status_code}",
            ) from exc

        items = resp.json()
        if not isinstance(items, list):
            return []

        return self._parse_stations(items)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch water-level observations for *station_id* over [start, end].

        The API serves one calendar day per request, so we iterate
        day-by-day from *start* to *end* (inclusive).

        **Note:** returned values are water level (cm), not discharge.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")
        observations: list[Observation] = []

        current = start.date() if isinstance(start, datetime) else start
        end_date = end.date() if isinstance(end, datetime) else end

        while current <= end_date:
            day_obs = await self._fetch_day(
                native_id, current, station_id,
            )
            observations.extend(day_obs)
            current += timedelta(days=1)

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent observations (today)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id, start=now, end=now,
        )

    # -----------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------

    async def _fetch_day(
        self,
        native_id: str,
        day: date,
        station_id: str,
    ) -> list[Observation]:
        """Fetch observations for a single calendar day."""
        date_str = day.isoformat()
        path = (
            f"/v1/hydro-stations/{native_id}"
            f"/observations/measured/{date_str}"
        )

        try:
            resp = await self._get(path)
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                logger.info(
                    "no_observations_for_day",
                    provider=self.slug,
                    station=native_id,
                    date=date_str,
                )
                return []
            raise ConnectorError(
                self.slug,
                f"Failed to fetch observations for "
                f"{native_id} on {date_str}: "
                f"HTTP {exc.response.status_code}",
            ) from exc

        data = resp.json()
        items = data.get("observations", [])
        return self._parse_observations(items, station_id)

    def _parse_stations(
        self,
        items: list[dict],
    ) -> list[Station]:
        """Parse the station array from /v1/hydro-stations."""
        stations: list[Station] = []
        for entry in items:
            code = str(entry.get("code", "")).strip()
            if not code:
                continue

            coords = entry.get("coordinates", {})
            lat_raw = coords.get("latitude")
            lon_raw = coords.get("longitude")
            if lat_raw is None or lon_raw is None:
                logger.warning(
                    "station_missing_coords",
                    provider=self.slug,
                    station=code,
                )
                continue

            try:
                lat = float(str(lat_raw))
                lon = float(str(lon_raw))
            except (ValueError, TypeError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    station=code,
                    error=str(exc),
                )
                continue

            stations.append(Station(
                id=self._station_id(code),
                provider=self.slug,
                native_id=code,
                name=entry.get("name", code),
                latitude=lat,
                longitude=lon,
                country_code="LT",
                river=entry.get("waterBody"),
            ))

        return stations

    def _parse_observations(
        self,
        items: list[dict],
        station_id: str,
    ) -> list[Observation]:
        """Parse a day's observations into ``Observation`` models.

        ``waterLevel`` is stored in ``discharge_m3s`` (it is actually
        water level in cm).
        """
        observations: list[Observation] = []
        for entry in items:
            ts_raw = entry.get("observationTimeUtc")
            if ts_raw is None:
                continue

            try:
                ts = datetime.fromisoformat(str(ts_raw))
            except ValueError:
                logger.warning(
                    "observation_invalid_timestamp",
                    provider=self.slug,
                    timestamp=ts_raw,
                )
                continue

            value_raw = entry.get("waterLevel")
            water_level: float | None = None
            if value_raw is not None:
                with contextlib.suppress(ValueError, TypeError):
                    water_level = float(str(value_raw))

            quality = (
                QualityFlag.MISSING
                if water_level is None
                else QualityFlag.RAW
            )

            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=water_level,
                quality=quality,
            ))

        return observations
