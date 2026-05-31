# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Czech Republic ČHMÚ connector — opendata.chmi.cz hydrology feed.

The Czech Hydrometeorological Institute publishes a clean, unauthenticated
open-data tree of per-station JSON files:

* Station catalogue:
  ``GET /hydrology/now/metadata/meta1.json`` — a CSV-in-JSON table with
  ``objID, …, STATION_NAME, STREAM_NAME, GEOGR1 (lat), GEOGR2 (lon), …``.
* Current day (10-min resolution):
  ``GET /hydrology/now/data/<objID>.json``
* Prior days (rolling archive, one file per day):
  ``GET /hydrology/recent/data/<YYYYMMDD>_<objID>.json``

Each data file holds ``objList[].tsList[]`` series; discharge is the series
with ``tsConID == "Q"`` (unit ``M3_S``), level is ``H`` (``CM``). Timestamps
are ISO-8601 in UTC (trailing ``Z``).

``fetch_observations`` walks the requested window day by day — ``now`` for the
current UTC day, ``recent`` for earlier days — so a single window can span the
live feed and the archive. Because ``recent`` retains a year-plus of days, a
daily run with a multi-day lookback recovers full 10-min resolution.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

_META_PATH = "/hydrology/now/metadata/meta1.json"
# Series connector id for discharge (průtok); level is "H".
_DISCHARGE_TSCON = "Q"
# Cap the per-call day walk so a pathological window can't fan out unboundedly.
_MAX_DAYS = 60


@register("czechia_chmi")
class CzechiaChmiConnector(BaseConnector):
    """Connector for the Czech ČHMÚ hydrology open-data feed."""

    slug = "czechia_chmi"
    display_name = "ČHMÚ (Czech Republic)"
    base_url = "https://opendata.chmi.cz"
    country_codes = ["CZ"]

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return the station catalogue from the metadata table."""
        resp = await self._get(_META_PATH)
        stations = self._parse_metadata(resp.content)
        logger.info("stations_fetched", provider=self.slug, count=len(stations))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge for a station, walking the window day by day."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        today = datetime.now(UTC).date()

        observations: list[Observation] = []
        for day in _days_in_range(start, end):
            if day > today:
                continue
            path = (
                f"/hydrology/now/data/{native_id}.json"
                if day == today
                else f"/hydrology/recent/data/{day:%Y%m%d}_{native_id}.json"
            )
            try:
                resp = await self._get(path)
            except httpx.HTTPStatusError as exc:
                # A missing day (404) is normal at the edges of the archive.
                if exc.response.status_code == 404:
                    continue
                raise ConnectorError(
                    self.slug,
                    f"Failed to fetch {native_id} for {day}: {exc}",
                ) from exc

            observations.extend(
                self._parse_discharge(resp.content, station_id, start, end),
            )

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # -----------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------

    def _parse_metadata(self, content: bytes) -> list[Station]:
        """Parse meta1.json (CSV-in-JSON) into Station models."""
        try:
            payload = json.loads(content)
            table = payload["data"]["data"]
            columns = table["header"].split(",")
            rows = table["values"]
        except (json.JSONDecodeError, KeyError, AttributeError) as exc:
            raise DataFormatError(self.slug, f"Invalid CHMI metadata: {exc}") from exc

        idx = {name: i for i, name in enumerate(columns)}
        required = ("objID", "STATION_NAME", "GEOGR1", "GEOGR2")
        if not all(col in idx for col in required):
            raise DataFormatError(self.slug, "CHMI metadata missing expected columns")

        stations: list[Station] = []
        for row in rows:
            code = str(row[idx["objID"]]).strip()
            lat = _to_float(row[idx["GEOGR1"]])
            lon = _to_float(row[idx["GEOGR2"]])
            if not code or lat is None or lon is None:
                continue
            river = row[idx["STREAM_NAME"]] if "STREAM_NAME" in idx else None
            stations.append(Station(
                id=self._station_id(code),
                provider=self.slug,
                native_id=code,
                name=str(row[idx["STATION_NAME"]]).strip() or code,
                latitude=lat,
                longitude=lon,
                country_code="CZ",
                river=str(river).strip() if river else None,
            ))
        return stations

    def _parse_discharge(
        self,
        content: bytes,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Extract in-window discharge points from a station data file."""
        try:
            payload = json.loads(content)
            obj_list = payload.get("objList", [])
        except json.JSONDecodeError as exc:
            raise DataFormatError(self.slug, f"Invalid CHMI data: {exc}") from exc

        observations: list[Observation] = []
        for obj in obj_list:
            for ts in obj.get("tsList", []):
                if ts.get("tsConID") != _DISCHARGE_TSCON:
                    continue
                for point in ts.get("tsData", []):
                    value = point.get("value")
                    if value is None:
                        continue
                    when = _parse_iso(point.get("dt"))
                    if when is None or not (start <= when <= end):
                        continue
                    observations.append(Observation(
                        station_id=station_id,
                        timestamp=when,
                        discharge_m3s=float(value),
                        quality=QualityFlag.RAW,
                    ))
        return observations


def _to_float(value: object) -> float | None:
    if value is None:
        return None
    try:
        return float(str(value).strip())
    except (ValueError, AttributeError):
        return None


def _parse_iso(value: str | None) -> datetime | None:
    """Parse an ISO-8601 UTC timestamp ending in ``Z``."""
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


def _days_in_range(start: datetime, end: datetime):
    """Yield each calendar date covered by [start, end], capped at _MAX_DAYS."""
    day = start.date()
    last = end.date()
    count = 0
    while day <= last and count < _MAX_DAYS:
        yield day
        day += timedelta(days=1)
        count += 1
