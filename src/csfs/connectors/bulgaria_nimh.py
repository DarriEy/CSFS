# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Bulgaria NIMH connector -- National Institute of Meteorology and Hydrology.

NIMH publishes daily river-runoff on its open-data portal as an HTML table that
is rendered per day via a POST:

    POST https://info.meteo.bg/openData/river-runoff/   (form: mydate=YYYY-MM-DD)
        -> <table class="nimh-table"> with one row per gauge:
           № | Река | Местност | Qmin | Qср | Qmax | H | Q | ΔH

``№`` is the station id, ``Река``/``Местност`` the river/place, and ``Q`` the
discharge (m³/s) for that date. Numbers use a Bulgarian decimal comma.

Stations are discovered from a recent day's table; observations iterate the
requested window day-by-day (NIMH publishes one value per gauge per day).
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

_BASE_URL = "https://info.meteo.bg"
_RUNOFF_PATH = "/openData/river-runoff/"
# One <tr> of 9 <td> cells: id, river, place, Qmin, Qmean, Qmax, H, Q, dH.
_ROW_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.DOTALL)
_CELL_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.DOTALL)
_TAG_RE = re.compile(r"<[^>]+>")
# Cap how many days a single observation request will fetch (one POST/day).
_MAX_DAYS = 366


def _num(text: str) -> float | None:
    """Parse a Bulgarian decimal-comma number; return None if not numeric."""
    t = text.strip().replace("\xa0", "").replace(" ", "")
    if not t or t in ("-", "—"):
        return None
    try:
        return float(t.replace(",", "."))
    except ValueError:
        return None


@register("bulgaria_nimh")
class BulgariaNimhConnector(BaseConnector):
    """Bulgaria NIMH daily river-runoff (per-day HTML table)."""

    slug = "bulgaria_nimh"
    display_name = "NIMH Bulgaria (open data)"
    base_url = _BASE_URL
    country_codes = ["BG"]
    max_concurrent_requests = 4

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        # date string -> {station_id: {"river", "place", "q"}}
        self._day_cache: dict[str, dict[str, dict]] = {}

    async def _fetch_day(self, day: str) -> dict[str, dict]:
        """POST for one day and parse its runoff table into {id: row}."""
        if day in self._day_cache:
            return self._day_cache[day]
        try:
            if self._request_sem is not None:
                async with self._request_sem:
                    resp = await self.client.post(_RUNOFF_PATH, data={"mydate": day})
            else:
                resp = await self.client.post(_RUNOFF_PATH, data={"mydate": day})
        except httpx.HTTPError as exc:
            raise ConnectorError(self.slug, f"NIMH request failed for {day}: {exc}") from exc
        rows: dict[str, dict] = {}
        for row_html in _ROW_RE.findall(resp.text):
            cells = [_TAG_RE.sub("", c).strip() for c in _CELL_RE.findall(row_html)]
            if len(cells) < 8 or not cells[0].isdigit():
                continue  # header / malformed row
            rows[cells[0]] = {
                "river": cells[1],
                "place": cells[2],
                "q": _num(cells[7]),  # the "Q" (discharge) column
            }
        self._day_cache[day] = rows
        return rows

    async def fetch_stations(self) -> list[Station]:
        """Discover gauges from a recent day's runoff table."""
        # Yesterday is reliably published; fall back a few days if empty.
        rows: dict[str, dict] = {}
        probe = datetime.now(UTC).date()
        for back in range(1, 5):
            rows = await self._fetch_day((probe - timedelta(days=back)).isoformat())
            if rows:
                break
        stations: list[Station] = []
        for native_id, row in rows.items():
            place = row["place"] or native_id
            river = row["river"] or None
            stations.append(Station(
                id=self._station_id(native_id),
                provider=self.slug,
                native_id=native_id,
                name=f"{river} ({place})" if river else place,
                latitude=0.0,  # NIMH runoff table carries no coordinates
                longitude=0.0,
                country_code="BG",
                river=river,
                is_active=True,
            ))
        logger.info("stations_fetched", provider=self.slug, count=len(stations))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch daily discharge for a station by iterating the date window."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        start_d = start.date() if isinstance(start, datetime) else start
        end_d = end.date() if isinstance(end, datetime) else end

        observations: list[Observation] = []
        day = start_d
        fetched = 0
        while day <= end_d and fetched < _MAX_DAYS:
            fetched += 1
            try:
                rows = await self._fetch_day(day.isoformat())
            except ConnectorError as exc:
                logger.warning("nimh_day_failed", provider=self.slug,
                               day=day.isoformat(), error=str(exc)[:120])
                day += timedelta(days=1)
                continue
            row = rows.get(native_id)
            if row is not None:
                discharge = row["q"]
                observations.append(Observation(
                    station_id=station_id,
                    timestamp=datetime(day.year, day.month, day.day, tzinfo=UTC),
                    discharge_m3s=discharge,
                    quality=QualityFlag.RAW if discharge is not None else QualityFlag.MISSING,
                ))
            day += timedelta(days=1)

        if day <= end_d:
            logger.info("nimh_window_truncated", provider=self.slug,
                        max_days=_MAX_DAYS, station=native_id)

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the last 7 days of daily discharge."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id, start=now - timedelta(days=7), end=now,
        )
