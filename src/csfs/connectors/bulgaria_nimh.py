# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Bulgaria NIMH connector — National Institute of Meteorology and Hydrology.

Bulgaria's NIMH publishes daily water runoff data for ~63 stations via
its open-data portal at https://info.meteo.bg/openData.  Data files
are served as plain-text or CSV downloads from the directory listing.

This connector supports two modes:

1. **API/directory discovery** — tries to fetch a station catalogue
   from the openData endpoint (JSON or HTML directory listing).

2. **Seed list fallback** — a curated list of ~15 major Bulgarian
   river gauging stations (Danube tributaries, Maritsa, Iskar, Struma,
   Mesta, etc.) is returned when the live endpoint is unavailable.

Endpoints used
--------------
* Station listing:
  GET /openData/ — may return JSON array or HTML directory listing.

* Observations:
  GET /openData/{station_file} — daily runoff data in CSV/text.

The connector is built defensively — it tries JSON first, falls back
to HTML parsing, and ultimately to the seed list.
"""

from __future__ import annotations

import re
from datetime import UTC, datetime, timedelta

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

# ---------------------------------------------------------------------------
# Curated seed stations — major Bulgarian river gauging points.
# Format: (native_id, name, lat, lon, river, area_km2 | None)
# ---------------------------------------------------------------------------
_SEED_STATIONS: list[tuple[str, str, float, float, str, float | None]] = [
    ("6842200", "Novo Selo", 44.16, 22.82, "Danube", None),
    ("6842800", "Ruse", 43.86, 25.95, "Danube", None),
    ("6842300", "Kunino", 43.18, 24.00, "Iskar", None),
    ("6865100", "Plovdiv", 42.15, 24.75, "Maritsa", None),
    ("6864100", "Krupnik", 41.85, 23.12, "Struma", None),
]


@register("bulgaria_nimh")
class BulgariaNimhConnector(BaseConnector):
    """Connector for Bulgaria's NIMH open hydrological data.

    Configuration options (via ``config`` dict):
        data_dir : str
            Local directory with downloaded NIMH CSV files.
    """

    slug = "bulgaria_nimh"
    display_name = "NIMH Bulgaria (open data)"
    base_url = "https://info.meteo.bg"
    country_codes: list[str] = ["BG"]

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return station catalogue from NIMH or seed list fallback."""
        # Try JSON endpoint first
        stations = await self._try_fetch_stations_json()
        if stations:
            logger.info(
                "stations_fetched",
                provider=self.slug,
                count=len(stations),
                source="api_json",
            )
            return stations

        # Try HTML directory listing
        stations = await self._try_fetch_stations_html()
        if stations:
            logger.info(
                "stations_fetched",
                provider=self.slug,
                count=len(stations),
                source="api_html",
            )
            return stations

        # Fall back to seed list
        stations = self._build_seed_stations()
        logger.info(
            "stations_fetched",
            provider=self.slug,
            count=len(stations),
            source="seed",
        )
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch daily runoff observations for a station.

        Tries the NIMH openData file endpoint first, then returns an
        empty chunk if unavailable.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        chunk = await self._try_fetch_opendata(
            native_id, station_id, start, end,
        )
        if chunk is not None:
            return chunk

        logger.info(
            "nimh_no_data",
            station=native_id,
            hint=(
                "NIMH openData endpoint may be unavailable. "
                "Check https://info.meteo.bg/openData for file "
                "availability."
            ),
        )
        return self._empty_chunk(station_id)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent observations (last 7 days)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(days=7),
            end=now,
        )

    # -----------------------------------------------------------------
    # Station discovery
    # -----------------------------------------------------------------

    async def _try_fetch_stations_json(
        self,
    ) -> list[Station] | None:
        """Try fetching station list as JSON from openData."""
        try:
            resp = await self._get(
                "/openData/",
                params={"format": "json"},
            )
            data = resp.json()
            if not isinstance(data, list):
                return None
            return self._parse_stations_json(data)
        except (
            ConnectorError,
            httpx.HTTPStatusError,
            ValueError,
        ):
            return None

    def _parse_stations_json(
        self, data: list[dict],
    ) -> list[Station]:
        """Parse a JSON station listing from NIMH."""
        stations: list[Station] = []
        for entry in data:
            native_id = str(entry.get("id", entry.get("code", "")))
            if not native_id:
                continue
            name = entry.get("name", entry.get("station", ""))
            lat = entry.get("lat", entry.get("latitude"))
            lon = entry.get("lon", entry.get("longitude"))
            if lat is None or lon is None:
                continue
            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=str(name),
                    latitude=float(str(lat)),
                    longitude=float(str(lon)),
                    country_code="BG",
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

    async def _try_fetch_stations_html(
        self,
    ) -> list[Station] | None:
        """Try fetching station list from HTML directory listing."""
        try:
            resp = await self._get("/openData/")
            html = resp.text
            if not html or "<" not in html:
                return None
            return self._parse_directory_html(html)
        except (ConnectorError, httpx.HTTPStatusError):
            return None

    def _parse_directory_html(
        self, html: str,
    ) -> list[Station] | None:
        """Extract station file names from an HTML directory listing.

        Looks for ``<a href="...">`` patterns pointing to CSV or
        text files with station-like names.
        """
        pattern = re.compile(
            r'href="([^"]+\.(?:csv|txt|dat))"',
            re.IGNORECASE,
        )
        matches = pattern.findall(html)
        if not matches:
            return None

        stations: list[Station] = []
        for filename in matches:
            native_id = re.sub(
                r"\.(csv|txt|dat)$", "", filename, flags=re.IGNORECASE,
            )
            if not native_id:
                continue
            stations.append(Station(
                id=self._station_id(native_id),
                provider=self.slug,
                native_id=native_id,
                name=native_id.replace("_", " ").title(),
                latitude=0.0,
                longitude=0.0,
                country_code="BG",
            ))
        return stations if stations else None

    def _build_seed_stations(self) -> list[Station]:
        """Build Station objects from the curated seed list."""
        stations: list[Station] = []
        for native_id, name, lat, lon, river, area in _SEED_STATIONS:
            stations.append(Station(
                id=self._station_id(native_id),
                provider=self.slug,
                native_id=native_id,
                name=name,
                latitude=float(str(lat)),
                longitude=float(str(lon)),
                country_code="BG",
                river=river,
                catchment_area_km2=(
                    float(str(area)) if area is not None else None
                ),
            ))
        return stations

    # -----------------------------------------------------------------
    # Observation fetching
    # -----------------------------------------------------------------

    async def _try_fetch_opendata(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk | None:
        """Try fetching observations from NIMH openData file."""
        # Try common file naming patterns
        filenames = [
            f"{native_id}.csv",
            f"{native_id}.txt",
            f"{native_id}.dat",
        ]
        for filename in filenames:
            try:
                resp = await self._get(f"/openData/{filename}")
                return self._parse_runoff_text(
                    resp.text, station_id, start, end,
                )
            except (ConnectorError, httpx.HTTPStatusError):
                continue
            except Exception as exc:
                logger.warning(
                    "nimh_parse_error",
                    provider=self.slug,
                    station=native_id,
                    filename=filename,
                    error=str(exc),
                )
                continue
        return None

    def _parse_runoff_text(
        self,
        text: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Parse daily runoff data from NIMH text/CSV file.

        Expected formats::

            2024-06-01,55.3
            2024-06-01;55.3
            01.06.2024  55.3

        Lines starting with ``#`` or lacking numeric data are skipped.
        """
        start_aware = (
            start if start.tzinfo else start.replace(tzinfo=UTC)
        )
        end_aware = (
            end if end.tzinfo else end.replace(tzinfo=UTC)
        )

        observations: list[Observation] = []
        for line in text.splitlines():
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue

            obs = self._parse_runoff_line(
                stripped, station_id, start_aware, end_aware,
            )
            if obs is not None:
                observations.append(obs)

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    def _parse_runoff_line(
        self,
        line: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> Observation | None:
        """Parse a single line of runoff data."""
        # Split on comma, semicolon, or whitespace
        parts = re.split(r"[,;\t\s]+", line, maxsplit=1)
        if len(parts) < 2:
            return None

        ts = self._parse_date(parts[0].strip())
        if ts is None:
            return None

        if ts < start or ts > end:
            return None

        val_str = parts[1].strip()
        discharge: float | None = None
        quality = QualityFlag.MISSING
        if val_str and val_str.lower() not in ("", "na", "nan", "-"):
            try:
                discharge = float(str(val_str))
                quality = QualityFlag.RAW
            except ValueError:
                pass

        return Observation(
            station_id=station_id,
            timestamp=ts,
            discharge_m3s=discharge,
            quality=quality,
        )

    @staticmethod
    def _parse_date(date_str: str) -> datetime | None:
        """Try multiple date formats common in Bulgarian datasets."""
        for fmt in (
            "%Y-%m-%d",
            "%d.%m.%Y",
            "%d/%m/%Y",
            "%Y/%m/%d",
        ):
            try:
                return datetime.strptime(
                    date_str, fmt,
                ).replace(tzinfo=UTC)
            except ValueError:
                continue
        return None

    # -----------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------

    def _empty_chunk(self, station_id: str) -> TimeSeriesChunk:
        """Return an empty TimeSeriesChunk for a station."""
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )
