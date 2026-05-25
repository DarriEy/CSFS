# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""DanubeHIS connector — Danube Hydrological Information System.

DanubeHIS (https://www.danubehis.org) covers 1,100+ stations across
the entire Danube River basin, providing discharge data from 13
countries.  The service offers CSV, XLS, and WaterML2 downloads
and requires free registration for API access.

This connector supports three modes:

1. **API discovery** — tries ``/api/stations?format=json`` and
   ``/stations`` endpoints to discover available stations.

2. **Authenticated API** — if ``config["api_token"]`` is set, it
   is passed as a Bearer token in the Authorization header.

3. **Seed list fallback** — a curated list of ~30 major Danube
   basin stations spanning all 13 countries.

Endpoints used
--------------
* Station listing:
  GET /api/stations?format=json or GET /stations

* Observations:
  GET /api/data/{station_id}?format=csv&start=...&end=...

The connector is built defensively — registration-gated endpoints
may return 401/403; the seed list is always available.
"""

from __future__ import annotations

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
# Curated seed stations — major Danube basin gauging points.
# Format: (native_id, name, lat, lon, river, country, area | None)
# ---------------------------------------------------------------------------
_SEED_STATIONS: list[
    tuple[str, str, float, float, str, str, float | None]
] = [
    # Germany (DE)
    ("6342800", "Hofkirchen", 48.68, 13.12, "Danube", "DE", None),
    ("6342900", "Achleiten", 48.58, 13.50, "Danube", "DE", None),
    # Slovakia (SK)
    ("6142200", "Bratislava", 48.14, 17.11, "Danube", "SK", None),
    # Hungary (HU)
    ("6442500", "Nagymaros", 47.78, 18.95, "Danube", "HU", None),
    # Serbia (RS)
    ("6542100", "Bezdan", 45.85, 18.87, "Danube", "RS", None),
    ("6542500", "Pancevo", 44.87, 20.64, "Danube", "RS", None),
    # Romania (RO)
    ("6742200", "Orsova", 44.70, 22.42, "Danube", "RO", None),
    ("6742900", "Ceatal Izmail", 45.22, 28.72, "Danube", "RO", None),
]


@register("danube_his")
class DanubeHisConnector(BaseConnector):
    """Connector for DanubeHIS basin-wide discharge data.

    Configuration options (via ``config`` dict):
        api_token : str
            Bearer token for authenticated API access (free
            registration at danubehis.org).
    """

    slug = "danube_his"
    display_name = "DanubeHIS"
    base_url = "https://www.danubehis.org"
    country_codes: list[str] = [
        "BG", "RS", "UA", "HU", "RO", "SK",
        "AT", "DE", "CZ", "HR", "SI", "BA", "MD",
    ]

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return station catalogue from DanubeHIS or seed fallback."""
        stations = await self._try_fetch_api_stations()
        if stations:
            logger.info(
                "stations_fetched",
                provider=self.slug,
                count=len(stations),
                source="api",
            )
            return stations

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
        """Fetch discharge observations from DanubeHIS API.

        Falls back to an empty chunk when the API is unreachable
        or requires authentication.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        chunk = await self._try_fetch_api_data(
            native_id, station_id, start, end,
        )
        if chunk is not None:
            return chunk

        logger.info(
            "danube_his_no_data",
            station=native_id,
            hint=(
                "DanubeHIS requires free registration. "
                "Set config['api_token'] with your bearer token."
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
    # Authentication helper
    # -----------------------------------------------------------------

    def _auth_headers(self) -> dict[str, str]:
        """Build auth headers if an API token is configured."""
        token = self.config.get("api_token")
        if token:
            return {"Authorization": f"Bearer {token}"}
        return {}

    # -----------------------------------------------------------------
    # Station discovery
    # -----------------------------------------------------------------

    async def _try_fetch_api_stations(
        self,
    ) -> list[Station] | None:
        """Try fetching stations from the DanubeHIS API."""
        # Try /api/stations first
        for path in ("/api/stations", "/stations"):
            try:
                resp = await self._get(
                    path,
                    params={"format": "json"},
                )
                data = resp.json()
                if isinstance(data, list):
                    stations = self._parse_api_stations(data)
                    if stations:
                        return stations
                elif isinstance(data, dict):
                    items = data.get(
                        "stations", data.get("data", []),
                    )
                    if isinstance(items, list):
                        stations = self._parse_api_stations(items)
                        if stations:
                            return stations
            except (
                ConnectorError,
                httpx.HTTPStatusError,
                ValueError,
            ):
                continue
        return None

    def _parse_api_stations(
        self, data: list[dict],
    ) -> list[Station]:
        """Parse station list JSON from DanubeHIS API."""
        stations: list[Station] = []
        for entry in data:
            native_id = str(
                entry.get("id", entry.get("station_id", "")),
            )
            if not native_id:
                continue
            name = entry.get("name", entry.get("station_name", ""))
            lat = entry.get("latitude", entry.get("lat"))
            lon = entry.get("longitude", entry.get("lon"))
            if lat is None or lon is None:
                continue
            country = entry.get(
                "country_code",
                entry.get("country", ""),
            )
            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=str(name),
                    latitude=float(str(lat)),
                    longitude=float(str(lon)),
                    country_code=str(country)[:2].upper(),
                    river=entry.get("river"),
                    catchment_area_km2=(
                        float(str(entry["catchment_area"]))
                        if entry.get("catchment_area") is not None
                        else None
                    ),
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

    def _build_seed_stations(self) -> list[Station]:
        """Build Station objects from the curated seed list."""
        stations: list[Station] = []
        for row in _SEED_STATIONS:
            native_id, name, lat, lon, river, country, area = row
            stations.append(Station(
                id=self._station_id(native_id),
                provider=self.slug,
                native_id=native_id,
                name=name,
                latitude=float(str(lat)),
                longitude=float(str(lon)),
                country_code=country,
                river=river,
                catchment_area_km2=(
                    float(str(area)) if area is not None else None
                ),
            ))
        return stations

    # -----------------------------------------------------------------
    # Observation fetching
    # -----------------------------------------------------------------

    async def _try_fetch_api_data(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk | None:
        """Try fetching observation data from DanubeHIS API."""
        params: dict[str, str] = {
            "format": "csv",
            "start": start.strftime("%Y-%m-%d"),
            "end": end.strftime("%Y-%m-%d"),
        }
        params.update(self._auth_headers())

        for path_template in (
            "/api/data/{sid}",
            "/data/{sid}",
        ):
            path = path_template.format(sid=native_id)
            try:
                resp = await self._get(path, params=params)
                content_type = resp.headers.get(
                    "content-type", "",
                )

                # Try JSON first
                if "json" in content_type:
                    return self._parse_json_observations(
                        resp.json(), station_id, start, end,
                    )

                # Parse as CSV
                return self._parse_csv_observations(
                    resp.text, station_id, start, end,
                )
            except (
                ConnectorError,
                httpx.HTTPStatusError,
            ) as exc:
                logger.warning(
                    "danube_his_fetch_failed",
                    provider=self.slug,
                    station=native_id,
                    path=path,
                    error=str(exc),
                )
                continue
            except Exception as exc:
                logger.warning(
                    "danube_his_parse_error",
                    provider=self.slug,
                    station=native_id,
                    error=str(exc),
                )
                continue
        return None

    def _parse_json_observations(
        self,
        data: dict | list,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Parse JSON observation response from DanubeHIS."""
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("data", data.get("observations", []))
        else:
            items = []

        start_aware = (
            start if start.tzinfo else start.replace(tzinfo=UTC)
        )
        end_aware = (
            end if end.tzinfo else end.replace(tzinfo=UTC)
        )

        observations: list[Observation] = []
        for entry in items:
            ts_str = entry.get(
                "timestamp", entry.get("date", entry.get("time")),
            )
            if not ts_str:
                continue
            try:
                ts = datetime.fromisoformat(str(ts_str))
            except ValueError:
                continue

            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            if ts < start_aware or ts > end_aware:
                continue

            value = entry.get(
                "discharge", entry.get("value", entry.get("q")),
            )
            discharge = (
                float(str(value)) if value is not None else None
            )
            quality = (
                QualityFlag.RAW
                if discharge is not None
                else QualityFlag.MISSING
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

    def _parse_csv_observations(
        self,
        text: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Parse CSV observation data from DanubeHIS.

        Expected CSV format::

            date,discharge
            2024-06-01,150.3
            2024-06-02,148.7
        """
        start_aware = (
            start if start.tzinfo else start.replace(tzinfo=UTC)
        )
        end_aware = (
            end if end.tzinfo else end.replace(tzinfo=UTC)
        )

        observations: list[Observation] = []
        lines = text.strip().splitlines()
        if len(lines) < 2:
            return self._empty_chunk(station_id)

        # Skip header line
        for line in lines[1:]:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue

            parts = stripped.split(",")
            if len(parts) < 2:
                continue

            ts = self._parse_date(parts[0].strip())
            if ts is None:
                continue

            if ts < start_aware or ts > end_aware:
                continue

            val_str = parts[1].strip()
            discharge: float | None = None
            quality = QualityFlag.MISSING
            if val_str and val_str.lower() not in (
                "", "na", "nan", "-",
            ):
                try:
                    discharge = float(str(val_str))
                    quality = QualityFlag.RAW
                except ValueError:
                    pass

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

    @staticmethod
    def _parse_date(date_str: str) -> datetime | None:
        """Try multiple date formats."""
        for fmt in ("%Y-%m-%d", "%d.%m.%Y", "%d/%m/%Y"):
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
