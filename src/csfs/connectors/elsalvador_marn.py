"""El Salvador MARN connector — SNET / AQUARIUS Time-Series.

El Salvador's Servicio Nacional de Estudios Territoriales (SNET), part of
the Ministerio de Medio Ambiente y Recursos Naturales (MARN), operates
100+ hydrological stations.  The system runs on AQUARIUS Time-Series, a
modern data management platform.

Endpoints attempted
-------------------
* Station listing:
  GET /Hidrometria/VisorDatosHidrologicos.aspx  (HTML viewer)
  GET /api/stations?format=json                 (hypothetical JSON API)
  GET /AQUARIUS/Publish/v2/GetLocationDescriptionList
      (standard AQUARIUS Publish endpoint)

  Fallback: a seed list of ~15 major stations covering the Lempa River
  system and other principal basins.

* Observations:
  GET /AQUARIUS/Publish/v2/GetTimeSeriesData
      ?TimeSeriesUniqueId={id}&From={ISO}&To={ISO}

  Fallback: returns an empty chunk with guidance.

Both endpoints may be unavailable or change; the connector is written
defensively.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import (
    Observation,
    QualityFlag,
    Station,
    TimeSeriesChunk,
)
from csfs.core.registry import register

logger = structlog.get_logger()

# ------------------------------------------------------------------
# Seed stations — major gauging points in El Salvador
# ------------------------------------------------------------------
_SEED_STATIONS: list[dict[str, object]] = [
    {
        "id": "SV-001", "name": "La Ceiba (Lempa)",
        "lat": 14.3167, "lon": -89.1333, "river": "Lempa",
    },
    {
        "id": "SV-002", "name": "El Guayabo (Lempa)",
        "lat": 13.8500, "lon": -88.9167, "river": "Lempa",
    },
    {
        "id": "SV-003", "name": "San Marcos (Lempa)",
        "lat": 13.8833, "lon": -89.1833, "river": "Lempa",
    },
    {
        "id": "SV-004", "name": "Paso del Oso (Lempa)",
        "lat": 13.8000, "lon": -88.7833, "river": "Lempa",
    },
    {
        "id": "SV-005", "name": "San Lorenzo (Lempa)",
        "lat": 13.9667, "lon": -88.9333, "river": "Lempa",
    },
    {
        "id": "SV-006", "name": "Puente Cuscatlan (Lempa)",
        "lat": 13.7333, "lon": -88.7333, "river": "Lempa",
    },
    {
        "id": "SV-007", "name": "Las Pavas (Jiboa)",
        "lat": 13.4833, "lon": -88.9333, "river": "Jiboa",
    },
    {
        "id": "SV-008", "name": "Desembocadura (Jiboa)",
        "lat": 13.4167, "lon": -88.9500, "river": "Jiboa",
    },
    {
        "id": "SV-009", "name": "La Hachadura (Paz)",
        "lat": 13.8500, "lon": -90.1000, "river": "Paz",
    },
    {
        "id": "SV-010", "name": "Puente Goascoran",
        "lat": 13.5000, "lon": -87.8000, "river": "Goascorán",
    },
    {
        "id": "SV-011", "name": "San Miguel (Grande de San Miguel)",
        "lat": 13.4667, "lon": -88.1833,
        "river": "Grande de San Miguel",
    },
    {
        "id": "SV-012", "name": "Vado Marin (Grande de San Miguel)",
        "lat": 13.3500, "lon": -88.1167,
        "river": "Grande de San Miguel",
    },
    {
        "id": "SV-013", "name": "Puente Sucio (Sucio)",
        "lat": 13.7000, "lon": -89.2500, "river": "Sucio",
    },
    {
        "id": "SV-014", "name": "San José Ingenio (Acelhuate)",
        "lat": 13.7667, "lon": -89.2000, "river": "Acelhuate",
    },
    {
        "id": "SV-015", "name": "Puente Acahuapa",
        "lat": 13.6333, "lon": -88.5667, "river": "Acahuapa",
    },
]

# Candidate AQUARIUS Publish base paths to probe
_AQUARIUS_PATHS = (
    "/AQUARIUS/Publish/v2",
    "/aquarius/Publish/v2",
    "/AQPublish/v2",
)


def _quality_from_grade(
    grade_code: int | str | None,
) -> QualityFlag:
    """Map an AQUARIUS numeric grade code to a CSFS quality flag.

    Common AQUARIUS grades: -1=unknown, 0=ungraded, 1..100=validated,
    150+=estimated.  We simplify conservatively.
    """
    if grade_code is None:
        return QualityFlag.RAW
    try:
        code = int(grade_code)
    except (ValueError, TypeError):
        return QualityFlag.RAW
    if code >= 150:
        return QualityFlag.ESTIMATED
    if code >= 1:
        return QualityFlag.GOOD
    return QualityFlag.RAW


@register("elsalvador_marn")
class ElSalvadorMARNConnector(BaseConnector):
    """Connector for El Salvador's MARN / SNET hydrological data."""

    slug = "elsalvador_marn"
    display_name = "MARN / SNET (El Salvador)"
    base_url = "https://www.snet.gob.sv"
    country_codes = ["SV"]

    # Cache the discovered AQUARIUS base path for the session
    _aquarius_base: str | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return available stations from MARN.

        Attempts the live AQUARIUS endpoint first; on failure, returns
        the built-in seed list so downstream workflows can proceed.
        """
        stations = await self._try_live_stations()
        if stations:
            return stations

        logger.info(
            "using_seed_stations",
            provider=self.slug,
            reason="live API unavailable or returned no data",
        )
        return self._build_seed_stations()

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for *station_id*.

        Probes AQUARIUS Publish v2 endpoints; returns an empty chunk
        with a log message when no live data source responds.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        chunk = await self._try_aquarius_data(
            native_id, station_id, start, end,
        )
        if chunk is not None:
            return chunk

        logger.info(
            "observations_unavailable",
            provider=self.slug,
            station=native_id,
            hint=(
                "AQUARIUS endpoints not reachable. Check "
                "https://www.snet.gob.sv for current API paths."
            ),
        )
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent 24 h of observations."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # ------------------------------------------------------------------
    # Internal: live station listing
    # ------------------------------------------------------------------

    async def _try_live_stations(self) -> list[Station]:
        """Probe known endpoints for a station list."""
        # Try JSON API
        for path in ("/api/stations", "/api/stations?format=json"):
            try:
                resp = await self._get(path)
                data = resp.json()
                parsed = self._parse_station_json(data)
                if parsed:
                    return parsed
            except (httpx.HTTPStatusError, ConnectorError, Exception):
                continue

        # Try AQUARIUS location list
        aquarius_base = await self._resolve_aquarius_base()
        if aquarius_base:
            try:
                resp = await self._get(
                    f"{aquarius_base}/GetLocationDescriptionList",
                )
                data = resp.json()
                parsed = self._parse_aquarius_locations(data)
                if parsed:
                    return parsed
            except (httpx.HTTPStatusError, ConnectorError, Exception):
                pass

        return []

    async def _resolve_aquarius_base(self) -> str | None:
        """Find a working AQUARIUS Publish path."""
        if self._aquarius_base is not None:
            return self._aquarius_base

        for candidate in _AQUARIUS_PATHS:
            try:
                resp = await self.client.get(
                    f"{candidate}/GetLocationDescriptionList",
                    timeout=10.0,
                )
                if resp.status_code == 200:
                    self._aquarius_base = candidate
                    return candidate
            except (httpx.HTTPError, Exception):
                continue

        return None

    # ------------------------------------------------------------------
    # Internal: observation retrieval
    # ------------------------------------------------------------------

    async def _try_aquarius_data(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk | None:
        """Attempt to fetch time-series data from AQUARIUS Publish."""
        aquarius_base = await self._resolve_aquarius_base()
        if not aquarius_base:
            return None

        params: dict[str, str] = {
            "TimeSeriesUniqueId": native_id,
            "From": start.isoformat(),
            "To": end.isoformat(),
        }
        try:
            resp = await self._get(
                f"{aquarius_base}/GetTimeSeriesData",
                params=params,
            )
            return self._parse_aquarius_ts(resp.json(), station_id)
        except (httpx.HTTPStatusError, ConnectorError, Exception):
            return None

    # ------------------------------------------------------------------
    # Parsers
    # ------------------------------------------------------------------

    def _parse_station_json(
        self, data: list[dict] | dict,
    ) -> list[Station]:
        """Parse a generic JSON station list."""
        items: list[dict] = (
            data if isinstance(data, list)
            else data.get("stations", data.get("estaciones", []))
        )
        stations: list[Station] = []
        for entry in items:
            native_id = str(
                entry.get("id", entry.get("codigo", ""))
            ).strip()
            if not native_id:
                continue

            lat = entry.get("latitude") or entry.get("lat")
            lon = entry.get("longitude") or entry.get("lon")
            if lat is None or lon is None:
                continue

            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=entry.get("name", native_id),
                    latitude=float(str(lat)),
                    longitude=float(str(lon)),
                    country_code="SV",
                    river=entry.get("river"),
                ))
            except (ValueError, KeyError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    station=native_id,
                    error=str(exc),
                )
        return stations

    def _parse_aquarius_locations(
        self, data: dict,
    ) -> list[Station]:
        """Parse AQUARIUS ``GetLocationDescriptionList`` response."""
        items = data.get(
            "LocationDescriptions",
            data.get("locations", []),
        )
        stations: list[Station] = []
        for entry in items:
            native_id = str(
                entry.get("Identifier", entry.get("id", ""))
            ).strip()
            if not native_id:
                continue

            lat = entry.get("Latitude") or entry.get("latitude")
            lon = entry.get("Longitude") or entry.get("longitude")
            if lat is None or lon is None:
                continue

            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=entry.get("Name", native_id),
                    latitude=float(str(lat)),
                    longitude=float(str(lon)),
                    country_code="SV",
                    river=entry.get("River"),
                ))
            except (ValueError, KeyError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    station=native_id,
                    error=str(exc),
                )
        return stations

    def _parse_aquarius_ts(
        self,
        data: dict,
        station_id: str,
    ) -> TimeSeriesChunk:
        """Parse AQUARIUS ``GetTimeSeriesData`` response."""
        points = data.get("Points", data.get("points", []))
        observations: list[Observation] = []

        for pt in points:
            ts_raw = pt.get("Timestamp") or pt.get("timestamp")
            if not ts_raw:
                continue
            try:
                ts = datetime.fromisoformat(str(ts_raw))
            except (ValueError, TypeError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid timestamp in AQUARIUS response: {exc}",
                ) from exc

            value_obj = pt.get("Value", pt.get("value", {}))
            if isinstance(value_obj, dict):
                raw_val = value_obj.get(
                    "Numeric", value_obj.get("numeric"),
                )
                grade = value_obj.get(
                    "GradeCode", value_obj.get("grade"),
                )
            else:
                raw_val = value_obj
                grade = None

            discharge = (
                float(str(raw_val))
                if raw_val is not None
                else None
            )
            quality = (
                QualityFlag.MISSING
                if discharge is None
                else _quality_from_grade(grade)
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

    # ------------------------------------------------------------------
    # Seed list
    # ------------------------------------------------------------------

    def _build_seed_stations(self) -> list[Station]:
        """Return curated seed stations for major Salvadoran rivers."""
        stations: list[Station] = []
        for s in _SEED_STATIONS:
            stations.append(Station(
                id=self._station_id(str(s["id"])),
                provider=self.slug,
                native_id=str(s["id"]),
                name=str(s["name"]),
                latitude=float(str(s["lat"])),
                longitude=float(str(s["lon"])),
                country_code="SV",
                river=str(s.get("river", "")),
            ))
        return stations
