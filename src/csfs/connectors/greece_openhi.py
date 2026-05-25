"""Greece OpenHI connector — Open Hydrosystem Information Network.

OpenHI (https://system.openhi.net) provides open hydrological data for Greece,
including discharge time series from gauging stations.

Endpoints used
--------------
* Station listing (paginated):
  GET https://system.openhi.net/api/stations/
  Returns paginated JSON: ``{"count", "next", "previous", "results": [...]}``.
  Each station has ``id``, ``name``, ``point`` (GeoJSON), etc.

* Observations:
  GET https://system.openhi.net/api/stations/{id}/data/
  Returns time-series records for a station.  Falls back to
  ``/api/ts_records/?station_id={id}`` if the first endpoint fails.
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
    base_url = "https://system.openhi.net"
    country_codes = ["GR"]

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all discharge gauging stations from OpenHI.

        The ``/api/stations/`` endpoint is paginated (DRF style).
        We follow ``next`` links until all pages are consumed.
        """
        stations: list[Station] = []
        page = 1

        while True:
            try:
                resp = await self._get(
                    "/api/stations/",
                    params={"page": page},
                )
            except httpx.HTTPStatusError as exc:
                raise ConnectorError(
                    self.slug,
                    f"Failed to fetch station list: "
                    f"HTTP {exc.response.status_code}",
                ) from exc

            data = resp.json()
            items = data.get("results", [])
            if not items:
                break

            stations.extend(self._parse_stations(items))

            if data.get("next") is None:
                break
            page += 1

        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for *station_id* over [start, end].

        Tries ``/api/stations/{id}/data/`` first; falls back to
        ``/api/ts_records/?station_id={id}`` on 404.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        params: dict[str, str] = {
            "start_date": start.isoformat(),
            "end_date": end.isoformat(),
        }

        try:
            resp = await self._get(
                f"/api/stations/{native_id}/data/",
                params=params,
            )
        except httpx.HTTPStatusError as exc:
            if exc.response.status_code == 404:
                resp = await self._try_ts_records_fallback(
                    native_id, params, station_id,
                )
            else:
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

    async def _try_ts_records_fallback(
        self,
        native_id: str,
        params: dict[str, str],
        station_id: str,
    ) -> httpx.Response:
        """Fallback to ``/api/ts_records/`` if per-station data endpoint 404s."""
        try:
            return await self._get(
                "/api/ts_records/",
                params={"station_id": native_id, **params},
            )
        except httpx.HTTPStatusError as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch observations for {native_id} "
                f"(fallback): HTTP {exc.response.status_code}",
            ) from exc

    def _parse_stations(self, items: list[dict]) -> list[Station]:
        """Parse a page of station results into ``Station`` models.

        Each station is expected to carry coordinates in a ``point``
        field (GeoJSON-style ``{"type": "Point", "coordinates": [lon, lat]}``)
        or as top-level ``latitude``/``longitude`` keys.
        """
        stations: list[Station] = []
        for entry in items:
            native_id = str(entry.get("id", "")).strip()
            if not native_id:
                continue

            lat, lon = self._extract_coords(entry)
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

    @staticmethod
    def _extract_coords(entry: dict) -> tuple[float | None, float | None]:
        """Extract (latitude, longitude) from a station dict.

        Supports GeoJSON ``point``, WKT ``geom``, and flat keys.
        """
        point = entry.get("point")
        if isinstance(point, dict):
            coords = point.get("coordinates")
            if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                return float(coords[1]), float(coords[0])

        geom = entry.get("geom")
        if isinstance(geom, str) and "POINT" in geom:
            import re
            match = re.search(r"POINT\s*\(\s*([\d.+-]+)\s+([\d.+-]+)\s*\)", geom)
            if match:
                return float(match.group(2)), float(match.group(1))

        lat = entry.get("latitude") or entry.get("lat")
        lon = entry.get("longitude") or entry.get("lon")
        if lat is not None and lon is not None:
            return float(lat), float(lon)

        return None, None

    def _parse_observations(
        self,
        data: dict | list,
        station_id: str,
    ) -> TimeSeriesChunk:
        """Parse the OpenHI observations response into a ``TimeSeriesChunk``.

        Handles both a bare list and a DRF-paginated dict with a
        ``results`` or ``data`` key.
        """
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("results", data.get("data", []))
        else:
            raise DataFormatError(
                self.slug,
                f"Unexpected response type: {type(data).__name__}",
            )

        observations: list[Observation] = []
        for entry in items:
            ts_raw = entry.get("timestamp") or entry.get("date")
            if ts_raw is None:
                raise DataFormatError(
                    self.slug,
                    "Missing timestamp/date in observation record",
                )
            try:
                ts = datetime.fromisoformat(str(ts_raw))
            except ValueError as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid timestamp: {ts_raw!r}",
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
