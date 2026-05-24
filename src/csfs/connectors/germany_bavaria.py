"""GKD Bayern connector — Bavarian State Office for the Environment.

The Gewässerkundlicher Dienst (GKD) Bayern provides hydrological data for
Bavaria via https://www.gkd.bayern.de.  The system frequently returns HTML
or CSV instead of JSON, so this connector is built very defensively.
"""

from __future__ import annotations

import json
from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()


@register("germany_bavaria")
class GermanyBavariaConnector(BaseConnector):
    """Connector for the Bavarian GKD discharge monitoring network."""

    slug = "germany_bavaria"
    display_name = "GKD Bayern"
    base_url = "https://www.gkd.bayern.de"
    country_codes = ["DE"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all discharge stations from GKD Bayern.

        Tries JSON endpoint first, falls back to HTML table scraping.
        """
        stations = await self._try_json_stations()
        if stations is not None:
            return stations

        stations = await self._try_html_stations()
        if stations is not None:
            return stations

        logger.warning(
            "fetch_stations_failed_all_endpoints",
            provider=self.slug,
        )
        return []

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for a station."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        chunk = await self._try_json_observations(
            native_id, station_id, start, end,
        )
        if chunk is not None:
            return chunk

        logger.warning(
            "fetch_observations_failed",
            provider=self.slug,
            station=native_id,
        )
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent 24 hours of observations."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # ------------------------------------------------------------------
    # Station fetching internals
    # ------------------------------------------------------------------

    async def _try_json_stations(self) -> list[Station] | None:
        """Attempt to fetch stations from the JSON endpoint."""
        try:
            resp = await self._get(
                "/gkd/abfluss/stations.json",
            )
            data = self._safe_json(resp)
            if data is None:
                return None
            if not isinstance(data, list):
                if isinstance(data, dict):
                    data = (
                        data.get("stations")
                        or data.get("data")
                        or data.get("features", [])
                    )
                if not isinstance(data, list):
                    return None
            return self._parse_stations(data)
        except (
            ConnectorError,
            httpx.HTTPStatusError,
            httpx.HTTPError,
        ) as exc:
            logger.warning(
                "stations_json_failed",
                provider=self.slug,
                error=str(exc),
            )
            return None

    async def _try_html_stations(self) -> list[Station] | None:
        """Attempt to scrape stations from the HTML table endpoint."""
        try:
            resp = await self._get(
                "/de/fluesse/abfluss/tabellen",
            )
            text = resp.text
            if not text or "<html" not in text.lower():
                return None
            return self._parse_station_html(text)
        except (
            ConnectorError,
            httpx.HTTPStatusError,
            httpx.HTTPError,
        ) as exc:
            logger.warning(
                "stations_html_failed",
                provider=self.slug,
                error=str(exc),
            )
            return None

    # ------------------------------------------------------------------
    # Observation fetching internals
    # ------------------------------------------------------------------

    async def _try_json_observations(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk | None:
        """Attempt to fetch observations from JSON endpoint."""
        try:
            resp = await self._get(
                f"/gkd/abfluss/{native_id}/values.json",
                params={
                    "start": start.strftime("%Y-%m-%d"),
                    "end": end.strftime("%Y-%m-%d"),
                },
            )
            data = self._safe_json(resp)
            if data is None:
                return None
            return self._parse_observations(data, station_id)
        except (
            ConnectorError,
            httpx.HTTPStatusError,
            httpx.HTTPError,
        ) as exc:
            logger.warning(
                "observations_json_failed",
                provider=self.slug,
                station=native_id,
                error=str(exc),
            )
            return None

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    def _parse_stations(self, data: list[dict]) -> list[Station]:
        """Parse station entries from JSON array."""
        stations: list[Station] = []
        for entry in data:
            try:
                native_id = str(
                    entry.get("id")
                    or entry.get("station_id")
                    or entry.get("messstelle_nr")
                    or ""
                )
                if not native_id:
                    continue

                name = str(
                    entry.get("name")
                    or entry.get("station_name")
                    or entry.get("messstellenname")
                    or ""
                )
                lat = _safe_float(
                    entry.get("latitude")
                    or entry.get("lat")
                    or entry.get("breite"),
                )
                lon = _safe_float(
                    entry.get("longitude")
                    or entry.get("lon")
                    or entry.get("laenge"),
                )
                river = (
                    entry.get("river")
                    or entry.get("gewaesser")
                    or entry.get("water")
                )

                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=name,
                    latitude=lat if lat is not None else 0.0,
                    longitude=lon if lon is not None else 0.0,
                    country_code="DE",
                    river=river,
                ))
            except (ValueError, KeyError, TypeError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    entry=str(entry)[:200],
                    error=str(exc),
                )
                continue
        return stations

    def _parse_station_html(self, html: str) -> list[Station]:
        """Best-effort parse of HTML table station listing."""
        import re

        stations: list[Station] = []
        try:
            pattern = re.compile(
                r"(\d{5,10})\s*</td>\s*<td[^>]*>\s*([^<]+)",
                re.IGNORECASE,
            )
            for match in pattern.finditer(html):
                native_id = match.group(1)
                name = match.group(2).strip()
                if not name:
                    continue
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=name,
                    latitude=0.0,
                    longitude=0.0,
                    country_code="DE",
                ))
        except Exception:
            logger.debug(
                "station_html_parse_failed",
                provider=self.slug,
            )
        return stations

    def _parse_observations(
        self,
        data: dict | list,
        station_id: str,
    ) -> TimeSeriesChunk:
        """Parse observation data from JSON response."""
        obs_list: list[dict] = []
        if isinstance(data, dict):
            obs_list = (
                data.get("values")
                or data.get("data")
                or data.get("measurements", [])
            )
        elif isinstance(data, list):
            obs_list = data

        if not isinstance(obs_list, list):
            obs_list = []

        observations: list[Observation] = []
        for entry in obs_list:
            try:
                ts = _parse_timestamp(entry)
                if ts is None:
                    continue
                value = entry.get("value") or entry.get("wert")
                discharge = _safe_float(value)
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
            except (ValueError, TypeError):
                continue

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    @staticmethod
    def _safe_json(resp: httpx.Response) -> dict | list | None:
        """Safely parse JSON from response, returning None on failure."""
        text = resp.text.strip()

        if not text:
            return None

        # Check if response looks like HTML rather than JSON
        if text.startswith("<!") or text.startswith("<html"):
            return None

        try:
            result: dict | list = json.loads(text)
            return result
        except (json.JSONDecodeError, ValueError):
            return None


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------


def _safe_float(value: object) -> float | None:
    """Safely convert a value to float."""
    if value is None:
        return None
    try:
        return float(str(value))
    except (ValueError, TypeError):
        return None


def _parse_timestamp(entry: dict) -> datetime | None:
    """Try multiple field names and date formats."""
    raw = (
        entry.get("timestamp")
        or entry.get("datetime")
        or entry.get("date")
        or entry.get("zeit")
    )
    if raw is None:
        return None

    raw_str = str(raw).strip()
    if not raw_str:
        return None

    try:
        return datetime.fromisoformat(raw_str)
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%d.%m.%Y %H:%M", "%d.%m.%Y"):
        try:
            return datetime.strptime(raw_str, fmt)
        except ValueError:
            continue

    return None
