"""South Africa Department of Water and Sanitation (DWS) connector.

DWS provides hydrological data through a web interface at
https://www.dws.gov.za/Hydrology/Verified. Their API is poorly documented
and responses can be unreliable, so this connector is intentionally
defensive: it logs warnings and returns empty results on failure rather
than crashing.

A seed list of major river gauging stations is maintained for
``fetch_stations`` and can be expanded via config or by parsing the DWS
station catalog when it is reachable.
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

# ── Seed station catalog ───────────────────────────────────────────────
# Major DWS river gauging stations with known metadata.  The connector
# will attempt to fetch a live catalog, falling back to this seed list.
_SEED_STATIONS: list[dict] = [
    {
        "native_id": "A2H012",
        "name": "Hartbeespoort Dam",
        "latitude": -25.748,
        "longitude": 27.879,
        "river": "Crocodile",
    },
    {
        "native_id": "C2H007",
        "name": "Vaal River at Orkney",
        "latitude": -26.988,
        "longitude": 26.667,
        "river": "Vaal",
    },
    {
        "native_id": "X2H016",
        "name": "Komati River at Tonga",
        "latitude": -25.963,
        "longitude": 31.877,
        "river": "Komati",
    },
    {
        "native_id": "D1H009",
        "name": "Orange River at Aliwal North",
        "latitude": -30.694,
        "longitude": 26.710,
        "river": "Orange",
    },
    {
        "native_id": "W5H005",
        "name": "Tugela River at Mandini",
        "latitude": -29.148,
        "longitude": 31.392,
        "river": "Tugela",
    },
    {
        "native_id": "G1H008",
        "name": "Berg River at Paarl",
        "latitude": -33.729,
        "longitude": 18.970,
        "river": "Berg",
    },
    {
        "native_id": "T3H006",
        "name": "Olifants River at Loskop North",
        "latitude": -25.413,
        "longitude": 29.358,
        "river": "Olifants",
    },
    {
        "native_id": "A6H011",
        "name": "Limpopo River at Beit Bridge",
        "latitude": -22.217,
        "longitude": 29.983,
        "river": "Limpopo",
    },
]


@register("south_africa_dws")
class SouthAfricaDWSConnector(BaseConnector):
    """Connector for the South Africa DWS hydrological data service."""

    slug = "south_africa_dws"
    display_name = "DWS Hydrology (South Africa)"
    base_url = "https://www.dws.gov.za/Hydrology/Verified"
    country_codes = ["ZA"]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return river gauging stations from DWS.

        Attempts to fetch a live station catalog; falls back to the
        built-in seed list when the remote service is unreachable.
        """
        stations = await self._try_fetch_live_stations()
        if stations:
            return stations

        logger.warning(
            "live_catalog_unavailable",
            provider=self.slug,
            msg="Using seed station list",
        )
        return self._build_seed_stations()

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch flow observations for *station_id* between *start* and *end*."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        observations = await self._try_fetch_observations(native_id, start, end)

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
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
    # Live station catalog
    # ------------------------------------------------------------------

    async def _try_fetch_live_stations(self) -> list[Station]:
        """Attempt to download the station catalog from DWS.

        Returns an empty list on any failure so the caller can fall back
        to the seed list.
        """
        try:
            resp = await self._get(
                "/HyDataSets.aspx",
                params={"Station": "", "SiteType": "Flow"},
            )
        except (ConnectorError, httpx.HTTPError) as exc:
            logger.warning(
                "station_catalog_fetch_failed",
                provider=self.slug,
                error=str(exc),
            )
            return []

        return self._parse_station_catalog(resp.text)

    def _parse_station_catalog(self, html: str) -> list[Station]:
        """Best-effort parse of the DWS station catalog response.

        The response format can vary (HTML table, partial JSON, etc.).
        We look for JSON-like payload first, then fall back to simple
        HTML table parsing.  Returns an empty list if nothing is usable.
        """
        import json
        import re

        # Strategy 1: look for an embedded JSON array
        json_match = re.search(r"\[[\s\S]*\]", html)
        if json_match:
            try:
                data = json.loads(json_match.group())
                return self._stations_from_json(data)
            except (json.JSONDecodeError, TypeError):
                pass

        # Strategy 2: very simple HTML <tr> scraping
        stations = self._stations_from_html(html)
        if stations:
            return stations

        return []

    def _stations_from_json(self, data: list[dict]) -> list[Station]:
        """Parse a JSON array of station records."""
        stations: list[Station] = []
        for entry in data:
            native_id = str(entry.get("Station") or entry.get("station") or "").strip()
            if not native_id:
                continue
            name = str(
                entry.get("StationName")
                or entry.get("stationName")
                or entry.get("Name")
                or native_id
            ).strip()
            try:
                lat = float(entry.get("Latitude") or entry.get("latitude") or 0.0)
                lon = float(entry.get("Longitude") or entry.get("longitude") or 0.0)
            except (ValueError, TypeError):
                lat, lon = 0.0, 0.0
            river = (
                str(entry.get("River") or entry.get("river") or "").strip() or None
            )
            try:
                stations.append(
                    Station(
                        id=self._station_id(native_id),
                        provider=self.slug,
                        native_id=native_id,
                        name=name,
                        latitude=lat,
                        longitude=lon,
                        country_code="ZA",
                        river=river,
                    )
                )
            except (ValueError, KeyError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    station=native_id,
                    error=str(exc),
                )
        return stations

    def _stations_from_html(self, html: str) -> list[Station]:
        """Minimal HTML table scraper for the DWS station catalog.

        Looks for ``<tr>`` rows whose first cell looks like a DWS station
        code (e.g. ``A2H012``).
        """
        import re

        stations: list[Station] = []
        row_pattern = re.compile(r"<tr[^>]*>(.*?)</tr>", re.DOTALL | re.IGNORECASE)
        cell_pattern = re.compile(r"<td[^>]*>(.*?)</td>", re.DOTALL | re.IGNORECASE)
        tag_strip = re.compile(r"<[^>]+>")
        station_code_re = re.compile(r"^[A-Z]\d[A-Z]\d{3}$")

        for row_match in row_pattern.finditer(html):
            cells = cell_pattern.findall(row_match.group(1))
            if not cells:
                continue
            first_cell = tag_strip.sub("", cells[0]).strip()
            if not station_code_re.match(first_cell):
                continue

            native_id = first_cell
            name = tag_strip.sub("", cells[1]).strip() if len(cells) > 1 else native_id
            try:
                lat = float(tag_strip.sub("", cells[2]).strip()) if len(cells) > 2 else 0.0
                lon = float(tag_strip.sub("", cells[3]).strip()) if len(cells) > 3 else 0.0
            except (ValueError, IndexError):
                lat, lon = 0.0, 0.0
            river = (
                tag_strip.sub("", cells[4]).strip() if len(cells) > 4 else None
            ) or None

            try:
                stations.append(
                    Station(
                        id=self._station_id(native_id),
                        provider=self.slug,
                        native_id=native_id,
                        name=name,
                        latitude=lat,
                        longitude=lon,
                        country_code="ZA",
                        river=river,
                    )
                )
            except (ValueError, KeyError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    station=native_id,
                    error=str(exc),
                )
        return stations

    def _build_seed_stations(self) -> list[Station]:
        """Build :class:`Station` objects from the built-in seed list."""
        return [
            Station(
                id=self._station_id(s["native_id"]),
                provider=self.slug,
                native_id=s["native_id"],
                name=s["name"],
                latitude=s["latitude"],
                longitude=s["longitude"],
                country_code="ZA",
                river=s.get("river"),
            )
            for s in _SEED_STATIONS
        ]

    # ------------------------------------------------------------------
    # Observation fetching
    # ------------------------------------------------------------------

    async def _try_fetch_observations(
        self,
        native_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Try the DWS data-values endpoint; return empty list on failure."""
        station_id = self._station_id(native_id)
        params = {
            "Station": native_id,
            "DataType": "Flow",
            "StartDate": start.strftime("%Y-%m-%d"),
            "EndDate": end.strftime("%Y-%m-%d"),
            "Format": "json",
        }

        try:
            resp = await self._get("/HyDataValues.aspx", params=params)
        except (ConnectorError, httpx.HTTPError) as exc:
            logger.warning(
                "observation_fetch_failed",
                provider=self.slug,
                station=native_id,
                error=str(exc),
            )
            return []

        return self._parse_observations(resp.text, station_id)

    def _parse_observations(self, body: str, station_id: str) -> list[Observation]:
        """Parse observations from a DWS response body.

        Tries JSON first, then falls back to CSV-like line parsing.
        Returns an empty list if the body is unparseable.
        """
        import json

        body = body.strip()
        if not body:
            return []

        # Strategy 1: JSON
        try:
            data = json.loads(body)
            if isinstance(data, list):
                return self._observations_from_json(data, station_id)
            if isinstance(data, dict):
                # Some DWS endpoints wrap data in {"Data": [...]}
                inner = data.get("Data") or data.get("data") or data.get("values")
                if isinstance(inner, list):
                    return self._observations_from_json(inner, station_id)
        except (json.JSONDecodeError, TypeError):
            pass

        # Strategy 2: CSV / plain-text lines
        return self._observations_from_csv(body, station_id)

    def _observations_from_json(
        self, data: list[dict], station_id: str
    ) -> list[Observation]:
        """Parse a JSON array of observation records."""
        observations: list[Observation] = []
        for entry in data:
            ts_raw = entry.get("Date") or entry.get("date") or entry.get("Timestamp")
            if ts_raw is None:
                continue
            try:
                ts = _parse_timestamp(str(ts_raw))
            except DataFormatError:
                logger.warning(
                    "observation_timestamp_invalid",
                    provider=self.slug,
                    station=station_id,
                    raw=ts_raw,
                )
                continue

            value = entry.get("Value") or entry.get("value") or entry.get("Flow")
            discharge = _safe_float(value)
            quality = QualityFlag.RAW if discharge is not None else QualityFlag.MISSING

            observations.append(
                Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=quality,
                )
            )
        return observations

    def _observations_from_csv(self, body: str, station_id: str) -> list[Observation]:
        """Parse CSV/plain-text lines into observations.

        Expected format (header optional):
            Date,Value   or   Date\tValue
        """
        import re

        observations: list[Observation] = []
        for line in body.splitlines():
            line = line.strip()
            if not line or line.lower().startswith("date"):
                continue

            parts = re.split(r"[,\t;]+", line, maxsplit=1)
            if len(parts) < 2:
                continue

            try:
                ts = _parse_timestamp(parts[0].strip())
            except DataFormatError:
                continue

            discharge = _safe_float(parts[1].strip())
            quality = QualityFlag.RAW if discharge is not None else QualityFlag.MISSING

            observations.append(
                Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=quality,
                )
            )
        return observations


# ── Module-level helpers ───────────────────────────────────────────────

def _parse_timestamp(raw: str) -> datetime:
    """Parse a timestamp string in any of the formats DWS may use."""
    for fmt in (
        "%Y-%m-%dT%H:%M:%S",
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%d %H:%M",
        "%Y-%m-%d",
        "%Y/%m/%d %H:%M:%S",
        "%Y/%m/%d",
        "%d/%m/%Y %H:%M:%S",
        "%d/%m/%Y",
    ):
        try:
            dt = datetime.strptime(raw, fmt)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            return dt
        except ValueError:
            continue

    # Try ISO format as a last resort (handles timezone offsets)
    try:
        return datetime.fromisoformat(raw)
    except ValueError:
        pass

    raise DataFormatError("south_africa_dws", f"Unparseable timestamp: {raw!r}")


def _safe_float(value: object) -> float | None:
    """Convert *value* to float, returning ``None`` for blanks or errors."""
    if value is None:
        return None
    try:
        result = float(value)  # type: ignore[arg-type]
    except (ValueError, TypeError):
        return None
    return result
