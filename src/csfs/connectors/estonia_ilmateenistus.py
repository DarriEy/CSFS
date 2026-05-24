"""Estonia Ilmateenistus connector — Estonian Weather Service hydrology data.

Provides water level and discharge observations from Estonian inland waters.
Base URL: https://www.ilmateenistus.ee

Station listing is obtained from the hydrology page (HTML parsing fallback)
or a JSON endpoint. Observations are fetched as CSV from the historical
data download interface.
"""

from __future__ import annotations

import csv
import io
import re
from datetime import UTC, datetime, timedelta

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# Regex to extract station entries from the Ilmateenistus hydrology HTML page.
# Matches table rows or links that contain station code, name, and coordinates.
_STATION_LINK_RE = re.compile(
    r'data-code=["\'](?P<code>[^"\']+)["\']'
    r'[^>]*data-name=["\'](?P<name>[^"\']+)["\']'
    r'[^>]*data-lat=["\'](?P<lat>[^"\']+)["\']'
    r'[^>]*data-lon=["\'](?P<lon>[^"\']+)["\']'
    r'(?:[^>]*data-water=["\'](?P<water>[^"\']+)["\'])?',
)

# Fallback: simpler link pattern for station pages
_STATION_SIMPLE_RE = re.compile(
    r'href=["\'](?:/siseveed/|/ilm/[^"\']*)'
    r'(?:station/)?(?P<code>\d+)["\']'
    r'[^<]*>(?P<name>[^<]+)<',
)


@register("estonia_ilmateenistus")
class EstoniaIlmateenistusConnector(BaseConnector):
    """Connector for the Estonian Weather Service (Ilmateenistus) hydrology data.

    The service publishes inland water observations (water level, discharge)
    for Estonian rivers and lakes. Station metadata may be available as JSON
    or scraped from the HTML hydrology portal. Observations are downloaded
    as CSV.
    """

    slug = "estonia_ilmateenistus"
    display_name = "Ilmateenistus (Estonia)"
    base_url = "https://www.ilmateenistus.ee"
    country_codes = ["EE"]

    # Candidate JSON endpoint paths for station listing (tried in order)
    _STATION_JSON_PATHS = (
        "/ilm/ilmavaatlused/vaatlusandmed/json/hydro",
        "/api/hydro/stations",
        "/siseveed/api/stations",
    )

    # HTML page to scrape stations from as a fallback
    _STATION_HTML_PATH = "/siseveed/"

    # Candidate CSV download paths for observations (tried in order)
    _OBS_CSV_PATHS = (
        "/siseveed/data/{station_code}",
        "/siseveed/historical-data/csv/{station_code}",
    )

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all hydrology stations from Ilmateenistus.

        Tries JSON endpoints first; falls back to HTML scraping.
        """
        # Try JSON endpoints
        for path in self._STATION_JSON_PATHS:
            try:
                resp = await self._get(path, params={"lang": "en"})
                data = resp.json()
                stations = self._parse_stations_json(data)
                if stations:
                    return stations
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "json_station_endpoint_failed",
                    provider=self.slug,
                    path=path,
                    error=str(exc),
                )

        # Fall back to HTML scraping
        try:
            resp = await self._get(
                self._STATION_HTML_PATH,
                params={"lang": "en"},
            )
            stations = self._parse_stations_html(resp.text)
            if stations:
                return stations
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch stations from all endpoints: {exc}",
            ) from exc

        raise ConnectorError(
            self.slug,
            "No stations found from any endpoint",
        )

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch hydrology observations for a station over a date range.

        Tries CSV download endpoints in order.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")
        last_exc: Exception | None = None

        for path_template in self._OBS_CSV_PATHS:
            path = path_template.format(station_code=native_id)
            try:
                resp = await self._get(
                    path,
                    params={
                        "var": "flow",
                        "period": "daily",
                        "format": "csv",
                        "start": start.strftime("%Y-%m-%d"),
                        "end": end.strftime("%Y-%m-%d"),
                        "lang": "en",
                    },
                )
                content_type = resp.headers.get("content-type", "")
                if "json" in content_type:
                    return self._parse_observations_json(
                        resp.json(), station_id, start, end,
                    )
                return self._parse_observations_csv(
                    resp.text, station_id, start, end,
                )
            except (DataFormatError, ConnectorError):
                raise
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "obs_endpoint_failed",
                    provider=self.slug,
                    station=native_id,
                    path=path,
                    error=str(exc),
                )
                last_exc = exc

        raise ConnectorError(
            self.slug,
            f"All observation endpoints failed for {native_id}: {last_exc}",
        ) from last_exc

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent observations (last 7 days)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(days=7),
            end=now,
        )

    # ------------------------------------------------------------------
    # Station parsers
    # ------------------------------------------------------------------

    def _parse_stations_json(self, data: list | dict) -> list[Station]:
        """Parse a JSON station list response.

        Handles both a bare list and a dict with a nested list under
        common keys like 'stations', 'data', or 'features'.
        """
        entries: list[dict] = []
        if isinstance(data, list):
            entries = data
        elif isinstance(data, dict):
            for key in ("stations", "data", "features"):
                candidate = data.get(key)
                if isinstance(candidate, list):
                    entries = candidate
                    break
            if not entries:
                return []
        else:
            return []

        stations: list[Station] = []
        for entry in entries:
            code = entry.get("code", entry.get("id", entry.get("stationId")))
            if not code:
                logger.warning(
                    "station_missing_code",
                    provider=self.slug,
                )
                continue

            native_id = str(code)

            try:
                lat = float(str(
                    entry.get("latitude", entry.get("lat", 0))
                ))
                lon = float(str(
                    entry.get("longitude", entry.get("lon", 0))
                ))
            except (ValueError, TypeError):
                lat, lon = 0.0, 0.0

            name = entry.get("name", entry.get("stationName", native_id))
            water_body = entry.get(
                "waterBody",
                entry.get("river", entry.get("water")),
            )

            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=name,
                    latitude=lat,
                    longitude=lon,
                    country_code="EE",
                    river=water_body,
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

    def _parse_stations_html(self, html: str) -> list[Station]:
        """Parse stations from the Ilmateenistus hydrology HTML page.

        Tries data-attribute patterns first, then falls back to simpler
        link patterns.
        """
        stations: list[Station] = []
        seen_codes: set[str] = set()

        # Try structured data-attribute pattern first
        for match in _STATION_LINK_RE.finditer(html):
            code = match.group("code")
            if code in seen_codes:
                continue
            seen_codes.add(code)

            try:
                lat = float(str(match.group("lat")))
                lon = float(str(match.group("lon")))
            except (ValueError, TypeError):
                lat, lon = 0.0, 0.0

            water = match.group("water")

            try:
                stations.append(Station(
                    id=self._station_id(code),
                    provider=self.slug,
                    native_id=code,
                    name=match.group("name"),
                    latitude=lat,
                    longitude=lon,
                    country_code="EE",
                    river=water,
                ))
            except (ValueError, KeyError) as exc:
                logger.warning(
                    "html_station_parse_failed",
                    provider=self.slug,
                    station=code,
                    error=str(exc),
                )
                continue

        if stations:
            return stations

        # Fallback: simple link-based extraction
        for match in _STATION_SIMPLE_RE.finditer(html):
            code = match.group("code")
            if code in seen_codes:
                continue
            seen_codes.add(code)

            try:
                stations.append(Station(
                    id=self._station_id(code),
                    provider=self.slug,
                    native_id=code,
                    name=match.group("name").strip(),
                    latitude=0.0,
                    longitude=0.0,
                    country_code="EE",
                ))
            except (ValueError, KeyError) as exc:
                logger.warning(
                    "html_station_parse_failed",
                    provider=self.slug,
                    station=code,
                    error=str(exc),
                )
                continue

        return stations

    # ------------------------------------------------------------------
    # Observation parsers
    # ------------------------------------------------------------------

    def _parse_observations_csv(
        self,
        csv_text: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Parse CSV observation data from the download endpoint.

        Expected CSV columns (flexible matching):
        date, flow (m3/s), water_level (m)
        """
        observations: list[Observation] = []
        start_naive = start.replace(tzinfo=None) if start.tzinfo else start
        end_naive = end.replace(tzinfo=None) if end.tzinfo else end

        reader = csv.reader(io.StringIO(csv_text.strip()))
        header: list[str] = []
        date_col = -1
        flow_col = -1
        level_col = -1

        for row in reader:
            if not row:
                continue

            # Detect header row
            if not header:
                lower_row = [c.strip().lower() for c in row]
                if self._is_header_row(lower_row):
                    header = lower_row
                    date_col = self._find_column(
                        header, ("date", "kuupaev", "datetime", "time"),
                    )
                    flow_col = self._find_column(
                        header, ("flow", "discharge", "q", "vooluhulk"),
                    )
                    level_col = self._find_column(
                        header,
                        ("water_level", "level", "tase", "waterlevel"),
                    )
                    continue
                # If first row looks like data (no header), use positional
                header = ["_positional"]
                date_col = 0
                flow_col = 1 if len(row) > 1 else -1
                level_col = 2 if len(row) > 2 else -1

            # Parse data row
            obs = self._parse_csv_row(
                row, date_col, flow_col, level_col,
                station_id, start_naive, end_naive,
            )
            if obs is not None:
                observations.append(obs)

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    def _parse_observations_json(
        self,
        data: list | dict,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Parse JSON observation data (alternative response format)."""
        entries: list[dict] = []
        if isinstance(data, list):
            entries = data
        elif isinstance(data, dict):
            for key in ("observations", "data", "values"):
                candidate = data.get(key)
                if isinstance(candidate, list):
                    entries = candidate
                    break

        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)
        if end.tzinfo is None:
            end = end.replace(tzinfo=UTC)

        observations: list[Observation] = []
        for entry in entries:
            ts_raw = entry.get(
                "date", entry.get("timestamp", entry.get("time")),
            )
            if not ts_raw:
                continue

            try:
                ts = datetime.fromisoformat(
                    str(ts_raw).replace("Z", "+00:00"),
                )
            except (ValueError, TypeError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid timestamp in observation: {ts_raw}",
                ) from exc

            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)

            if ts < start or ts > end:
                continue

            discharge = self._extract_discharge(entry)

            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=discharge,
                quality=(
                    QualityFlag.RAW if discharge is not None
                    else QualityFlag.MISSING
                ),
            ))

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # ------------------------------------------------------------------
    # CSV helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _is_header_row(lower_row: list[str]) -> bool:
        """Check if a row looks like a CSV header."""
        header_keywords = {
            "date", "kuupaev", "datetime", "time",
            "flow", "discharge", "q", "vooluhulk",
            "water_level", "level", "tase", "waterlevel",
        }
        return any(cell in header_keywords for cell in lower_row)

    @staticmethod
    def _find_column(header: list[str], candidates: tuple[str, ...]) -> int:
        """Find the index of a column by trying multiple name candidates."""
        for candidate in candidates:
            for i, col in enumerate(header):
                if candidate in col:
                    return i
        return -1

    def _parse_csv_row(
        self,
        row: list[str],
        date_col: int,
        flow_col: int,
        level_col: int,
        station_id: str,
        start_naive: datetime,
        end_naive: datetime,
    ) -> Observation | None:
        """Parse a single CSV data row into an Observation or None."""
        if date_col < 0 or date_col >= len(row):
            return None

        date_str = row[date_col].strip()
        if not date_str:
            return None

        try:
            ts = self._parse_date(date_str)
        except ValueError:
            return None

        ts_naive = ts.replace(tzinfo=None) if ts.tzinfo else ts
        if ts_naive < start_naive or ts_naive > end_naive:
            return None

        discharge: float | None = None

        # Prefer flow column, fall back to level column
        if flow_col >= 0 and flow_col < len(row):
            raw = row[flow_col].strip()
            if raw and raw not in ("-", "", "NA", "n/a"):
                try:
                    discharge = float(str(raw.replace(",", ".")))
                except (ValueError, TypeError):
                    discharge = None

        if discharge is None and level_col >= 0 and level_col < len(row):
            raw = row[level_col].strip()
            if raw and raw not in ("-", "", "NA", "n/a"):
                try:
                    discharge = float(str(raw.replace(",", ".")))
                except (ValueError, TypeError):
                    discharge = None

        return Observation(
            station_id=station_id,
            timestamp=ts,
            discharge_m3s=discharge,
            quality=(
                QualityFlag.RAW if discharge is not None
                else QualityFlag.MISSING
            ),
        )

    @staticmethod
    def _parse_date(date_str: str) -> datetime:
        """Parse a date string in various formats used by Ilmateenistus."""
        for fmt in (
            "%Y-%m-%d",
            "%d.%m.%Y",
            "%Y-%m-%dT%H:%M:%S",
            "%d.%m.%Y %H:%M",
            "%Y-%m-%d %H:%M:%S",
        ):
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        return datetime.fromisoformat(date_str)

    @staticmethod
    def _extract_discharge(entry: dict) -> float | None:
        """Extract a discharge or water level value from a JSON observation."""
        for key in ("flow", "discharge", "q", "value", "waterLevel"):
            raw = entry.get(key)
            if raw is not None:
                try:
                    return float(str(raw))
                except (ValueError, TypeError):
                    continue
        return None
