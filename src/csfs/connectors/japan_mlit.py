"""Japan MLIT Water Information System connector.

The Ministry of Land, Infrastructure, Transport and Tourism (MLIT) operates
Japan's river monitoring network via http://www1.river.go.jp.  The system
is primarily HTML-based and notoriously fragile, so this connector takes a
defensive approach:

* **Station listing** is backed by a curated seed list of major gauging
  stations with known IDs, coordinates, and river names.  A live discovery
  call is attempted but the seed list is always returned as a fallback.
* **Observations** are fetched from the telemetry data endpoint which
  returns CSV-like content.  Parsing is wrapped in extensive error handling.
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

# ---------------------------------------------------------------------------
# Curated seed stations – major discharge gauging points across Japan.
# Format: (native_id, name, latitude, longitude, river)
# ---------------------------------------------------------------------------
_SEED_STATIONS: list[tuple[str, str, float, float, str]] = [
    ("305011283018070", "Kurihashi", 36.1314, 139.7006, "Tone River"),
    ("303051283015040", "Ojiya", 37.3000, 138.7936, "Shinano River"),
    ("306021283019050", "Kitakami-Ohashi", 39.2667, 141.1167, "Kitakami River"),
    ("304041283016060", "Kumagai", 36.1500, 139.3833, "Ara River"),
    ("309011283022030", "Okayama", 34.6617, 133.9350, "Asahi River"),
    ("310031283024070", "Daini-Miyamae", 34.0667, 134.5500, "Yoshino River"),
    ("308011283021040", "Hirakata", 34.8167, 135.6500, "Yodo River"),
    ("302011283013020", "Moiwa", 43.0167, 141.3333, "Ishikari River"),
    ("311021283025050", "Senoshita", 33.3167, 131.6167, "Ono River"),
    ("312011283027030", "Hitoyoshi", 32.2167, 130.7500, "Kuma River"),
]


@register("japan_mlit")
class JapanMlitConnector(BaseConnector):
    slug = "japan_mlit"
    display_name = "MLIT Water Information System (Japan)"
    base_url = "http://www1.river.go.jp"
    country_codes = ["JP"]

    # Telemetry data path pattern.  KIND=9 → discharge, BESSION=1 → CSV mode.
    _DATA_PATH = "/cgi-bin/DspFlowData.exe"

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        self._station_cache: dict[str, Station] = {}

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return curated discharge stations.

        We always return the seed list.  A live discovery call is attempted
        to augment the list, but failures are silently ignored because the
        MLIT web interface is unreliable for automated access.
        """
        stations = [self._build_seed_station(row) for row in _SEED_STATIONS]

        # Try augmenting from live endpoint (best-effort)
        try:
            live = await self._discover_stations_live()
            # Merge live stations that are not already in the seed list
            seed_ids = {s.native_id for s in stations}
            for st in live:
                if st.native_id not in seed_ids:
                    stations.append(st)
        except Exception:
            logger.debug("live_station_discovery_skipped", provider=self.slug)

        # Populate cache
        for s in stations:
            self._station_cache[s.native_id] = s

        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for *station_id* between *start* and *end*."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        all_observations: list[Observation] = []
        current = start
        while current < end:
            day_end = min(current + timedelta(days=1), end)
            try:
                obs = await self._fetch_day(native_id, current, station_id)
                # Filter to requested window
                all_observations.extend(
                    o for o in obs if start <= o.timestamp <= end
                )
            except ConnectorError:
                logger.warning(
                    "day_fetch_failed",
                    provider=self.slug,
                    station=native_id,
                    date=current.isoformat(),
                )
            current = day_end

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=all_observations,
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
    # Internal helpers
    # ------------------------------------------------------------------

    def _build_seed_station(self, row: tuple[str, str, float, float, str]) -> Station:
        """Create a Station model from a seed-list tuple."""
        native_id, name, lat, lon, river = row
        return Station(
            id=self._station_id(native_id),
            provider=self.slug,
            native_id=native_id,
            name=name,
            latitude=lat,
            longitude=lon,
            country_code="JP",
            river=river,
        )

    async def _discover_stations_live(self) -> list[Station]:
        """Attempt to discover stations from the MLIT station-info endpoint.

        This endpoint returns HTML and is fragile; failures are expected.
        """
        resp = await self._get(
            "/cgi-bin/SiteInfo.exe",
            params={"ID": "0", "KIND": "2"},
        )
        return self._parse_station_html(resp.text)

    def _parse_station_html(self, html: str) -> list[Station]:
        """Best-effort parse of the station listing HTML.

        The page layout changes frequently so we only extract rows that
        match a lenient regex.  Returns an empty list on any parse failure.
        """
        stations: list[Station] = []
        try:
            # Look for table rows containing station data.
            # Pattern: 15-digit station ID followed by station name.
            pattern = re.compile(
                r"(\d{15})\s*</td>\s*<td[^>]*>\s*([^<]+)",
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
                    country_code="JP",
                ))
        except Exception:
            logger.debug("station_html_parse_failed", provider=self.slug)
        return stations

    async def _fetch_day(
        self,
        native_id: str,
        day: datetime,
        station_id: str,
    ) -> list[Observation]:
        """Fetch a single day's discharge data for a station."""
        params = {
            "KIND": "9",
            "ID": native_id,
            "BESSION": "1",
            "YEAR": str(day.year),
            "MONTH": str(day.month).zfill(2),
            "DAY": str(day.day).zfill(2),
        }
        try:
            resp = await self._get(self._DATA_PATH, params=params)
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch data for station {native_id}: {exc}",
            ) from exc

        return self._parse_csv_response(resp.text, station_id, day)

    def _parse_csv_response(
        self,
        text: str,
        station_id: str,
        day: datetime,
    ) -> list[Observation]:
        """Parse CSV/text response into Observation objects.

        The MLIT telemetry endpoint returns data in several possible
        formats.  We try CSV first, then fall back to line-by-line
        whitespace-delimited parsing.
        """
        text = text.strip()
        if not text:
            return []

        # Attempt 1: standard CSV
        observations = self._try_parse_csv(text, station_id, day)
        if observations is not None:
            return observations

        # Attempt 2: whitespace-delimited lines (time  value)
        observations = self._try_parse_whitespace(text, station_id, day)
        if observations is not None:
            return observations

        raise DataFormatError(
            self.slug,
            f"Unable to parse response for station {station_id}",
        )

    def _try_parse_csv(
        self,
        text: str,
        station_id: str,
        day: datetime,
    ) -> list[Observation] | None:
        """Try to parse text as CSV with columns: datetime/time, discharge."""
        try:
            reader = csv.reader(io.StringIO(text))
            observations: list[Observation] = []
            for row in reader:
                if len(row) < 2:
                    continue
                ts = self._parse_timestamp(row[0].strip(), day)
                if ts is None:
                    continue
                discharge = self._parse_discharge(row[1].strip())
                quality = QualityFlag.RAW if discharge is not None else QualityFlag.MISSING
                observations.append(Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=quality,
                ))
            if observations:
                return observations
        except Exception:
            pass
        return None

    def _try_parse_whitespace(
        self,
        text: str,
        station_id: str,
        day: datetime,
    ) -> list[Observation] | None:
        """Try to parse text as whitespace-delimited lines: time  value."""
        try:
            observations: list[Observation] = []
            for line in text.splitlines():
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                parts = line.split()
                if len(parts) < 2:
                    continue
                ts = self._parse_timestamp(parts[0], day)
                if ts is None:
                    continue
                discharge = self._parse_discharge(parts[1])
                quality = QualityFlag.RAW if discharge is not None else QualityFlag.MISSING
                observations.append(Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=quality,
                ))
            if observations:
                return observations
        except Exception:
            pass
        return None

    @staticmethod
    def _parse_timestamp(raw: str, day: datetime) -> datetime | None:
        """Parse a timestamp string into a datetime.

        Accepts full ISO-8601 timestamps or HH:MM time-of-day values
        (anchored to *day*).
        """
        # Full datetime (ISO-8601 with or without timezone)
        for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y/%m/%d %H:%M:%S"):
            try:
                return datetime.strptime(raw, fmt).replace(tzinfo=UTC)
            except ValueError:
                continue

        # Time-only (e.g., "14:30")
        time_match = re.match(r"^(\d{1,2}):(\d{2})$", raw)
        if time_match:
            hour, minute = int(time_match.group(1)), int(time_match.group(2))
            if 0 <= hour <= 23 and 0 <= minute <= 59:
                return day.replace(hour=hour, minute=minute, second=0, tzinfo=UTC)

        return None

    @staticmethod
    def _parse_discharge(raw: str) -> float | None:
        """Parse a discharge value, returning None for missing/invalid data."""
        if not raw or raw in ("--", "-", "***", "N/A", "欠測"):
            return None
        try:
            return float(raw)
        except (ValueError, TypeError):
            return None
