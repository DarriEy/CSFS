"""Germany Baden-Württemberg connector -- HVZ (Hochwasservorhersagezentrale).

HVZ BW (operated by LUBW) publishes near-real-time water level and discharge
data for its gauging network in Baden-Württemberg. Unlike PEGELONLINE, HVZ does
NOT expose a per-station time-series JSON/REST endpoint: historical curves are
served only as pre-rendered GIF plots (``gifs/<id>-340.GIF``). The only
machine-readable discharge data is the *current* measurement, which the site
ships to its frontend inside a JavaScript "Stammdaten" file -- a big
``HVZ_Site.PEG_DB = [ [ ... ], ... ];`` array, one row per station.

This connector therefore parses that catalogue file:
  * ``fetch_stations()`` returns every station that publishes a discharge
    (Abfluss / Q) value in m³/s.
  * ``fetch_observations()`` returns the single most-recent discharge value
    for the requested station (filtered to the requested window). HVZ is a
    "latest value only" provider; there is no historical time series available
    in structured form.

Column layout of each PEG_DB row (subset we use), per ``js/hvz_peg_var.js``::

    0  DASA   station id (native_id)
    1  NAME   station name
    2  GEW    river (Gewässer)
    7  Q      current discharge value (Abfluss), e.g. "12.6" or "--"
    8  QD     discharge unit, "m³/s" (water level "W" is col 4, unit "cm")
    9  QZ     discharge timestamp, "DD.MM.YYYY HH:MM MESZ"
    20 GL     geographic longitude
    21 GB     geographic latitude

References
----------
- Portal: https://www.hvz.baden-wuerttemberg.de/
- Catalogue: https://www.hvz.baden-wuerttemberg.de/js/hvz_peg_stmn.js
- Column defs: https://www.hvz.baden-wuerttemberg.de/js/hvz_peg_var.js
"""

from __future__ import annotations

import csv
import io
import re
from datetime import UTC, datetime, timedelta, timezone

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import (
    Observation,
    QualityFlag,
    Station,
    TimeSeriesChunk,
)
from csfs.core.registry import register

logger = structlog.get_logger()

# Path to the JavaScript "Stammdaten" catalogue (relative to base_url).
_CATALOGUE_PATH = "/js/hvz_peg_stmn.js"

# Column indices within each PEG_DB row (see module docstring).
_COL_DASA = 0
_COL_NAME = 1
_COL_RIVER = 2
_COL_Q = 7
_COL_QDIM = 8
_COL_QDAT = 9
_COL_LON = 20
_COL_LAT = 21

_DISCHARGE_UNIT = "m³/s"

# HVZ timestamps look like "02.06.2026 07:00 MESZ". MESZ = CEST = UTC+2,
# MEZ = CET = UTC+1.
_TZ_OFFSETS = {
    "MESZ": timezone(timedelta(hours=2)),
    "MEZ": timezone(timedelta(hours=1)),
}

# Matches the body of   HVZ_Site.PEG_DB = [ ... ];
_PEG_DB_RE = re.compile(r"PEG_DB\s*=\s*\[(.*)\]\s*;", re.S)


@register("germany_bw")
class GermanyBwConnector(BaseConnector):
    """Connector for Baden-Württemberg HVZ (LUBW).

    HVZ has no time-series API, so observations are limited to the current
    discharge value carried in the station catalogue.
    """

    slug = "germany_bw"
    display_name = "HVZ Baden-Württemberg (LUBW)"
    base_url = "https://www.hvz.baden-wuerttemberg.de"
    country_codes = ["DE"]

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        # native_id -> parsed catalogue row (list of strings)
        self._catalogue: dict[str, list[str]] = {}

    async def fetch_stations(self) -> list[Station]:
        """Return every HVZ station that publishes a discharge (Q) value."""
        resp = await self._get(_CATALOGUE_PATH)
        return self._parse_stations(resp.text)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Return the current discharge value for a station (latest-only).

        HVZ does not expose historical time series, so this yields at most one
        observation: the catalogue's current discharge reading, included only if
        its timestamp falls within ``[start, end]``.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")
        row = await self._resolve_row(native_id)

        observations: list[Observation] = []
        obs = self._row_to_observation(row, station_id)
        if obs is not None and start <= obs.timestamp <= end:
            observations.append(obs)

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_rows(self, text: str) -> list[list[str]]:
        """Extract and CSV-parse the PEG_DB array rows from the JS catalogue."""
        match = _PEG_DB_RE.search(text)
        if not match:
            raise DataFormatError(
                self.slug, "PEG_DB array not found in HVZ catalogue"
            )

        rows: list[list[str]] = []
        for raw in re.findall(r"\[(.*?)\]", match.group(1), re.S):
            try:
                fields = next(
                    csv.reader(
                        io.StringIO(raw), quotechar="'", skipinitialspace=True
                    )
                )
            except StopIteration:
                continue
            if len(fields) > _COL_LAT:
                rows.append([f.strip() for f in fields])
        return rows

    def _parse_stations(self, text: str) -> list[Station]:
        """Parse the catalogue and return discharge-capable stations."""
        stations: list[Station] = []
        self._catalogue = {}

        for fields in self._parse_rows(text):
            native_id = fields[_COL_DASA]
            if not native_id:
                continue
            # Keep only stations that publish a discharge series (unit m³/s).
            if fields[_COL_QDIM] != _DISCHARGE_UNIT:
                continue

            self._catalogue[native_id] = fields

            try:
                lat = float(fields[_COL_LAT])
                lon = float(fields[_COL_LON])
            except (ValueError, IndexError):
                lat, lon = 0.0, 0.0

            river = fields[_COL_RIVER] or None
            try:
                stations.append(
                    Station(
                        id=self._station_id(native_id),
                        provider=self.slug,
                        native_id=native_id,
                        name=fields[_COL_NAME] or native_id,
                        latitude=lat,
                        longitude=lon,
                        country_code="DE",
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
                continue

        return stations

    def _row_to_observation(
        self, fields: list[str], station_id: str
    ) -> Observation | None:
        """Convert a catalogue row's current discharge into an Observation.

        Returns ``None`` if the station has no current discharge value.
        """
        if fields[_COL_QDIM] != _DISCHARGE_UNIT:
            return None

        raw_value = fields[_COL_Q]
        if raw_value in ("--", ""):
            return None

        try:
            discharge = float(raw_value)
        except ValueError:
            return None

        ts = self._parse_timestamp(fields[_COL_QDAT])
        if ts is None:
            return None

        return Observation(
            station_id=station_id,
            timestamp=ts,
            discharge_m3s=discharge,
            quality=QualityFlag.RAW,
        )

    @staticmethod
    def _parse_timestamp(raw: str) -> datetime | None:
        """Parse 'DD.MM.YYYY HH:MM MESZ' into a UTC-aware datetime."""
        parts = raw.split()
        if len(parts) < 2:
            return None
        date_part, time_part = parts[0], parts[1]
        tz = _TZ_OFFSETS.get(parts[2]) if len(parts) > 2 else None
        try:
            naive = datetime.strptime(
                f"{date_part} {time_part}", "%d.%m.%Y %H:%M"
            )
        except ValueError:
            return None
        if tz is not None:
            naive = naive.replace(tzinfo=tz)
        else:
            naive = naive.replace(tzinfo=UTC)
        return naive.astimezone(UTC)

    async def _resolve_row(self, native_id: str) -> list[str]:
        """Return the catalogue row for a station, fetching the list if needed."""
        if native_id in self._catalogue:
            return self._catalogue[native_id]

        await self.fetch_stations()

        if native_id not in self._catalogue:
            raise DataFormatError(
                self.slug,
                f"No HVZ discharge station found for id '{native_id}'",
            )
        return self._catalogue[native_id]
