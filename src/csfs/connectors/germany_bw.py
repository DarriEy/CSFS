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
  * ``fetch_observations()`` returns the most-recent discharge (Q, m³/s) and
    water-level (W, cm → converted to m) values for the requested station
    (filtered to the requested window). HVZ is a "latest value only" provider;
    there is no historical time series available in structured form.

Column layout of each PEG_DB row (subset we use), per ``js/hvz_peg_var.js``::

    0  DASA   station id (native_id)
    1  NAME   station name
    2  GEW    river (Gewässer)
    4  W      current water level value (Wasserstand), e.g. "52" or "--"
    5  WD     water level unit, "cm"
    6  WZ     water level timestamp, "DD.MM.YYYY HH:MM MESZ"
    7  Q      current discharge value (Abfluss), e.g. "12.6" or "--"
    8  QD     discharge unit, "m³/s"
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
    Resolution,
    Station,
    TimeSeriesChunk,
    Variable,
)
from csfs.core.registry import register

logger = structlog.get_logger()

# Path to the JavaScript "Stammdaten" catalogue (relative to base_url).
_CATALOGUE_PATH = "/js/hvz_peg_stmn.js"

# Column indices within each PEG_DB row (see module docstring).
_COL_DASA = 0
_COL_NAME = 1
_COL_RIVER = 2
_COL_W = 4
_COL_WDIM = 5
_COL_WDAT = 6
_COL_Q = 7
_COL_QDIM = 8
_COL_QDAT = 9
_COL_LON = 20
_COL_LAT = 21

_DISCHARGE_UNIT = "m³/s"
_LEVEL_UNIT = "cm"

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
    supported_variables = ("discharge", "stage")

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
        """Return the current discharge and water-level values (latest-only).

        HVZ does not expose historical time series, so this yields at most one
        observation per variable: the catalogue's current discharge (Q) and
        water level (W) readings, each included only if its timestamp falls
        within ``[start, end]``.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")
        row = await self._resolve_row(native_id)

        observations = [
            obs
            for obs in self._row_to_observations(row, station_id)
            if start <= obs.timestamp <= end
        ]

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

    def _row_to_observations(
        self, fields: list[str], station_id: str
    ) -> list[Observation]:
        """Convert a catalogue row's current readings into Observations.

        Emits the current discharge (Q, m³/s) and water level (W, cm → m)
        when present. HVZ carries a single spot reading per variable, so
        both are tagged instantaneous.
        """
        observations: list[Observation] = []

        if fields[_COL_QDIM] == _DISCHARGE_UNIT:
            discharge = self._parse_value(fields[_COL_Q])
            ts = self._parse_timestamp(fields[_COL_QDAT])
            if discharge is not None and ts is not None:
                observations.append(
                    Observation(
                        station_id=station_id,
                        timestamp=ts,
                        variable=Variable.DISCHARGE,
                        resolution=Resolution.INSTANTANEOUS,
                        value=discharge,
                        quality=QualityFlag.RAW,
                    )
                )

        if fields[_COL_WDIM] == _LEVEL_UNIT:
            level = self._parse_value(fields[_COL_W])
            ts = self._parse_timestamp(fields[_COL_WDAT])
            if level is not None and ts is not None:
                observations.append(
                    Observation(
                        station_id=station_id,
                        timestamp=ts,
                        variable=Variable.STAGE,
                        resolution=Resolution.INSTANTANEOUS,
                        value=level / 100.0,  # HVZ levels are cm; store metres
                        quality=QualityFlag.RAW,
                    )
                )

        return observations

    @staticmethod
    def _parse_value(raw: str) -> float | None:
        """Parse a catalogue value cell; '--' and '' mean no current value."""
        if raw in ("--", ""):
            return None
        try:
            return float(raw)
        except ValueError:
            return None

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
        naive = naive.replace(tzinfo=tz) if tz is not None else naive.replace(tzinfo=UTC)
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
