# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Vietnam Mekong Delta connector — EIDC hourly discharge and sediment.

The UK Centre for Ecology & Hydrology's Environmental Information Data
Centre (EIDC) hosts hourly discharge and suspended sediment data for
four Mekong Delta stations in Vietnam (2005-2015).

This connector supports three modes:

1. **EIDC data-package download** — downloads the openly-licensed
   (Open Government Licence) data archive directly from the EIDC data
   package server and parses the per-station ``*ratings.csv`` files,
   which carry a genuine ``Discharge (m3/s)`` column of in-situ
   measurements (2005-2017).  This is the default live source and
   requires no configuration or authentication.

2. **EIDC catalogue fetch** — attempts to read observation data from
   the EIDC catalogue JSON API (document
   ac5b28ca-e087-4aec-974a-5a9f84b06595).  The catalogue API serves
   metadata only, so this path is a best-effort fallback.

3. **Local CSV files** — reads downloaded CSV files from a local
   directory configured via ``config["data_dir"]``.  Expected columns:
   ``datetime, discharge_m3s, sediment``.

If no source is available, empty chunks with download guidance are
returned.

Note on units: the EIDC archive also ships ``*fluxes.csv`` files, but
those hold daily *sediment* fluxes (kg/s scale), not discharge.  Only
the ``Discharge (m3/s)`` column of the ratings files is treated as
discharge here.
"""

from __future__ import annotations

import io
import zipfile
from datetime import UTC, datetime
from pathlib import Path

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
# Constants
# ---------------------------------------------------------------------------
EIDC_CATALOGUE_DOC = (
    "ac5b28ca-e087-4aec-974a-5a9f84b06595"
)
EIDC_CATALOGUE_URL = (
    "https://catalogue.ceh.ac.uk/documents/"
    + EIDC_CATALOGUE_DOC
)
# Openly downloadable (Open Government Licence) data-package archive.
EIDC_DATA_ZIP_URL = (
    "https://data-package.ceh.ac.uk/data/"
    + EIDC_CATALOGUE_DOC
    + ".zip"
)

# Maps a seed-station native_id to the stem of its ``*ratings.csv`` file
# inside the EIDC archive (e.g. ``chau_doc`` -> ``Chaudoc``).
_RATINGS_FILE_STEMS: dict[str, str] = {
    "chau_doc": "Chaudoc",
    "tan_chau": "Tanchau",
    "can_tho": "Cantho",
    "my_thaun": "Mythaun",
}

# ---------------------------------------------------------------------------
# Curated seed catalogue of 4 Mekong Delta stations
# ---------------------------------------------------------------------------
_SEED_STATIONS: list[dict] = [
    {
        "native_id": "chau_doc",
        "name": "Chau Doc",
        "lat": 10.70,
        "lon": 105.12,
        "river": "Mekong (Bassac)",
    },
    {
        "native_id": "tan_chau",
        "name": "Tan Chau",
        "lat": 10.80,
        "lon": 105.23,
        "river": "Mekong (Tien)",
    },
    {
        "native_id": "can_tho",
        "name": "Can Tho",
        "lat": 10.03,
        "lon": 105.78,
        "river": "Mekong (Bassac)",
    },
    {
        "native_id": "my_thaun",
        "name": "My Thaun",
        "lat": 10.27,
        "lon": 106.06,
        "river": "Mekong (Tien)",
    },
]


@register("vietnam_mekong")
class VietnamMekongConnector(BaseConnector):
    """Connector for EIDC Mekong Delta discharge and sediment data.

    Configuration options (via ``config`` dict):
        data_dir : str | Path
            Directory containing downloaded EIDC CSV files with
            hourly discharge and sediment data.
    """

    slug = "vietnam_mekong"
    display_name = "Vietnam Mekong Delta (EIDC)"
    base_url = "https://catalogue.ceh.ac.uk"
    country_codes: list[str] = ["VN"]

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        # Cache the downloaded archive bytes for the lifetime of the
        # connector so multi-station fetches hit the network only once.
        self._archive_bytes: bytes | None = None
        self._archive_attempted = False

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return seed list of Mekong Delta stations."""
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
        """Fetch observations via EIDC catalogue or local CSV files.

        Tries the EIDC catalogue API first, then local CSV fallback.
        Returns empty chunk with guidance if none work.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        # Primary live source: openly-licensed EIDC data-package archive.
        chunk = await self._try_fetch_data_package(
            native_id, station_id, start, end,
        )
        if chunk is not None:
            return chunk

        # Try EIDC catalogue endpoint (metadata-only; best effort)
        chunk = await self._try_fetch_eidc(
            native_id, station_id, start, end,
        )
        if chunk is not None:
            return chunk

        # Try local CSV files
        chunk = self._try_fetch_local_csv(
            native_id, station_id, start, end,
        )
        if chunk is not None:
            return chunk

        logger.info(
            "vietnam_mekong_no_data_source",
            station=native_id,
            hint=(
                "Download EIDC Mekong Delta data from "
                f"{EIDC_CATALOGUE_URL} and set "
                "config['data_dir'] to the download directory."
            ),
        )
        return self._empty_chunk(station_id)

    # ------------------------------------------------------------------
    # Seed catalogue
    # ------------------------------------------------------------------

    def _build_seed_stations(self) -> list[Station]:
        """Build Station objects from the curated seed list."""
        stations: list[Station] = []
        for entry in _SEED_STATIONS:
            stations.append(
                Station(
                    id=self._station_id(entry["native_id"]),
                    provider=self.slug,
                    native_id=entry["native_id"],
                    name=entry["name"],
                    latitude=float(str(entry["lat"])),
                    longitude=float(str(entry["lon"])),
                    country_code="VN",
                    river=entry.get("river"),
                )
            )
        return stations

    # ------------------------------------------------------------------
    # EIDC data-package archive (primary live source)
    # ------------------------------------------------------------------

    async def _try_fetch_data_package(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk | None:
        """Download and parse the EIDC data-package ratings file.

        The archive ships one ``<Station>ratings.csv`` per station with
        a ``Discharge (m3/s)`` column of in-situ measurements.  The
        archive is downloaded once and cached on the instance.
        """
        archive = await self._get_archive_bytes()
        if archive is None:
            return None

        stem = _RATINGS_FILE_STEMS.get(native_id)
        if stem is None:
            return None

        start_aware = (
            start if start.tzinfo else start.replace(tzinfo=UTC)
        )
        end_aware = (
            end if end.tzinfo else end.replace(tzinfo=UTC)
        )

        try:
            observations = self._parse_ratings_archive(
                archive, stem, station_id, start_aware, end_aware,
            )
        except (zipfile.BadZipFile, KeyError, OSError) as exc:
            logger.warning(
                "eidc_archive_parse_failed",
                provider=self.slug,
                station=native_id,
                error=str(exc),
            )
            return None

        if not observations:
            return None

        observations.sort(key=lambda o: o.timestamp)
        logger.info(
            "vietnam_mekong_data_package_loaded",
            station=native_id,
            count=len(observations),
        )
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    async def _get_archive_bytes(self) -> bytes | None:
        """Download the EIDC data-package zip once, caching the result."""
        if self._archive_bytes is not None:
            return self._archive_bytes
        if self._archive_attempted:
            return None
        self._archive_attempted = True
        try:
            resp = await self._get(EIDC_DATA_ZIP_URL)
        except Exception as exc:  # noqa: BLE001 - network resilience
            logger.warning(
                "eidc_archive_download_failed",
                provider=self.slug,
                url=EIDC_DATA_ZIP_URL,
                error=str(exc),
            )
            return None
        self._archive_bytes = resp.content
        return self._archive_bytes

    def _parse_ratings_archive(
        self,
        archive: bytes,
        stem: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse the ``<stem>ratings.csv`` member of the EIDC archive.

        Columns: ``Month, Day, Year, Discharge (m3/s),
        Section Averaged SSC (mg/l), Sediment Flux (kg/s)``.
        """
        import csv

        observations: list[Observation] = []
        with zipfile.ZipFile(io.BytesIO(archive)) as zf:
            member = next(
                (
                    n for n in zf.namelist()
                    if n.lower().endswith(
                        f"{stem.lower()}ratings.csv",
                    )
                ),
                None,
            )
            if member is None:
                return observations
            with zf.open(member) as fh:
                reader = csv.reader(
                    io.TextIOWrapper(fh, encoding="utf-8"),
                )
                rows = list(reader)

        if len(rows) < 2:
            return observations

        header = [h.strip().lower() for h in rows[0]]
        try:
            m_col = header.index("month")
            d_col = header.index("day")
            y_col = header.index("year")
        except ValueError:
            return observations
        q_col = next(
            (
                i for i, h in enumerate(header)
                if h.startswith("discharge")
            ),
            None,
        )
        if q_col is None:
            return observations

        max_col = max(m_col, d_col, y_col, q_col)
        for row in rows[1:]:
            if len(row) <= max_col:
                continue
            ts = self._parse_mdy(
                row[m_col], row[d_col], row[y_col],
            )
            if ts is None or ts < start or ts > end:
                continue
            raw = row[q_col].strip()
            if not raw or raw.lower() in ("", "na", "nan", "-"):
                continue
            try:
                discharge = float(str(raw))
            except ValueError:
                continue
            observations.append(
                Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=QualityFlag.RAW,
                )
            )
        return observations

    @staticmethod
    def _parse_mdy(
        month: str, day: str, year: str,
    ) -> datetime | None:
        """Build a UTC timestamp from separate M/D/Y string fields."""
        try:
            return datetime(
                int(str(year).strip()),
                int(str(month).strip()),
                int(str(day).strip()),
                tzinfo=UTC,
            )
        except (ValueError, TypeError):
            return None

    # ------------------------------------------------------------------
    # EIDC catalogue fetch
    # ------------------------------------------------------------------

    async def _try_fetch_eidc(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk | None:
        """Try fetching from EIDC catalogue API."""
        try:
            resp = await self._get(
                f"/documents/{EIDC_CATALOGUE_DOC}",
                params={
                    "station": native_id,
                    "format": "json",
                },
            )
            data = resp.json()
            return self._parse_eidc_response(
                data, station_id, start, end,
            )
        except (ConnectorError, Exception) as exc:
            logger.warning(
                "eidc_catalogue_failed",
                provider=self.slug,
                station=native_id,
                error=str(exc),
            )
            return None

    def _parse_eidc_response(
        self,
        data: dict | list,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk | None:
        """Parse EIDC JSON response into observations."""
        obs_list: list[dict] = []
        if isinstance(data, dict):
            obs_list = (
                data.get("data")
                or data.get("observations")
                or data.get("results", [])
            )
        elif isinstance(data, list):
            obs_list = data
        else:
            return None

        if not isinstance(obs_list, list):
            return None

        start_aware = (
            start if start.tzinfo else start.replace(tzinfo=UTC)
        )
        end_aware = (
            end if end.tzinfo else end.replace(tzinfo=UTC)
        )

        observations = self._parse_obs_entries(
            obs_list, station_id, start_aware, end_aware,
        )
        if not observations:
            return None

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    def _parse_obs_entries(
        self,
        entries: list[dict],
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse observation entries from API response."""
        observations: list[Observation] = []
        for entry in entries:
            try:
                ts = self._parse_timestamp(entry)
                if ts is None:
                    continue

                ts_aware = (
                    ts if ts.tzinfo
                    else ts.replace(tzinfo=UTC)
                )
                if ts_aware < start or ts_aware > end:
                    continue

                value = (
                    entry.get("discharge_m3s")
                    or entry.get("discharge")
                    or entry.get("value")
                )
                discharge = (
                    float(str(value))
                    if value is not None
                    else None
                )

                observations.append(
                    Observation(
                        station_id=station_id,
                        timestamp=ts_aware,
                        discharge_m3s=discharge,
                        quality=(
                            QualityFlag.RAW
                            if discharge is not None
                            else QualityFlag.MISSING
                        ),
                    )
                )
            except (ValueError, TypeError) as exc:
                logger.warning(
                    "observation_parse_failed",
                    provider=self.slug,
                    entry=str(entry)[:200],
                    error=str(exc),
                )
                continue
        return observations

    # ------------------------------------------------------------------
    # Local CSV fallback
    # ------------------------------------------------------------------

    def _try_fetch_local_csv(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk | None:
        """Try reading observations from local EIDC CSV files.

        Expected CSV columns: datetime, discharge_m3s, sediment.
        Filenames may contain the station name.
        """
        data_dir = self.config.get("data_dir")
        if not data_dir:
            return None

        data_path = Path(data_dir)
        if not data_path.is_dir():
            return None

        csv_files = list(data_path.glob("*.csv"))
        if not csv_files:
            return None

        start_aware = (
            start if start.tzinfo else start.replace(tzinfo=UTC)
        )
        end_aware = (
            end if end.tzinfo else end.replace(tzinfo=UTC)
        )

        observations: list[Observation] = []
        for csv_file in csv_files:
            obs = self._parse_eidc_csv(
                csv_file, native_id, station_id,
                start_aware, end_aware,
            )
            observations.extend(obs)

        if not observations:
            return None

        observations.sort(key=lambda o: o.timestamp)

        logger.info(
            "vietnam_mekong_csv_loaded",
            station=native_id,
            count=len(observations),
        )

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    def _parse_eidc_csv(
        self,
        file_path: Path,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse an EIDC CSV file for a specific station."""
        observations: list[Observation] = []

        # Check if filename contains station name
        file_lower = file_path.stem.lower().replace(
            "-", "_",
        ).replace(" ", "_")
        native_lower = native_id.lower()
        # Accept if filename contains station ID or is generic
        has_station_in_name = native_lower in file_lower
        is_generic = not any(
            s["native_id"] in file_lower
            for s in _SEED_STATIONS
        )
        if not has_station_in_name and not is_generic:
            return observations

        try:
            lines = file_path.read_text(
                encoding="utf-8",
            ).splitlines()
        except OSError as exc:
            raise ConnectorError(
                self.slug,
                f"Cannot read CSV file {file_path}: {exc}",
            ) from exc

        if not lines:
            return observations

        header = lines[0].strip().split(",")
        dt_col = self._find_datetime_column(header)
        discharge_col = self._find_discharge_column(header)
        station_col = self._find_station_column(header)

        if dt_col is None or discharge_col is None:
            return observations

        for line in lines[1:]:
            parts = line.strip().split(",")
            max_col = max(
                dt_col, discharge_col,
                station_col if station_col is not None else 0,
            )
            if len(parts) <= max_col:
                continue

            # Filter by station column if present
            if station_col is not None:
                row_station = parts[station_col].strip().lower()
                row_station_norm = row_station.replace(
                    " ", "_",
                ).replace("-", "_")
                if (
                    native_lower not in row_station_norm
                    and row_station_norm not in native_lower
                ):
                    continue

            obs = self._parse_csv_row(
                parts, dt_col, discharge_col,
                station_id, start, end,
            )
            if obs is not None:
                observations.append(obs)

        return observations

    def _parse_csv_row(
        self,
        parts: list[str],
        dt_col: int,
        discharge_col: int,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> Observation | None:
        """Parse a single CSV data row."""
        dt_str = parts[dt_col].strip()
        ts = self._parse_datetime(dt_str)
        if ts is None:
            return None

        if ts < start or ts > end:
            return None

        val_str = parts[discharge_col].strip()
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

        return Observation(
            station_id=station_id,
            timestamp=ts,
            discharge_m3s=discharge,
            quality=quality,
        )

    # ------------------------------------------------------------------
    # Column finding helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _find_datetime_column(header: list[str]) -> int | None:
        """Find the datetime column index."""
        for i, col in enumerate(header):
            col_lower = col.strip().lower()
            if col_lower in (
                "datetime", "date_time", "timestamp",
                "date", "time",
            ):
                return i
        return None

    @staticmethod
    def _find_discharge_column(header: list[str]) -> int | None:
        """Find the discharge column index."""
        for i, col in enumerate(header):
            col_lower = col.strip().lower()
            if col_lower in (
                "discharge_m3s", "discharge", "q_m3s",
                "flow", "value",
            ):
                return i
        return None

    @staticmethod
    def _find_station_column(header: list[str]) -> int | None:
        """Find the station column index."""
        for i, col in enumerate(header):
            col_lower = col.strip().lower()
            if col_lower in ("station", "station_id", "site"):
                return i
        return None

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_timestamp(entry: dict) -> datetime | None:
        """Parse a timestamp from various field names and formats."""
        raw = (
            entry.get("datetime")
            or entry.get("date")
            or entry.get("timestamp")
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

        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%d/%m/%Y %H:%M",
        ):
            try:
                return datetime.strptime(raw_str, fmt)
            except ValueError:
                continue

        return None

    @staticmethod
    def _parse_datetime(dt_str: str) -> datetime | None:
        """Try multiple datetime formats for CSV data."""
        for fmt in (
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%d/%m/%Y %H:%M:%S",
            "%d/%m/%Y %H:%M",
            "%d/%m/%Y",
        ):
            try:
                return datetime.strptime(
                    dt_str, fmt,
                ).replace(tzinfo=UTC)
            except ValueError:
                continue
        return None

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _empty_chunk(self, station_id: str) -> TimeSeriesChunk:
        """Return an empty TimeSeriesChunk for a station."""
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )
