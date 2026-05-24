"""Panama STRI (Smithsonian Tropical Research Institute) connector.

STRI provides Panama Canal watershed discharge data collected by the
ACP (Panama Canal Authority).  Data is distributed as a ZIP archive
containing 15-minute river discharge measurements from stations in
the Canal Zone.

This connector supports two modes:

1. **Station catalogue** -- a curated seed list of ~10 stations in
   the Canal watershed (Chagres River, Gatun Lake tributaries, etc.)
   with approximate Canal Zone coordinates.

2. **Observations** -- the connector attempts to download the ZIP
   archive from the STRI physical monitoring portal.  If download
   fails or no data is available, it falls back to reading
   pre-downloaded CSV files from ``config["data_dir"]``.

Base URL: https://biogeodb.stri.si.edu/physical_monitoring
ZIP file:  /downloads/acp_discharge_15min.zip

References
----------
- STRI Physical Monitoring: https://biogeodb.stri.si.edu/physical_monitoring
- Panama Canal Authority (ACP): https://pancanal.com
"""

from __future__ import annotations

import csv
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

_ZIP_PATH = "/downloads/acp_discharge_15min.zip"
_STRI_DOWNLOAD_URL = (
    "https://biogeodb.stri.si.edu/physical_monitoring"
    "/downloads/acp_discharge_15min.zip"
)

# ---------------------------------------------------------------------------
# Curated seed catalogue -- major ACP discharge stations in the
# Panama Canal watershed.  Coordinates are approximate for the Canal
# Zone (lat ~9.1-9.4, lon ~-79.5 to -80.0).
# ---------------------------------------------------------------------------

_SEED_STATIONS: list[dict] = [
    {
        "native_id": "CHA",
        "name": "Chagres River at Chico",
        "lat": 9.21,
        "lon": -79.57,
        "river": "Chagres",
        "area": 414.0,
    },
    {
        "native_id": "CHR",
        "name": "Chagres River at Ciri Grande",
        "lat": 9.25,
        "lon": -79.63,
        "river": "Chagres",
        "area": 985.0,
    },
    {
        "native_id": "GAT",
        "name": "Gatun at Gatun Dam",
        "lat": 9.28,
        "lon": -79.92,
        "river": "Gatun",
        "area": 3338.0,
    },
    {
        "native_id": "PEQ",
        "name": "Pequeni River at Pueblo Viejo",
        "lat": 9.27,
        "lon": -79.55,
        "river": "Pequeni",
        "area": 135.0,
    },
    {
        "native_id": "BOQ",
        "name": "Boqueron River at Peluca",
        "lat": 9.22,
        "lon": -79.60,
        "river": "Boqueron",
        "area": 175.0,
    },
    {
        "native_id": "CND",
        "name": "Chilibre at Chilibrillo Dam",
        "lat": 9.17,
        "lon": -79.62,
        "river": "Chilibre",
        "area": 103.0,
    },
    {
        "native_id": "ALH",
        "name": "Alajuela at Madden Dam",
        "lat": 9.22,
        "lon": -79.62,
        "river": "Chagres",
        "area": 1026.0,
    },
    {
        "native_id": "TRI",
        "name": "Trinidad River at Gamboa",
        "lat": 9.12,
        "lon": -79.70,
        "river": "Trinidad",
        "area": 88.0,
    },
    {
        "native_id": "CRS",
        "name": "Cirí Grande River at Los Cañones",
        "lat": 9.10,
        "lon": -79.80,
        "river": "Cirí Grande",
        "area": 240.0,
    },
    {
        "native_id": "GCQ",
        "name": "Gatuncillo River at Nuevo Limón",
        "lat": 9.30,
        "lon": -79.85,
        "river": "Gatuncillo",
        "area": 78.0,
    },
]


def _safe_float(value: object) -> float | None:
    """Safely convert a value to float."""
    if value is None:
        return None
    try:
        return float(str(value))
    except (ValueError, TypeError):
        return None


@register("panama_stri")
class PanamaSTRIConnector(BaseConnector):
    """STRI connector -- Canal watershed discharge from ACP stations.

    Configuration options (via ``config`` dict):
        data_dir : str | Path
            Directory containing pre-downloaded ACP CSV files or
            the extracted ZIP archive contents.
        seed_only : bool
            If True (default), return the curated seed catalogue
            without attempting a network fetch for station discovery.
    """

    slug = "panama_stri"
    display_name = "STRI Panama Canal Watershed (ACP)"
    base_url = "https://biogeodb.stri.si.edu/physical_monitoring"
    country_codes: list[str] = ["PA"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return Panama Canal watershed stations from the seed list.

        By default, the curated seed list is returned (fast, no
        network).  Set ``config["seed_only"] = False`` to attempt
        discovery from the STRI portal (falls back to seed on error).
        """
        seed_only = self.config.get("seed_only", True)

        if not seed_only:
            try:
                return await self._fetch_stations_remote()
            except Exception as exc:
                logger.warning(
                    "panama_stri_remote_fallback_to_seed",
                    error=str(exc),
                )

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
        """Fetch observations for a station from ZIP download or local files.

        Strategy:
        1. Check ``config["data_dir"]`` for pre-downloaded CSV files.
        2. If no local files, attempt to download the ZIP archive from
           the STRI portal, extract CSV data, and parse observations.
        3. If download fails, return empty chunk with guidance.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        # Try local files first
        data_dir = self.config.get("data_dir")
        if data_dir:
            data_path = Path(data_dir)
            file_path = self._find_data_file(data_path, native_id)
            if file_path is not None:
                return self._load_csv_file(
                    file_path, station_id, native_id, start, end,
                )

        # Try downloading ZIP from STRI portal
        try:
            return await self._fetch_from_zip(
                station_id, native_id, start, end,
            )
        except Exception as exc:
            logger.info(
                "panama_stri_download_failed",
                station=native_id,
                error=str(exc),
                hint=(
                    "Download the ACP discharge ZIP manually from "
                    f"{_STRI_DOWNLOAD_URL} and set "
                    "config['data_dir'] to the extracted directory."
                ),
            )

        return self._empty_chunk(station_id)

    # ------------------------------------------------------------------
    # Remote station discovery
    # ------------------------------------------------------------------

    async def _fetch_stations_remote(self) -> list[Station]:
        """Attempt to discover stations from the STRI portal.

        Fetches the ZIP file header to verify availability, then
        returns the seed list (the ZIP does not contain station
        metadata beyond what is in the data rows).
        """
        resp = await self._get(_ZIP_PATH)
        if resp.status_code != 200:
            raise ConnectorError(
                self.slug,
                f"STRI ZIP download returned {resp.status_code}",
            )
        return self._build_seed_stations()

    # ------------------------------------------------------------------
    # ZIP download and parsing
    # ------------------------------------------------------------------

    async def _fetch_from_zip(
        self,
        station_id: str,
        native_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Download ZIP from STRI and extract observations."""
        resp = await self._get(_ZIP_PATH)
        content = resp.content

        start_aware = (
            start if start.tzinfo else start.replace(tzinfo=UTC)
        )
        end_aware = (
            end if end.tzinfo else end.replace(tzinfo=UTC)
        )

        observations = self._parse_zip_content(
            content, station_id, native_id, start_aware, end_aware,
        )

        logger.info(
            "panama_stri_zip_observations_loaded",
            station=native_id,
            count=len(observations),
        )

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    def _parse_zip_content(
        self,
        content: bytes,
        station_id: str,
        native_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse CSV files within the ZIP for a specific station."""
        observations: list[Observation] = []

        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                for name in zf.namelist():
                    if not name.lower().endswith(".csv"):
                        continue
                    csv_bytes = zf.read(name)
                    csv_text = csv_bytes.decode("utf-8", errors="replace")
                    obs = self._parse_csv_text(
                        csv_text, station_id, native_id, start, end,
                    )
                    observations.extend(obs)
        except zipfile.BadZipFile as exc:
            raise ConnectorError(
                self.slug,
                f"Invalid ZIP file from STRI: {exc}",
            ) from exc

        return observations

    # ------------------------------------------------------------------
    # Local file handling
    # ------------------------------------------------------------------

    def _find_data_file(
        self, data_dir: Path, native_id: str,
    ) -> Path | None:
        """Locate a CSV file for a given station.

        Common naming patterns:
          {native_id}.csv
          acp_discharge_{native_id}.csv
          acp_discharge_15min.csv (all stations in one file)
        """
        candidates = [
            data_dir / f"{native_id}.csv",
            data_dir / f"acp_discharge_{native_id}.csv",
            data_dir / "acp_discharge_15min.csv",
        ]
        for candidate in candidates:
            if candidate.is_file():
                return candidate
        return None

    def _load_csv_file(
        self,
        file_path: Path,
        station_id: str,
        native_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Load and parse a local CSV file."""
        try:
            text = file_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ConnectorError(
                self.slug,
                f"Cannot read file {file_path}: {exc}",
            ) from exc

        start_aware = (
            start if start.tzinfo else start.replace(tzinfo=UTC)
        )
        end_aware = (
            end if end.tzinfo else end.replace(tzinfo=UTC)
        )

        observations = self._parse_csv_text(
            text, station_id, native_id, start_aware, end_aware,
        )

        logger.info(
            "panama_stri_observations_loaded",
            station=native_id,
            count=len(observations),
            file=str(file_path),
        )

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # ------------------------------------------------------------------
    # CSV parsing
    # ------------------------------------------------------------------

    def _parse_csv_text(
        self,
        text: str,
        station_id: str,
        native_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse CSV text with datetime, station_id, discharge columns.

        Expected columns: datetime (or date/time), station_id,
        discharge (m3/s).  Rows not matching the requested station
        are skipped.
        """
        observations: list[Observation] = []
        reader = csv.DictReader(io.StringIO(text))

        if reader.fieldnames is None:
            return observations

        field_map = {
            f.lower().strip(): f for f in reader.fieldnames
        }

        datetime_col = (
            field_map.get("datetime")
            or field_map.get("date")
            or field_map.get("time")
        )
        station_col = field_map.get("station_id") or field_map.get(
            "station",
        )
        value_col = (
            field_map.get("discharge")
            or field_map.get("discharge_m3s")
            or field_map.get("streamflow")
            or field_map.get("value")
        )

        if not datetime_col or not value_col:
            return observations

        for row in reader:
            obs = self._parse_row(
                row, datetime_col, station_col, value_col,
                station_id, native_id, start, end,
            )
            if obs is not None:
                observations.append(obs)

        return observations

    def _parse_row(
        self,
        row: dict[str, str],
        datetime_col: str,
        station_col: str | None,
        value_col: str,
        station_id: str,
        native_id: str,
        start: datetime,
        end: datetime,
    ) -> Observation | None:
        """Parse a single CSV row into an Observation."""
        # Filter by station if the CSV has a station column
        if station_col:
            row_station = row.get(station_col, "").strip()
            if row_station and row_station != native_id:
                return None

        date_str = row.get(datetime_col, "").strip()
        value_str = row.get(value_col, "").strip()

        if not date_str:
            return None

        ts = self._parse_timestamp(date_str)
        if ts is None:
            return None

        if ts < start or ts > end:
            return None

        discharge: float | None = None
        quality = QualityFlag.RAW

        if value_str:
            parsed = _safe_float(value_str)
            if parsed is not None:
                discharge = parsed
            else:
                quality = QualityFlag.MISSING

        return Observation(
            station_id=station_id,
            timestamp=ts,
            discharge_m3s=discharge,
            quality=quality,
        )

    @staticmethod
    def _parse_timestamp(date_str: str) -> datetime | None:
        """Parse a timestamp string, trying multiple formats."""
        formats = [
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%d %H:%M",
            "%Y-%m-%d",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d %H:%M",
        ]
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).replace(
                    tzinfo=UTC,
                )
            except ValueError:
                continue
        return None

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
                    country_code="PA",
                    river=entry.get("river"),
                    catchment_area_km2=(
                        float(str(entry["area"]))
                        if entry.get("area") is not None
                        else None
                    ),
                )
            )
        return stations

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
