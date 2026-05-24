"""Israel Caravan Extension connector (Zenodo record 15003600).

A Caravan-format extension for Israel containing 95 catchments with
daily discharge data.  Data is published on Zenodo and follows the
standard Caravan CSV format (date, streamflow columns).

This connector supports two modes:

1. **Station catalogue** -- a curated seed list of ~20 major Israeli
   catchments (Jordan River tributaries, Yarkon, Kishon, Ayalon,
   Be'er Sheva area wadis) with approximate coordinates.

2. **Observations from local files** -- reads Caravan-format CSV
   files from ``config["data_dir"]``.  Standard Caravan layout is
   expected (``{basin_id}.csv`` or ``timeseries/csv/{basin_id}.csv``).
   If no files are found, logs Zenodo download instructions.

References
----------
- Zenodo record: https://zenodo.org/records/15003600
- Caravan format: Kratzert et al. (2023)
"""

from __future__ import annotations

import csv
import io
from datetime import UTC, datetime
from pathlib import Path

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

# Zenodo record for Caravan-Israel
_ZENODO_RECORD_ID = "15003600"
_ZENODO_DOWNLOAD_URL = (
    f"https://zenodo.org/records/{_ZENODO_RECORD_ID}"
)

# ---------------------------------------------------------------------------
# Curated seed catalogue -- ~20 major Israeli catchments
# Coordinates are approximate centroids for each catchment.
# ---------------------------------------------------------------------------

_SEED_STATIONS: list[dict] = [
    # Jordan River basin
    {
        "id": "il_jordan_hazbani",
        "name": "Hazbani (Snir) at Dan",
        "lat": 33.22,
        "lon": 35.65,
        "river": "Hazbani",
        "area": 161.0,
    },
    {
        "id": "il_jordan_dan",
        "name": "Dan Spring at Tel Dan",
        "lat": 33.25,
        "lon": 35.65,
        "river": "Dan",
        "area": 52.0,
    },
    {
        "id": "il_jordan_banias",
        "name": "Banias at Banias Nature Reserve",
        "lat": 33.25,
        "lon": 35.69,
        "river": "Banias",
        "area": 122.0,
    },
    {
        "id": "il_jordan_upper",
        "name": "Upper Jordan at Huri Bridge",
        "lat": 33.07,
        "lon": 35.62,
        "river": "Jordan",
        "area": 820.0,
    },
    {
        "id": "il_jordan_yarmuk",
        "name": "Yarmouk at Naharayim",
        "lat": 32.65,
        "lon": 35.57,
        "river": "Yarmouk",
        "area": 6805.0,
    },
    # Coastal plain rivers
    {
        "id": "il_yarkon",
        "name": "Yarkon at Rosh HaAyin",
        "lat": 32.10,
        "lon": 34.95,
        "river": "Yarkon",
        "area": 1733.0,
    },
    {
        "id": "il_kishon",
        "name": "Kishon at Haifa Bay",
        "lat": 32.80,
        "lon": 35.08,
        "river": "Kishon",
        "area": 1100.0,
    },
    {
        "id": "il_ayalon",
        "name": "Ayalon at Highway 1 Bridge",
        "lat": 32.03,
        "lon": 34.82,
        "river": "Ayalon",
        "area": 782.0,
    },
    {
        "id": "il_alexander",
        "name": "Alexander at Emet Junction",
        "lat": 32.38,
        "lon": 34.90,
        "river": "Alexander",
        "area": 545.0,
    },
    {
        "id": "il_hadera",
        "name": "Hadera at Highway 4",
        "lat": 32.45,
        "lon": 34.92,
        "river": "Hadera",
        "area": 360.0,
    },
    {
        "id": "il_taninim",
        "name": "Taninim at Jisr az-Zarqa",
        "lat": 32.53,
        "lon": 34.90,
        "river": "Taninim",
        "area": 435.0,
    },
    # Negev wadis
    {
        "id": "il_beersheva_besor",
        "name": "Nahal Besor at Urim",
        "lat": 31.33,
        "lon": 34.42,
        "river": "Besor",
        "area": 3625.0,
    },
    {
        "id": "il_beersheva_arad",
        "name": "Nahal Arad near Arad",
        "lat": 31.26,
        "lon": 35.22,
        "river": "Arad",
        "area": 210.0,
    },
    {
        "id": "il_lachish",
        "name": "Nahal Lachish at Kiryat Gat",
        "lat": 31.60,
        "lon": 34.78,
        "river": "Lachish",
        "area": 980.0,
    },
    {
        "id": "il_shikma",
        "name": "Nahal Shikma at Highway 35",
        "lat": 31.58,
        "lon": 34.55,
        "river": "Shikma",
        "area": 710.0,
    },
    # Northern rivers
    {
        "id": "il_naaman",
        "name": "Nahal Naaman at Akko",
        "lat": 32.92,
        "lon": 35.08,
        "river": "Naaman",
        "area": 240.0,
    },
    {
        "id": "il_gaaton",
        "name": "Nahal Gaaton at Nahariya",
        "lat": 33.00,
        "lon": 35.10,
        "river": "Gaaton",
        "area": 72.0,
    },
    # Sea of Galilee basin
    {
        "id": "il_meshushim",
        "name": "Nahal Meshushim at Golan",
        "lat": 32.85,
        "lon": 35.75,
        "river": "Meshushim",
        "area": 160.0,
    },
    {
        "id": "il_daliyot",
        "name": "Nahal Daliyot at Kinneret",
        "lat": 32.80,
        "lon": 35.58,
        "river": "Daliyot",
        "area": 52.0,
    },
    {
        "id": "il_amud",
        "name": "Nahal Amud at Tabgha",
        "lat": 32.87,
        "lon": 35.53,
        "river": "Amud",
        "area": 225.0,
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


@register("israel_caravan")
class IsraelCaravanConnector(BaseConnector):
    """Connector for the Caravan-Israel extension on Zenodo.

    Configuration options (via ``config`` dict):
        data_dir : str | Path
            Directory containing Caravan-Israel CSV files.
            Expected layout: ``{basin_id}.csv`` or
            ``timeseries/csv/{basin_id}.csv``.
        seed_only : bool
            If True (default), return the curated seed catalogue.
            If False, attempt to query Zenodo for record metadata.
    """

    slug = "israel_caravan"
    display_name = "Caravan-Israel Extension (Zenodo)"
    base_url = "https://zenodo.org/api"
    country_codes: list[str] = ["IL"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return Israeli catchment stations from seed or Zenodo.

        By default, the curated seed list is returned (fast, no
        network).  Set ``config["seed_only"] = False`` to attempt
        Zenodo metadata discovery (falls back to seed on error).
        """
        seed_only = self.config.get("seed_only", True)

        if not seed_only:
            try:
                return await self._fetch_stations_zenodo()
            except Exception as exc:
                logger.warning(
                    "israel_caravan_zenodo_fallback_to_seed",
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
        """Read observations from local Caravan-Israel CSV files.

        Standard Caravan format: ``date, streamflow`` columns.
        If no local data directory is configured or the file does
        not exist, logs Zenodo download instructions and returns
        an empty chunk.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = self.config.get("data_dir")

        if not data_dir:
            logger.info(
                "israel_caravan_no_data_dir",
                station=native_id,
                hint=(
                    "Set config['data_dir'] to a directory containing "
                    "Caravan-Israel CSV files. Download from "
                    f"{_ZENODO_DOWNLOAD_URL}"
                ),
            )
            return self._empty_chunk(station_id)

        data_path = Path(data_dir)
        file_path = self._find_data_file(data_path, native_id)

        if file_path is None:
            logger.info(
                "israel_caravan_file_not_found",
                station=native_id,
                data_dir=str(data_path),
                hint=(
                    f"Download Caravan-Israel CSV for basin "
                    f"{native_id} from {_ZENODO_DOWNLOAD_URL}"
                ),
            )
            return self._empty_chunk(station_id)

        start_aware = (
            start if start.tzinfo else start.replace(tzinfo=UTC)
        )
        end_aware = (
            end if end.tzinfo else end.replace(tzinfo=UTC)
        )

        observations = self._parse_csv_file(
            file_path, station_id, start_aware, end_aware,
        )

        logger.info(
            "israel_caravan_observations_loaded",
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
    # Zenodo metadata discovery
    # ------------------------------------------------------------------

    async def _fetch_stations_zenodo(self) -> list[Station]:
        """Query Zenodo API for record metadata and file list.

        Zenodo metadata doesn't provide per-station info, so after
        verifying the record exists and has files, we fall back to
        the seed list.
        """
        try:
            resp = await self._get(
                f"/records/{_ZENODO_RECORD_ID}",
            )
            data = resp.json()
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch Zenodo record "
                f"{_ZENODO_RECORD_ID}: {exc}",
            ) from exc

        files = data.get("files", [])
        if not files:
            raise DataFormatError(
                self.slug,
                f"Zenodo record {_ZENODO_RECORD_ID} has no files",
            )

        logger.info(
            "israel_caravan_zenodo_files_discovered",
            provider=self.slug,
            file_count=len(files),
        )

        return self._build_seed_stations()

    # ------------------------------------------------------------------
    # Seed catalogue
    # ------------------------------------------------------------------

    def _build_seed_stations(self) -> list[Station]:
        """Build Station objects from the curated seed list."""
        stations: list[Station] = []
        for entry in _SEED_STATIONS:
            stations.append(
                Station(
                    id=self._station_id(entry["id"]),
                    provider=self.slug,
                    native_id=entry["id"],
                    name=entry["name"],
                    latitude=float(str(entry["lat"])),
                    longitude=float(str(entry["lon"])),
                    country_code="IL",
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
    # Local file parsing
    # ------------------------------------------------------------------

    def _find_data_file(
        self, data_dir: Path, basin_id: str,
    ) -> Path | None:
        """Locate Caravan-Israel CSV file for a given basin.

        Searches in multiple locations matching Caravan layout:
          {data_dir}/{basin_id}.csv
          {data_dir}/timeseries/csv/{basin_id}.csv
        """
        candidates = [
            data_dir / f"{basin_id}.csv",
            data_dir / "timeseries" / "csv" / f"{basin_id}.csv",
        ]
        for candidate in candidates:
            if candidate.is_file():
                return candidate
        return None

    def _parse_csv_file(
        self,
        file_path: Path,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse a Caravan-format CSV with date and streamflow columns.

        Expected columns: date, streamflow (or discharge).
        """
        observations: list[Observation] = []

        try:
            text = file_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ConnectorError(
                self.slug,
                f"Cannot read file {file_path}: {exc}",
            ) from exc

        reader = csv.DictReader(io.StringIO(text))
        if reader.fieldnames is None:
            return observations

        field_map = {
            f.lower().strip(): f for f in reader.fieldnames
        }

        date_col = field_map.get("date") or field_map.get("time")
        value_col = (
            field_map.get("streamflow")
            or field_map.get("discharge")
            or field_map.get("streamflow_mmd")
            or field_map.get("discharge_m3s")
        )

        if not date_col or not value_col:
            return observations

        for row in reader:
            obs = self._parse_csv_row(
                row, date_col, value_col, station_id, start, end,
            )
            if obs is not None:
                observations.append(obs)

        return observations

    def _parse_csv_row(
        self,
        row: dict[str, str],
        date_col: str,
        value_col: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> Observation | None:
        """Parse a single CSV row into an Observation."""
        date_str = row.get(date_col, "").strip()
        value_str = row.get(value_col, "").strip()

        if not date_str:
            return None

        try:
            ts = datetime.strptime(date_str, "%Y-%m-%d").replace(
                tzinfo=UTC,
            )
        except ValueError:
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
