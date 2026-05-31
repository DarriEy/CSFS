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
from csfs.core.downloads import ensure_dataset
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
    {"id": "il_60190", "name": "Gauge 60190 (Eilat region)", "lat": 29.5263, "lon": 34.9138, "area": 14.8},
    {"id": "il_60105", "name": "Gauge 60105 (Arava)", "lat": 30.0890, "lon": 35.1145, "area": 22.1},
    {"id": "il_57180", "name": "Gauge 57180 (Central Negev)", "lat": 30.3113, "lon": 35.0105, "area": 166.5},
    {"id": "il_56140", "name": "Gauge 56140 (Northern Negev)", "lat": 30.6138, "lon": 34.8589, "area": 111.0},
    {"id": "il_23103", "name": "Gauge 23103 (Beersheba)", "lat": 30.9081, "lon": 34.8514, "area": 17.0},
    {"id": "il_48192", "name": "Gauge 48192 (Dead Sea basin)", "lat": 31.1346, "lon": 35.3494, "area": 359.1},
    {"id": "il_23150", "name": "Gauge 23150 (Western Negev)", "lat": 31.3856, "lon": 34.4502, "area": 2588.5},
    {"id": "il_48130", "name": "Gauge 48130 (Judean Desert)", "lat": 31.5868, "lon": 35.3524, "area": 139.0},
    {"id": "il_19178", "name": "Gauge 19178 (Shfela)", "lat": 31.7818, "lon": 34.6834, "area": 432.5},
    {"id": "il_18150", "name": "Gauge 18150 (Coastal plain south)", "lat": 31.8700, "lon": 34.7524, "area": 615.8},
    {"id": "il_17162", "name": "Gauge 17162 (Tel Aviv area)", "lat": 32.0087, "lon": 34.9165, "area": 90.6},
    {"id": "il_17110", "name": "Gauge 17110 (Sharon)", "lat": 32.1475, "lon": 34.9622, "area": 238.8},
    {"id": "il_14120", "name": "Gauge 14120 (Hadera)", "lat": 32.4402, "lon": 34.9555, "area": 578.9},
    {"id": "il_38175", "name": "Gauge 38175 (Jezreel Valley)", "lat": 32.5065, "lon": 35.5185, "area": 181.6},
    {"id": "il_12140", "name": "Gauge 12140 (Carmel coast)", "lat": 32.5929, "lon": 34.9477, "area": 68.6},
    {"id": "il_8146", "name": "Gauge 8146 (Haifa region)", "lat": 32.7214, "lon": 35.0974, "area": 694.9},
    {"id": "il_31163", "name": "Gauge 31163 (Sea of Galilee)", "lat": 32.8896, "lon": 35.6595, "area": 108.7},
    {"id": "il_4110", "name": "Gauge 4110 (Western Galilee)", "lat": 32.9999, "lon": 35.1306, "area": 37.4},
    {"id": "il_30155", "name": "Gauge 30155 (Upper Galilee)", "lat": 33.1485, "lon": 35.6401, "area": 44.1},
    {"id": "il_30120", "name": "Gauge 30120 (Upper Jordan)", "lat": 33.2386, "lon": 35.6244, "area": 608.7},
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

        Standard Caravan format: ``date, streamflow`` columns. The
        Caravan-Israel extension is auto-downloaded and cached on first use
        (see :func:`csfs.core.downloads.ensure_dataset`); set
        ``config['data_dir']`` to use a pre-downloaded copy, or
        ``config['auto_download'] = False`` to disable. If the data is
        unavailable, returns an empty chunk.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = await ensure_dataset(self.slug, self.config)

        if data_dir is None:
            logger.info(
                "israel_caravan_no_data_dir",
                station=native_id,
                hint=(
                    "Caravan-Israel data unavailable (auto-download disabled "
                    f"or failed). Download from {_ZENODO_DOWNLOAD_URL}"
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
            data_dir / "timeseries" / "csv" / "il" / f"{basin_id}.csv",
        ]
        for candidate in candidates:
            if candidate.is_file():
                return candidate
        # The auto-downloaded extension extracts to a nested Caravan tree
        # (timeseries/csv/il/<id>.csv); search recursively as a fallback.
        match = next(
            (p for p in data_dir.rglob(f"{basin_id}.csv") if p.is_file()),
            None,
        )
        return match

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
