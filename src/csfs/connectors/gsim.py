"""GSIM (Global Streamflow Indices and Metadata) connector -- PANGAEA.

GSIM provides streamflow indices (NOT raw daily discharge) for 30,959
stations globally, covering approximately 1950-2016.  Data is archived
on PANGAEA (DOI: 10.1594/PANGAEA.887477).

Important: GSIM distributes pre-computed indices such as mean annual
flow, monthly statistics, and seasonal patterns.  It does NOT provide
raw daily discharge time series.  The ``fetch_observations`` method
parses these index files and maps the available indices into the
``Observation`` model, using the discharge field to store the index
value.

This connector supports two modes:

1. **Station catalogue** -- a curated seed list of major stations from
   the GSIM catalogue, with coordinates and metadata embedded in the
   connector.

2. **Indices from local files** -- GSIM text files downloaded from
   PANGAEA are read from ``config["data_dir"]``.  Files follow the
   naming pattern ``{station_id}.mon`` (monthly) or
   ``{station_id}.year`` (yearly).

References
----------
- DOI: 10.1594/PANGAEA.887477
- Paper: Gudmundsson et al. (2018) – Global Streamflow Indices
"""

from __future__ import annotations

import csv
import io
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

# PANGAEA dataset identifier
_PANGAEA_DOI = "10.1594/PANGAEA.887477"
_PANGAEA_DOWNLOAD_URL = (
    "https://doi.pangaea.de/10.1594/PANGAEA.887477"
)

# Missing-value sentinel used in GSIM files
_MISSING_VALUE = -999.0

# ---------------------------------------------------------------------------
# Curated seed catalogue of major GSIM stations
# ---------------------------------------------------------------------------
# Representative global subset from the GSIM catalogue (30,959 total).
# Coordinates from Gudmundsson et al. (2018).
# ---------------------------------------------------------------------------

_SEED_STATIONS: list[dict] = [
    {"id": "BR_0000243", "name": "Amazon at Obidos", "lat": -1.95, "lon": -55.51, "country": "BR", "river": "Amazon", "area": 4670000.0},
    {"id": "CD_0000004", "name": "Congo at Kinshasa", "lat": -4.14, "lon": 15.33, "country": "CD", "river": "Congo", "area": 3747320.0},
    {"id": "RU_0000081", "name": "Ob at Salekhard", "lat": 66.57, "lon": 66.53, "country": "RU", "river": "Ob", "area": 2949998.0},
    {"id": "AR_0000006", "name": "Parana at Timbues", "lat": -32.67, "lon": -60.71, "country": "AR", "river": "Parana", "area": 2346000.0},
    {"id": "CN_0000180", "name": "Yangtze at Datong", "lat": 30.77, "lon": 117.62, "country": "CN", "river": "Yangtze", "area": 1705383.0},
    {"id": "CA_0006066", "name": "Mackenzie at Arctic Red River", "lat": 67.46, "lon": -133.75, "country": "CA", "river": "Mackenzie", "area": 1679100.0},
    {"id": "US_0005806", "name": "Mississippi at Vicksburg", "lat": 32.32, "lon": -90.91, "country": "US", "river": "Mississippi", "area": 1140500.0},
    {"id": "BJ_0000017", "name": "Niger at Malanville", "lat": 11.87, "lon": 3.38, "country": "BJ", "river": "Niger", "area": 1000000.0},
    {"id": "MZ_0000002", "name": "Zambezi at Matundo-Cais", "lat": -16.15, "lon": 33.59, "country": "MZ", "river": "Zambezi", "area": 940000.0},
    {"id": "ZA_0000001", "name": "Orange at Vioolsdrif", "lat": -28.76, "lon": 17.72, "country": "ZA", "river": "Orange", "area": 866486.0},
    {"id": "RO_0000038", "name": "Danube at Ceatal Izmail", "lat": 45.22, "lon": 28.72, "country": "RO", "river": "Danube", "area": 807000.0},
    {"id": "KH_0000001", "name": "Mekong at Phnom Penh", "lat": 11.58, "lon": 104.94, "country": "KH", "river": "Mekong", "area": 663000.0},
    {"id": "BD_0000001", "name": "Brahmaputra at Bahadurabad", "lat": 25.18, "lon": 89.67, "country": "BD", "river": "Brahmaputra", "area": 636130.0},
    {"id": "MX_0000008", "name": "Colorado at Lindero Internacional", "lat": 32.72, "lon": -114.72, "country": "MX", "river": "Colorado", "area": 631960.0},
    {"id": "LA_0000014", "name": "Mekong at Pakse", "lat": 15.12, "lon": 105.80, "country": "LA", "river": "Mekong", "area": 545000.0},
    {"id": "DE_0000332", "name": "Elbe at Neu-Darchau", "lat": 53.23, "lon": 10.89, "country": "DE", "river": "Elbe", "area": 131950.0},
    {"id": "BG_0000005", "name": "Danube at Silistra", "lat": 44.13, "lon": 27.26, "country": "BG", "river": "Danube", "area": 689700.0},
    {"id": "VE_0000009", "name": "Orinoco at Puente Angostura", "lat": 8.15, "lon": -63.60, "country": "VE", "river": "Orinoco", "area": 836000.0},
    {"id": "ML_0000032", "name": "Niger at Ansongo", "lat": 15.67, "lon": 0.50, "country": "ML", "river": "Niger", "area": 566000.0},
    {"id": "TD_0000003", "name": "Chari at Ndjamena", "lat": 12.12, "lon": 15.03, "country": "TD", "river": "Chari", "area": 600000.0},
]


def _safe_float(value: object) -> float | None:
    """Safely convert a value to float."""
    if value is None:
        return None
    try:
        return float(str(value))
    except (ValueError, TypeError):
        return None


@register("gsim")
class GSIMConnector(BaseConnector):
    """Connector for GSIM (Global Streamflow Indices) on PANGAEA.

    Note: GSIM provides streamflow *indices* (e.g., mean annual flow,
    monthly statistics), NOT raw daily discharge.  The ``Observation``
    model's ``discharge_m3s`` field stores the index value.

    Configuration options (via ``config`` dict):
        data_dir : str | Path
            Directory containing GSIM text files
            (``{station_id}.mon``, ``{station_id}.year``,
            or CSV exports).
    """

    slug = "gsim"
    display_name = (
        "GSIM (Global Streamflow Indices and Metadata)"
    )
    base_url = "https://doi.pangaea.de"
    country_codes: list[str] = ["global"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return GSIM stations from the curated seed list.

        Optionally verifies the PANGAEA record is accessible.
        """
        if not self.config.get("seed_only", True):
            try:
                await self._verify_pangaea_record()
            except Exception as exc:
                logger.warning(
                    "gsim_pangaea_unreachable",
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
        """Read streamflow indices from local GSIM files.

        GSIM provides indices (mean, min, max, etc.), NOT raw daily
        discharge.  The ``discharge_m3s`` field stores the index
        value (typically mean monthly or annual flow in m3/s).

        If no data directory is configured or the file does not exist,
        logs guidance and returns an empty ``TimeSeriesChunk``.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = self.config.get("data_dir")

        if not data_dir:
            logger.info(
                "gsim_no_data_dir",
                station=native_id,
                hint=(
                    "Set config['data_dir'] to a directory containing "
                    "GSIM text files. Download from "
                    f"{_PANGAEA_DOWNLOAD_URL}"
                ),
            )
            return self._empty_chunk(station_id)

        data_path = Path(data_dir)
        file_path = self._find_data_file(data_path, native_id)

        if file_path is None:
            logger.info(
                "gsim_file_not_found",
                station=native_id,
                data_dir=str(data_path),
                hint=(
                    f"Download GSIM data for station {native_id} "
                    f"from {_PANGAEA_DOWNLOAD_URL}"
                ),
            )
            return self._empty_chunk(station_id)

        start_aware = (
            start if start.tzinfo else start.replace(tzinfo=UTC)
        )
        end_aware = (
            end if end.tzinfo else end.replace(tzinfo=UTC)
        )

        observations = self._parse_gsim_file(
            file_path, station_id, start_aware, end_aware,
        )

        logger.info(
            "gsim_indices_loaded",
            station=native_id,
            count=len(observations),
            file=str(file_path),
            note="Values are streamflow indices, not raw discharge",
        )

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # ------------------------------------------------------------------
    # PANGAEA verification
    # ------------------------------------------------------------------

    async def _verify_pangaea_record(self) -> None:
        """Verify the PANGAEA record is accessible."""
        resp = await self._get(f"/{_PANGAEA_DOI}")
        if resp.status_code not in (200, 301, 302):
            raise ConnectorError(
                self.slug,
                f"PANGAEA record {_PANGAEA_DOI} returned "
                f"status {resp.status_code}",
            )
        logger.info(
            "gsim_pangaea_verified",
            doi=_PANGAEA_DOI,
        )

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
                    country_code=entry["country"],
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
        self, data_dir: Path, station_id: str,
    ) -> Path | None:
        """Locate GSIM data file for a station.

        Common naming patterns:
          {station_id}.mon    -- monthly indices
          {station_id}.year   -- yearly indices
          {station_id}.csv    -- CSV export
        """
        candidates = [
            data_dir / f"{station_id}.mon",
            data_dir / f"{station_id}.year",
            data_dir / f"{station_id}.csv",
        ]
        # Also search inside GSIM_indices subdirectories
        for sub in ["GSIM_indices/TIMESERIES/monthly",
                     "GSIM_indices/TIMESERIES/yearly",
                     "TIMESERIES/monthly",
                     "TIMESERIES/yearly"]:
            candidates.append(data_dir / sub / f"{station_id}.mon")
            candidates.append(data_dir / sub / f"{station_id}.year")
        for candidate in candidates:
            if candidate.is_file():
                return candidate
        return None

    def _parse_gsim_file(
        self,
        file_path: Path,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse a GSIM index file.

        Supports two formats:
        1. GSIM text format: comment lines starting with '#',
           then tab/comma separated data with year, month, and
           index columns.
        2. CSV format with headers.

        The first numeric column after the date is treated as the
        index value (typically mean flow in m3/s).
        """
        try:
            text = file_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ConnectorError(
                self.slug,
                f"Cannot read GSIM file {file_path}: {exc}",
            ) from exc

        suffix = file_path.suffix.lower()
        if suffix == ".csv":
            return self._parse_csv_format(
                text, station_id, start, end,
            )
        return self._parse_text_format(
            text, station_id, start, end,
        )

    def _parse_text_format(
        self,
        text: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse GSIM native text format.

        Lines starting with '#' are comments.  Data lines are
        whitespace or tab separated: year, month, index_value, ...
        """
        observations: list[Observation] = []
        lines = text.splitlines()

        data_lines: list[str] = []
        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            data_lines.append(stripped)

        if not data_lines:
            return observations

        # Skip header row if present
        first = data_lines[0]
        if not first[0].isdigit():
            # Detect GSIM .mon CSV format: "date", "MEAN", ...
            if "date" in first.lower() and "mean" in first.lower():
                return self._parse_gsim_mon_csv(
                    data_lines, station_id, start, end,
                )
            data_lines = data_lines[1:]

        for line in data_lines:
            obs = self._parse_text_line(
                line, station_id, start, end,
            )
            if obs is not None:
                observations.append(obs)

        return observations

    def _parse_gsim_mon_csv(
        self,
        data_lines: list[str],
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse GSIM .mon/.year CSV: date,MEAN,SD,CV,...

        GSIM files use ``,\\t`` as delimiter. Normalise to plain comma.
        """
        import csv
        import io

        observations: list[Observation] = []
        cleaned = [line.replace(",\t", ",").replace('"', '') for line in data_lines]
        text = "\n".join(cleaned)
        reader = csv.DictReader(io.StringIO(text))

        for row in reader:
            date_str = (row.get("date") or "").strip()
            if not date_str:
                continue
            try:
                ts = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
            except ValueError:
                continue
            if ts < start or ts > end:
                continue

            mean_str = (row.get("MEAN") or "").strip()
            discharge = _safe_float(mean_str)
            if discharge is not None and abs(discharge - _MISSING_VALUE) < 0.01:
                discharge = None

            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=discharge,
                quality=QualityFlag.RAW if discharge is not None else QualityFlag.MISSING,
            ))

        return observations

    def _parse_text_line(
        self,
        line: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> Observation | None:
        """Parse a single GSIM text data line.

        Expected format: year<sep>month<sep>value[<sep>...]
        where <sep> is whitespace, tab, comma, or semicolon.
        """
        # Split on any common delimiter
        import re
        parts = re.split(r"[,;\t\s]+", line.strip())
        if len(parts) < 3:
            return None

        try:
            year = int(parts[0])
            month = int(parts[1])
            ts = datetime(year, month, 1, tzinfo=UTC)
        except (ValueError, IndexError):
            return None

        if ts < start or ts > end:
            return None

        value_str = parts[2].strip()
        discharge: float | None = None
        quality = QualityFlag.RAW

        raw_value = _safe_float(value_str)
        if raw_value is not None:
            if abs(raw_value - _MISSING_VALUE) < 0.01:
                discharge = None
                quality = QualityFlag.MISSING
            else:
                discharge = raw_value
        else:
            quality = QualityFlag.MISSING

        return Observation(
            station_id=station_id,
            timestamp=ts,
            discharge_m3s=discharge,
            quality=quality,
        )

    def _parse_csv_format(
        self,
        text: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse CSV-formatted GSIM data.

        Expected columns: year, month, mean (or similar index name).
        """
        observations: list[Observation] = []
        reader = csv.DictReader(io.StringIO(text))
        if reader.fieldnames is None:
            return observations

        field_map = {
            f.lower().strip(): f for f in reader.fieldnames
        }

        year_col = field_map.get("year")
        month_col = field_map.get("month")
        date_col = field_map.get("date")
        value_col = (
            field_map.get("mean")
            or field_map.get("value")
            or field_map.get("discharge")
            or field_map.get("index")
        )

        if not value_col:
            return observations

        for row in reader:
            obs = self._parse_csv_row(
                row, year_col, month_col, date_col,
                value_col, station_id, start, end,
            )
            if obs is not None:
                observations.append(obs)

        return observations

    def _parse_csv_row(
        self,
        row: dict[str, str],
        year_col: str | None,
        month_col: str | None,
        date_col: str | None,
        value_col: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> Observation | None:
        """Parse a single CSV row into an Observation."""
        ts: datetime | None = None

        if date_col:
            date_str = row.get(date_col, "").strip()
            if date_str:
                try:
                    ts = datetime.strptime(
                        date_str, "%Y-%m-%d",
                    ).replace(tzinfo=UTC)
                except ValueError:
                    try:
                        ts = datetime.strptime(
                            date_str, "%Y-%m",
                        ).replace(tzinfo=UTC)
                    except ValueError:
                        return None

        if ts is None and year_col:
            year_str = row.get(year_col, "").strip()
            month_str = (
                row.get(month_col, "1").strip()
                if month_col
                else "1"
            )
            try:
                ts = datetime(
                    int(year_str), int(month_str), 1, tzinfo=UTC,
                )
            except (ValueError, TypeError):
                return None

        if ts is None:
            return None

        if ts < start or ts > end:
            return None

        value_str = row.get(value_col, "").strip()
        discharge: float | None = None
        quality = QualityFlag.RAW

        if value_str:
            raw_value = _safe_float(value_str)
            if raw_value is not None:
                if abs(raw_value - _MISSING_VALUE) < 0.01:
                    discharge = None
                    quality = QualityFlag.MISSING
                else:
                    discharge = raw_value
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
