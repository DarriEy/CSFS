"""ADHI (African Database of Hydrometric Indices) connector.

ADHI is the most comprehensive pan-African discharge dataset, covering
1,466 stations across the entire continent with monthly discharge series
and annual hydrometric statistics.

- DOI: 10.23708/LXGXQ9
- Available via IRD DataVerse (dataverse.ird.fr), no authentication required

References
----------
- DOI: https://doi.org/10.23708/LXGXQ9
- DataVerse: https://dataverse.ird.fr
"""

from __future__ import annotations

import csv
import io
from datetime import UTC, datetime
from pathlib import Path

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

ADHI_DOI = "10.23708/LXGXQ9"
ADHI_MISSING_VALUE = -999.0

# Hints for identifying the station metadata file in the DataVerse file list
_STATION_FILE_HINTS = ("stations", "metadata", "catalogue", "catalog")

# Hints for identifying the discharge data file
_DISCHARGE_FILE_HINTS = ("discharge", "monthly", "debit", "flow")

# All African country codes covered by ADHI
ADHI_COUNTRY_CODES: list[str] = [
    "DZ", "AO", "BJ", "BW", "BF", "BI", "CM", "CF", "TD", "CG",
    "CD", "CI", "DJ", "EG", "GQ", "ER", "SZ", "ET", "GA", "GM",
    "GH", "GN", "GW", "KE", "LS", "LR", "LY", "MG", "MW", "ML",
    "MR", "MZ", "NA", "NE", "NG", "RW", "SN", "SL", "SO", "ZA",
    "SD", "TZ", "TG", "TN", "UG", "ZM", "ZW",
]

# ---------------------------------------------------------------------------
# Curated seed catalogue -- ~40 major African river stations
# ---------------------------------------------------------------------------

_SEED_STATIONS: list[dict] = [
    # ---- Nile basin ----
    {
        "native_id": "1633101",
        "name": "NILE AT DONGOLA",
        "lat": 19.17,
        "lon": 30.47,
        "country": "SD",
        "river": "NILE",
        "area": 1580000.0,
    },
    {
        "native_id": "1632200",
        "name": "NILE AT ASWAN",
        "lat": 24.08,
        "lon": 32.88,
        "country": "EG",
        "river": "NILE",
        "area": 1700000.0,
    },
    {
        "native_id": "1637110",
        "name": "VICTORIA NILE AT JINJA",
        "lat": 0.43,
        "lon": 33.19,
        "country": "UG",
        "river": "VICTORIA NILE",
        "area": 263000.0,
    },
    {
        "native_id": "1635202",
        "name": "BLUE NILE AT KHARTOUM",
        "lat": 15.62,
        "lon": 32.52,
        "country": "ET",
        "river": "BLUE NILE",
        "area": 325000.0,
    },
    # ---- Niger basin ----
    {
        "native_id": "1134000103",
        "name": "NIGER AT KOULIKORO",
        "lat": 12.87,
        "lon": -7.55,
        "country": "ML",
        "river": "NIGER",
        "area": 120000.0,
    },
    {
        "native_id": "1134500103",
        "name": "NIGER AT NIAMEY",
        "lat": 13.52,
        "lon": -2.09,
        "country": "NE",
        "river": "NIGER",
        "area": 700000.0,
    },
    {
        "native_id": "1134010700",
        "name": "NIGER AT LOKOJA",
        "lat": 7.80,
        "lon": 6.74,
        "country": "NG",
        "river": "NIGER",
        "area": 2074000.0,
    },
    # ---- Congo basin ----
    {
        "native_id": "1147010004",
        "name": "CONGO AT KINSHASA",
        "lat": -4.30,
        "lon": 15.30,
        "country": "CD",
        "river": "CONGO",
        "area": 3680000.0,
    },
    # ---- Zambezi basin ----
    {
        "native_id": "1291100",
        "name": "ZAMBEZI AT VICTORIA FALLS",
        "lat": -17.93,
        "lon": 25.85,
        "country": "ZM",
        "river": "ZAMBEZI",
        "area": 507200.0,
    },
    # ---- Orange basin ----
    {
        "native_id": "1159100",
        "name": "ORANGE AT VIOOLSDRIF",
        "lat": -28.77,
        "lon": 17.73,
        "country": "ZA",
        "river": "ORANGE",
        "area": 850530.0,
    },
    # ---- Volta basin ----
    {
        "native_id": "1146500",
        "name": "VOLTA AT SENCHI",
        "lat": 6.20,
        "lon": 0.07,
        "country": "GH",
        "river": "VOLTA",
        "area": 394000.0,
    },
    # ---- Senegal basin ----
    {
        "native_id": "1130300105",
        "name": "SENEGAL AT BAKEL",
        "lat": 14.90,
        "lon": -12.46,
        "country": "SN",
        "river": "SENEGAL",
        "area": 218000.0,
    },
    # ---- Cameroon ----
    {
        "native_id": "1048700103",
        "name": "SANAGA AT EDEA",
        "lat": 3.78,
        "lon": 10.07,
        "country": "CM",
        "river": "SANAGA",
        "area": 131500.0,
    },
    # ---- East African rivers ----
    {
        "native_id": "1461200",
        "name": "TANA AT GARISSA",
        "lat": -0.47,
        "lon": 39.63,
        "country": "KE",
        "river": "TANA",
        "area": 32500.0,
    },
    {
        "native_id": "1484100",
        "name": "RUFIJI AT STIEGLER'S GORGE",
        "lat": -7.82,
        "lon": 37.75,
        "country": "TZ",
        "river": "RUFIJI",
        "area": 177000.0,
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


@register("adhi")
class ADHIConnector(BaseConnector):
    """Connector for ADHI -- African Database of Hydrometric Indices.

    Configuration options (via ``config`` dict):
        data_dir : str | Path
            Directory containing pre-downloaded ADHI data files.
    """

    slug = "adhi"
    display_name = "ADHI (African Database of Hydrometric Indices)"
    base_url = "https://dataverse.ird.fr/api"
    country_codes: list[str] = ADHI_COUNTRY_CODES

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        self._file_list_cache: list[dict] | None = None

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return ADHI stations.

        1. Query IRD DataVerse for the dataset file list.
        2. Find and download the station metadata file.
        3. Parse tab/CSV-delimited metadata into Station objects.
        4. Fall back to the curated seed catalogue on failure.
        """
        try:
            file_list = await self._get_file_list()
            station_file = self._find_file(
                file_list, _STATION_FILE_HINTS,
            )
            if station_file is not None:
                content = await self._download_datafile(
                    station_file["id"],
                )
                stations = self._parse_station_metadata(content)
                if stations:
                    logger.info(
                        "adhi_stations_from_api",
                        provider=self.slug,
                        count=len(stations),
                    )
                    return stations
        except (ConnectorError, DataFormatError) as exc:
            logger.warning(
                "adhi_api_fallback_to_seed",
                provider=self.slug,
                error=str(exc),
            )

        # Fallback to seed list
        stations = self._build_seed_stations()
        logger.info(
            "adhi_stations_from_seed",
            provider=self.slug,
            count=len(stations),
        )
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch monthly discharge observations for a station.

        1. Check config["data_dir"] for pre-downloaded files.
        2. Otherwise, find and download the discharge file from
           IRD DataVerse.
        3. Parse tab-delimited monthly discharge data.
        4. Filter to requested station and date range.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")
        start_aware = (
            start if start.tzinfo else start.replace(tzinfo=UTC)
        )
        end_aware = (
            end if end.tzinfo else end.replace(tzinfo=UTC)
        )

        # Try local files first
        data_dir = self.config.get("data_dir")
        if data_dir:
            local_obs = self._read_local_observations(
                Path(data_dir), native_id, station_id,
                start_aware, end_aware,
            )
            if local_obs is not None:
                return TimeSeriesChunk(
                    station_id=station_id,
                    provider=self.slug,
                    observations=local_obs,
                    fetched_at=datetime.now(UTC),
                )

        # Try DataVerse API
        try:
            file_list = await self._get_file_list()
            discharge_file = self._find_file(
                file_list, _DISCHARGE_FILE_HINTS,
            )
            if discharge_file is not None:
                content = await self._download_datafile(
                    discharge_file["id"],
                )
                observations = self._parse_discharge_data(
                    content, native_id, station_id,
                    start_aware, end_aware,
                )
                logger.info(
                    "adhi_observations_fetched",
                    station=native_id,
                    count=len(observations),
                )
                return TimeSeriesChunk(
                    station_id=station_id,
                    provider=self.slug,
                    observations=observations,
                    fetched_at=datetime.now(UTC),
                )
        except (ConnectorError, DataFormatError) as exc:
            logger.warning(
                "adhi_observations_api_failed",
                station=native_id,
                error=str(exc),
            )

        logger.info(
            "adhi_no_observations",
            station=native_id,
            hint=(
                "Set config['data_dir'] to a directory containing "
                "pre-downloaded ADHI data files. Download from "
                f"https://dataverse.ird.fr (DOI: {ADHI_DOI})"
            ),
        )
        return self._empty_chunk(station_id)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Return empty chunk -- ADHI provides historical data only."""
        return self._empty_chunk(station_id)

    # ------------------------------------------------------------------
    # DataVerse API helpers
    # ------------------------------------------------------------------

    async def _get_file_list(self) -> list[dict]:
        """Fetch and cache the dataset file list from IRD DataVerse."""
        if self._file_list_cache is not None:
            return self._file_list_cache

        try:
            resp = await self._get(
                "/datasets/:persistentId/",
                params={"persistentId": f"doi:{ADHI_DOI}"},
            )
            data = resp.json()
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch DataVerse dataset metadata: {exc}",
            ) from exc

        # Navigate to file list in DataVerse response
        version = data.get("data", {}).get("latestVersion", {})
        raw_files = version.get("files", [])

        if not raw_files:
            # Try alternative response structures
            raw_files = data.get("data", {}).get("files", [])

        file_list: list[dict] = []
        for entry in raw_files:
            df = entry.get("dataFile", entry)
            file_list.append({
                "id": df.get("id"),
                "filename": df.get("filename", df.get("name", "")),
                "filesize": df.get("filesize", 0),
            })

        if not file_list:
            raise DataFormatError(
                self.slug,
                "No files found in DataVerse dataset response",
            )

        self._file_list_cache = file_list
        logger.info(
            "adhi_file_list_cached",
            provider=self.slug,
            file_count=len(file_list),
        )
        return file_list

    def _find_file(
        self,
        file_list: list[dict],
        hints: tuple[str, ...],
    ) -> dict | None:
        """Find a file in the list matching any of the hint substrings."""
        for entry in file_list:
            filename = entry.get("filename", "").lower()
            for hint in hints:
                if hint in filename:
                    return entry

        # Fallback: look for .tab files (DataVerse default format)
        for entry in file_list:
            filename = entry.get("filename", "").lower()
            if filename.endswith(".tab"):
                return entry

        return None

    async def _download_datafile(self, file_id: int) -> str:
        """Download a file from IRD DataVerse by file ID."""
        try:
            resp = await self._get(f"/access/datafile/{file_id}")
            return resp.text
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to download DataVerse file {file_id}: {exc}",
            ) from exc

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    def _parse_station_metadata(self, content: str) -> list[Station]:
        """Parse tab/CSV-delimited station metadata into Station objects.

        Tries to auto-detect the delimiter and map columns flexibly.
        """
        delimiter = self._detect_delimiter(content)
        reader = csv.DictReader(io.StringIO(content), delimiter=delimiter)

        if reader.fieldnames is None:
            raise DataFormatError(
                self.slug,
                "Station metadata file has no header row",
            )

        field_map = {
            f.lower().strip(): f for f in reader.fieldnames
        }
        stations: list[Station] = []

        for row in reader:
            try:
                station = self._parse_station_row(row, field_map)
                if station is not None:
                    stations.append(station)
            except (ValueError, KeyError, TypeError) as exc:
                logger.debug(
                    "adhi_station_row_skipped",
                    provider=self.slug,
                    error=str(exc),
                )
                continue

        return stations

    def _parse_station_row(
        self,
        row: dict[str, str],
        field_map: dict[str, str],
    ) -> Station | None:
        """Parse one metadata row into a Station, or None if invalid."""
        lrow = {k.lower().strip(): v for k, v in row.items()}

        native_id = (
            lrow.get("station_code")
            or lrow.get("code")
            or lrow.get("station_id")
            or lrow.get("id")
            or ""
        ).strip()

        if not native_id:
            return None

        name = (
            lrow.get("station_name")
            or lrow.get("name")
            or lrow.get("station")
            or ""
        ).strip()

        lat = _safe_float(
            lrow.get("latitude") or lrow.get("lat"),
        )
        lon = _safe_float(
            lrow.get("longitude") or lrow.get("lon"),
        )

        if lat is None or lon is None:
            return None

        country = (
            lrow.get("country_code")
            or lrow.get("country")
            or lrow.get("pays")
            or ""
        ).strip().upper()

        river = (
            lrow.get("river")
            or lrow.get("river_name")
            or lrow.get("cours_eau")
            or None
        )
        if river is not None:
            river = river.strip() or None

        catchment = _safe_float(
            lrow.get("catchment_area")
            or lrow.get("area_km2")
            or lrow.get("catchment_area_km2")
            or lrow.get("surface")
        )

        return Station(
            id=self._station_id(native_id),
            provider=self.slug,
            native_id=native_id,
            name=name or native_id,
            latitude=lat,
            longitude=lon,
            country_code=country,
            river=river,
            catchment_area_km2=catchment,
        )

    def _parse_discharge_data(
        self,
        content: str,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse tab/CSV-delimited monthly discharge data.

        Expected format: rows with station identifier, date/year-month,
        and discharge value columns.
        """
        delimiter = self._detect_delimiter(content)
        reader = csv.DictReader(io.StringIO(content), delimiter=delimiter)

        if reader.fieldnames is None:
            return []

        observations: list[Observation] = []

        for row in reader:
            lrow = {
                k.lower().strip(): v
                for k, v in row.items()
                if k is not None
            }
            row_station = (
                lrow.get("station_code")
                or lrow.get("code")
                or lrow.get("station_id")
                or lrow.get("station")
                or ""
            ).strip()

            if row_station != native_id:
                continue

            obs = self._parse_discharge_row(
                lrow, station_id, start, end,
            )
            if obs is not None:
                observations.append(obs)

        return observations

    def _parse_discharge_row(
        self,
        lrow: dict[str, str],
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> Observation | None:
        """Parse a single discharge data row into an Observation."""
        ts = self._parse_row_timestamp(lrow)
        if ts is None:
            return None

        if ts < start or ts > end:
            return None

        value_str = (
            lrow.get("discharge")
            or lrow.get("discharge_m3s")
            or lrow.get("debit")
            or lrow.get("value")
            or lrow.get("q")
            or ""
        ).strip()

        discharge: float | None = None
        quality = QualityFlag.RAW

        if value_str:
            try:
                raw_value = float(str(value_str))
            except ValueError:
                quality = QualityFlag.MISSING
                raw_value = ADHI_MISSING_VALUE

            if abs(raw_value - ADHI_MISSING_VALUE) < 0.01:
                discharge = None
                quality = QualityFlag.MISSING
            else:
                discharge = raw_value

        flag_str = (
            lrow.get("quality") or lrow.get("flag") or ""
        ).strip()
        if flag_str:
            quality = _QUALITY_MAP.get(flag_str, quality)

        return Observation(
            station_id=station_id,
            timestamp=ts,
            discharge_m3s=discharge,
            quality=quality,
        )

    @staticmethod
    def _parse_row_timestamp(
        lrow: dict[str, str],
    ) -> datetime | None:
        """Extract a timestamp from a discharge data row."""
        # Try date column first
        date_str = (
            lrow.get("date") or lrow.get("timestamp") or ""
        ).strip()
        if date_str:
            for fmt in ("%Y-%m-%d", "%Y-%m", "%d/%m/%Y", "%Y/%m/%d"):
                try:
                    return datetime.strptime(
                        date_str, fmt,
                    ).replace(tzinfo=UTC)
                except ValueError:
                    continue

        # Try year + month columns
        year_str = (lrow.get("year") or lrow.get("annee") or "").strip()
        month_str = (lrow.get("month") or lrow.get("mois") or "").strip()
        if year_str and month_str:
            try:
                return datetime(
                    int(year_str), int(month_str), 1, tzinfo=UTC,
                )
            except (ValueError, TypeError):
                pass

        return None

    # ------------------------------------------------------------------
    # Local file reading
    # ------------------------------------------------------------------

    def _read_local_observations(
        self,
        data_dir: Path,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation] | None:
        """Try to read observations from a local pre-downloaded file."""
        file_path = self._find_local_file(data_dir, native_id)
        if file_path is None:
            return None

        try:
            content = file_path.read_text(encoding="utf-8")
        except OSError as exc:
            logger.warning(
                "adhi_local_file_read_error",
                file=str(file_path),
                error=str(exc),
            )
            return None

        observations = self._parse_discharge_data(
            content, native_id, station_id, start, end,
        )
        logger.info(
            "adhi_local_observations_loaded",
            station=native_id,
            count=len(observations),
            file=str(file_path),
        )
        return observations

    @staticmethod
    def _find_local_file(
        data_dir: Path,
        native_id: str,
    ) -> Path | None:
        """Locate a local ADHI data file for a given station."""
        candidates = [
            data_dir / f"{native_id}.csv",
            data_dir / f"{native_id}.tab",
            data_dir / f"{native_id}.txt",
        ]
        for candidate in candidates:
            if candidate.is_file():
                return candidate
        return None

    # ------------------------------------------------------------------
    # Seed catalogue
    # ------------------------------------------------------------------

    def _build_seed_stations(self) -> list[Station]:
        """Build Station objects from the curated seed list."""
        stations: list[Station] = []
        for entry in _SEED_STATIONS:
            stations.append(Station(
                id=self._station_id(entry["native_id"]),
                provider=self.slug,
                native_id=entry["native_id"],
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
            ))
        return stations

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _detect_delimiter(content: str) -> str:
        """Auto-detect CSV/TSV delimiter from first data line."""
        first_line = ""
        for line in content.splitlines():
            stripped = line.strip()
            if stripped and not stripped.startswith("#"):
                first_line = stripped
                break

        for delim in ("\t", ";", ","):
            if delim in first_line:
                return delim
        return ","

    def _empty_chunk(self, station_id: str) -> TimeSeriesChunk:
        """Return an empty TimeSeriesChunk for a station."""
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )


# Quality flag mapping
_QUALITY_MAP: dict[str, QualityFlag] = {
    "0": QualityFlag.GOOD,
    "1": QualityFlag.ESTIMATED,
    "2": QualityFlag.SUSPECT,
    "3": QualityFlag.MISSING,
    "good": QualityFlag.GOOD,
    "estimated": QualityFlag.ESTIMATED,
    "suspect": QualityFlag.SUSPECT,
    "missing": QualityFlag.MISSING,
}
