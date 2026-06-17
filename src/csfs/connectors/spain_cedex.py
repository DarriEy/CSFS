"""Spain MITECO/CEDEX connector for historical streamflow data.

MITECO (Ministerio para la Transicion Ecologica y el Reto Demografico)
publishes streamflow data through its CEDEX anuario de aforos system.

This connector supports three modes (tried in order):

1. **Station catalogue** -- a curated seed list of ~30 major gauging
   stations across Spain's principal river basins (Ebro, Duero, Tajo,
   Guadiana, Guadalquivir, Segura, Jucar, etc.).  A station shapefile
   and KMZ are available for download but require geospatial
   dependencies to parse, so the seed list is used by default.

2. **Observations from local files** -- yearbook ZIP archives
   (``TablaAnuarioYYYY-YY.zip``, ~300 MB each) are read from a local
   directory configured via ``config["data_dir"]``.  These ZIPs contain
   semicolon-delimited CSV files with daily discharge in m3/s.

3. **Observations from the CEDEX anuario download portal** -- when no
   local data is configured, the per-basin daily-discharge CSV
   (``afliq.csv``, columns ``indroea;fecha;altura;caudal``) is streamed
   directly from ``https://ceh.cedex.es/anuarioaforos`` and filtered to
   the requested station and date range.  The CEDEX server only
   negotiates legacy TLS, so a relaxed SSL context is used for this host.

If a station cannot be served from any of these, ``fetch_observations``
logs guidance and returns an empty ``TimeSeriesChunk``.
"""

from __future__ import annotations

import csv
import io
import ssl
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import httpx
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

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------
MITECO_BASE_URL = "https://www.mapama.gob.es"
MITECO_DOWNLOAD_PATH = "/app/descargas/descargafichero.aspx"
MITECO_STATION_SHAPEFILE = "estacionesaforos.zip"
MITECO_STATION_KMZ = "estacionesaforos.kmz"
MITECO_YEARBOOK_PATTERN = "TablaAnuario{year_range}.zip"

MITECO_DOWNLOAD_URL = (
    f"{MITECO_BASE_URL}{MITECO_DOWNLOAD_PATH}"
    f"?f=TablaAnuario2020-21.zip"
)

# CEDEX anuario de aforos public download portal. Each river-basin folder
# exposes a daily-discharge CSV (``afliq.csv``) keyed by the national
# ``indroea`` station code. Data spans the full station record up to the
# yearbook's hydrological year (the 2020-2021 anuario ends 30/09/2021).
CEDEX_ANUARIO_BASE_URL = "https://ceh.cedex.es/anuarioaforos"
CEDEX_ANUARIO_FOLDER = "anuario-2020-2021"
CEDEX_DAILY_FLOW_FILE = "afliq.csv"
# CEDEX still requires OpenSSL security level 1; the default level rejects
# the server's handshake. This context is used only for the CEDEX host.
_CEDEX_SSL_CONTEXT = ssl.create_default_context()
_CEDEX_SSL_CONTEXT.set_ciphers("DEFAULT@SECLEVEL=1")

# Spanish date format used in yearbook CSVs
_DATE_FORMAT_DMY = "%d/%m/%Y"

# CSV column names (Spanish headers) -- case-insensitive matching
_COL_STATION = ("estacion", "estación", "indroea", "codigo")
_COL_DATE = ("fecha",)
_COL_DISCHARGE = ("caudal", "caudal_medio", "qmed", "valor")

# Quality flag mapping for MITECO validation codes
_QUALITY_MAP: dict[str | None, QualityFlag] = {
    None: QualityFlag.RAW,
    "": QualityFlag.RAW,
    "0": QualityFlag.GOOD,
    "1": QualityFlag.GOOD,
    "2": QualityFlag.ESTIMATED,
    "3": QualityFlag.SUSPECT,
}

# ---------------------------------------------------------------------------
# Curated seed catalogue of major Spanish gauging stations
# ---------------------------------------------------------------------------
# Covers all major hydrographic confederations in Spain:
# Ebro, Duero, Tajo, Guadiana, Guadalquivir, Segura, Jucar,
# Mino-Sil, Cantabrico, Cuencas Internas de Cataluna, etc.
# ---------------------------------------------------------------------------

_SEED_STATIONS: list[dict] = [
    # Ebro basin
    {
        "code": "9001",
        "name": "EBRO EN MIRANDA DE EBRO",
        "lat": 42.68,
        "lon": -2.95,
        "river": "EBRO",
        "area": 3327.0,
    },
    {
        "code": "9002",
        "name": "EBRO EN CASTEJON",
        "lat": 42.17,
        "lon": -1.69,
        "river": "EBRO",
        "area": 25094.0,
    },
    {
        "code": "9120",
        "name": "EBRO EN ZARAGOZA",
        "lat": 41.65,
        "lon": -0.88,
        "river": "EBRO",
        "area": 40434.0,
    },
    {
        "code": "9027",
        "name": "EBRO EN TORTOSA",
        "lat": 40.81,
        "lon": 0.52,
        "river": "EBRO",
        "area": 84230.0,
    },
    # Duero basin
    {
        "code": "2001",
        "name": "DUERO EN SORIA",
        "lat": 41.76,
        "lon": -2.47,
        "river": "DUERO",
        "area": 3219.0,
    },
    {
        "code": "2060",
        "name": "DUERO EN TORO",
        "lat": 41.52,
        "lon": -5.39,
        "river": "DUERO",
        "area": 44027.0,
    },
    {
        "code": "2102",
        "name": "DUERO EN ZAMORA",
        "lat": 41.50,
        "lon": -5.75,
        "river": "DUERO",
        "area": 49218.0,
    },
    # Tajo basin
    {
        "code": "3001",
        "name": "TAJO EN TRILLO",
        "lat": 40.70,
        "lon": -2.59,
        "river": "TAJO",
        "area": 6232.0,
    },
    {
        "code": "3045",
        "name": "TAJO EN ARANJUEZ",
        "lat": 40.04,
        "lon": -3.61,
        "river": "TAJO",
        "area": 9340.0,
    },
    {
        "code": "3070",
        "name": "TAJO EN TOLEDO",
        "lat": 39.86,
        "lon": -4.02,
        "river": "TAJO",
        "area": 24788.0,
    },
    {
        "code": "3170",
        "name": "TAJO EN ALCANTARA",
        "lat": 39.72,
        "lon": -6.89,
        "river": "TAJO",
        "area": 52625.0,
    },
    # Guadiana basin
    {
        "code": "4001",
        "name": "GUADIANA EN CIUDAD REAL",
        "lat": 38.98,
        "lon": -3.93,
        "river": "GUADIANA",
        "area": 9700.0,
    },
    {
        "code": "4112",
        "name": "GUADIANA EN BADAJOZ",
        "lat": 38.88,
        "lon": -6.97,
        "river": "GUADIANA",
        "area": 48400.0,
    },
    # Guadalquivir basin
    {
        "code": "5001",
        "name": "GUADALQUIVIR EN MENGIBAR",
        "lat": 37.97,
        "lon": -3.81,
        "river": "GUADALQUIVIR",
        "area": 8250.0,
    },
    {
        "code": "5036",
        "name": "GUADALQUIVIR EN CORDOBA",
        "lat": 37.88,
        "lon": -4.78,
        "river": "GUADALQUIVIR",
        "area": 28800.0,
    },
    {
        "code": "5072",
        "name": "GUADALQUIVIR EN SEVILLA",
        "lat": 37.39,
        "lon": -5.99,
        "river": "GUADALQUIVIR",
        "area": 49500.0,
    },
    # Segura basin
    {
        "code": "7001",
        "name": "SEGURA EN CIEZA",
        "lat": 38.24,
        "lon": -1.42,
        "river": "SEGURA",
        "area": 7205.0,
    },
    {
        "code": "7030",
        "name": "SEGURA EN ORIHUELA",
        "lat": 38.08,
        "lon": -0.95,
        "river": "SEGURA",
        "area": 14600.0,
    },
    # Jucar basin
    {
        "code": "8001",
        "name": "JUCAR EN CUENCA",
        "lat": 40.07,
        "lon": -2.13,
        "river": "JUCAR",
        "area": 3840.0,
    },
    {
        "code": "8036",
        "name": "JUCAR EN ALCIRA",
        "lat": 39.15,
        "lon": -0.44,
        "river": "JUCAR",
        "area": 19850.0,
    },
    # Mino-Sil basin
    {
        "code": "1001",
        "name": "MINO EN LUGO",
        "lat": 43.01,
        "lon": -7.55,
        "river": "MINO",
        "area": 2303.0,
    },
    {
        "code": "1050",
        "name": "MINO EN ORENSE",
        "lat": 42.33,
        "lon": -7.86,
        "river": "MINO",
        "area": 11520.0,
    },
    {
        "code": "1080",
        "name": "SIL EN PONFERRADA",
        "lat": 42.55,
        "lon": -6.59,
        "river": "SIL",
        "area": 2985.0,
    },
    # Cantabrico
    {
        "code": "1301",
        "name": "NALON EN OVIEDO",
        "lat": 43.36,
        "lon": -5.85,
        "river": "NALON",
        "area": 2760.0,
    },
    {
        "code": "1401",
        "name": "NERVION EN BILBAO",
        "lat": 43.26,
        "lon": -2.92,
        "river": "NERVION",
        "area": 1754.0,
    },
    # Cuencas Internas de Cataluna
    {
        "code": "10001",
        "name": "LLOBREGAT EN MARTORELL",
        "lat": 41.47,
        "lon": 1.93,
        "river": "LLOBREGAT",
        "area": 4565.0,
    },
    {
        "code": "10020",
        "name": "TER EN GIRONA",
        "lat": 41.98,
        "lon": 2.82,
        "river": "TER",
        "area": 1790.0,
    },
    # Cuencas del Sur (Andalucia)
    {
        "code": "6001",
        "name": "GUADALHORCE EN MALAGA",
        "lat": 36.72,
        "lon": -4.42,
        "river": "GUADALHORCE",
        "area": 3157.0,
    },
    # Canarias
    {
        "code": "12001",
        "name": "BARRANCO DE LA ALDEA (GRAN CANARIA)",
        "lat": 28.03,
        "lon": -15.79,
        "river": "BARRANCO DE LA ALDEA",
        "area": 65.0,
    },
]


# ---------------------------------------------------------------------------
# Station -> CEDEX anuario basin folder
# ---------------------------------------------------------------------------
# Maps each seed ``indroea`` code to the CEDEX river-basin folder whose
# ``afliq.csv`` carries that station. Verified against the anuario-2020-2021
# ``estaf.csv`` station catalogues. Stations whose basin folder token is not
# resolvable in the anuario portal (e.g. Catalan internal basins, southern
# Andalusian basins, Canary Islands) are intentionally omitted -- those fall
# through to an empty chunk rather than guessing a wrong folder.
_STATION_BASIN: dict[str, str] = {
    # Ebro
    "9001": "EBRO", "9002": "EBRO", "9120": "EBRO", "9027": "EBRO",
    # Duero
    "2001": "DUERO", "2060": "DUERO", "2102": "DUERO",
    # Tajo
    "3001": "TAJO", "3045": "TAJO", "3070": "TAJO", "3170": "TAJO",
    # Guadiana
    "4001": "GUADIANA",
    # Guadalquivir
    "5001": "GUADALQUIVIR", "5072": "GUADALQUIVIR",
    # Segura
    "7001": "SEGURA", "7030": "SEGURA",
    # Jucar
    "8001": "JUCAR", "8036": "JUCAR",
    # Cantabrico (CEDEX groups the Mino/Sil/Nalon gauges here)
    "1050": "CANTABRICO", "1080": "CANTABRICO", "1301": "CANTABRICO",
}


@register("spain_cedex")
class SpainCedexConnector(BaseConnector):
    """MITECO/CEDEX connector -- seed catalogue, observations from local files.

    Configuration options (via ``config`` dict):
        data_dir : str | Path
            Directory containing yearbook ZIP or extracted CSV files.
        verify_endpoint : bool
            If True, ping the download endpoint during fetch_stations
            to confirm it is still live (default False).
    """

    slug = "spain_cedex"
    display_name = "MITECO/CEDEX Anuario de Aforos (Spain)"
    base_url = MITECO_BASE_URL
    country_codes: list[str] = ["ES"]

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        # Cache downloaded per-basin afliq.csv text so repeated station
        # fetches within one connector lifetime hit the network once.
        self._basin_csv_cache: dict[str, str] = {}

    async def fetch_stations(self) -> list[Station]:
        """Return Spanish gauging stations from the curated seed list.

        Optionally verifies the MITECO download endpoint is live when
        ``config["verify_endpoint"]`` is True.
        """
        if self.config.get("verify_endpoint", False):
            await self._verify_download_endpoint()

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
        """Read observations from local files or the CEDEX download portal.

        Local yearbook ZIP/CSV files (``config["data_dir"]``) take
        precedence. When no local data directory is configured, the
        station's daily-discharge record is streamed from the public CEDEX
        anuario portal. Returns an empty chunk if neither source yields
        data for the station.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = self.config.get("data_dir")

        start_aware = start if start.tzinfo else start.replace(tzinfo=UTC)
        end_aware = end if end.tzinfo else end.replace(tzinfo=UTC)

        if not data_dir:
            return await self._fetch_from_cedex(
                station_id, native_id, start_aware, end_aware,
            )

        data_path = Path(data_dir)

        # Try extracted CSV files first, then ZIP archives
        observations = self._read_from_csv_files(
            data_path, native_id, start_aware, end_aware,
        )
        if not observations:
            observations = self._read_from_zip_files(
                data_path, native_id, start_aware, end_aware,
            )

        if not observations:
            logger.info(
                "miteco_no_data_found",
                station=native_id,
                data_dir=str(data_path),
                hint=(
                    f"Download yearbook data from {MITECO_DOWNLOAD_URL} "
                    f"and place in {data_path}"
                ),
            )

        logger.info(
            "miteco_observations_loaded",
            station=native_id,
            count=len(observations),
        )

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # ------------------------------------------------------------------
    # CEDEX anuario download portal
    # ------------------------------------------------------------------

    async def _fetch_from_cedex(
        self,
        station_id: str,
        native_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch daily discharge for one station from the CEDEX portal."""
        basin = _STATION_BASIN.get(native_id)
        if basin is None:
            logger.info(
                "cedex_no_basin_mapping",
                station=native_id,
                hint=(
                    "Station is not mapped to a CEDEX anuario basin folder; "
                    f"set config['data_dir'] and download from {MITECO_DOWNLOAD_URL}"
                ),
            )
            return self._empty_chunk(station_id)

        try:
            text = await self._load_basin_csv(basin, native_id)
        except Exception as exc:  # noqa: BLE001 - network/TLS resilience
            logger.warning(
                "cedex_download_failed",
                station=native_id,
                basin=basin,
                error=str(exc)[:200],
            )
            return self._empty_chunk(station_id)

        observations = self._parse_csv_text(text, native_id, start, end)
        logger.info(
            "cedex_observations_loaded",
            station=native_id,
            basin=basin,
            count=len(observations),
        )
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    async def _load_basin_csv(self, basin: str, native_id: str) -> str:
        """Stream a basin ``afliq.csv`` and return header + station rows.

        The per-basin files are large (tens to >100 MB), so the response is
        streamed and only the header plus rows for ``native_id`` are
        retained. The filtered text (per station) is cached for the
        connector lifetime to avoid re-downloading for repeated calls.
        """
        cache_key = f"{basin}:{native_id}"
        cached = self._basin_csv_cache.get(cache_key)
        if cached is not None:
            return cached

        url = (
            f"{CEDEX_ANUARIO_BASE_URL}/{CEDEX_ANUARIO_FOLDER}"
            f"/{basin}/{CEDEX_DAILY_FLOW_FILE}"
        )
        # The CEDEX host negotiates only legacy TLS, so use a dedicated
        # client with the relaxed SSL context rather than the base client
        # (which targets the MITECO host with default TLS).
        kept: list[str] = []
        header: str | None = None
        prefix = f"{native_id};"
        client = httpx.AsyncClient(
            timeout=httpx.Timeout(120.0, connect=15.0),
            verify=_CEDEX_SSL_CONTEXT,
            follow_redirects=True,
            headers={"User-Agent": "CSFS/0.1 (https://github.com/DarriEy/CSFS)"},
        )
        async with client, client.stream("GET", url) as resp:
            resp.raise_for_status()
            buf = ""
            async for raw in resp.aiter_bytes():
                buf += raw.decode("latin-1", errors="replace")
                lines = buf.split("\n")
                buf = lines.pop()  # keep trailing partial line
                for line in lines:
                    if header is None:
                        header = line
                        continue
                    if line.startswith(prefix):
                        kept.append(line)
            if buf and header is not None and buf.startswith(prefix):
                kept.append(buf)

        text = "\n".join([header or "", *kept])
        self._basin_csv_cache[cache_key] = text
        return text

    # ------------------------------------------------------------------
    # Download endpoint verification
    # ------------------------------------------------------------------

    async def _verify_download_endpoint(self) -> None:
        """Ping the MITECO download endpoint to confirm availability."""
        try:
            resp = await self._get(
                MITECO_DOWNLOAD_PATH,
                params={"f": MITECO_STATION_KMZ},
            )
            logger.info(
                "miteco_endpoint_verified",
                status=resp.status_code,
                content_length=len(resp.content),
            )
        except Exception as exc:
            logger.warning(
                "miteco_endpoint_check_failed",
                error=str(exc),
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
                    id=self._station_id(entry["code"]),
                    provider=self.slug,
                    native_id=entry["code"],
                    name=entry["name"],
                    latitude=float(str(entry["lat"])),
                    longitude=float(str(entry["lon"])),
                    country_code="ES",
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
    # CSV file parsing
    # ------------------------------------------------------------------

    def _read_from_csv_files(
        self,
        data_dir: Path,
        native_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Search for and parse extracted CSV files in the data dir."""
        observations: list[Observation] = []

        csv_files = sorted(data_dir.glob("*.csv"))
        for csv_file in csv_files:
            try:
                text = csv_file.read_text(
                    encoding="utf-8-sig", errors="replace",
                )
                obs = self._parse_csv_text(
                    text, native_id, start, end,
                )
                observations.extend(obs)
            except OSError as exc:
                logger.warning(
                    "miteco_csv_read_error",
                    file=str(csv_file),
                    error=str(exc),
                )

        return observations

    def _read_from_zip_files(
        self,
        data_dir: Path,
        native_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Search for yearbook ZIPs and parse CSV entries within."""
        observations: list[Observation] = []

        zip_files = sorted(data_dir.glob("*.zip"))
        for zip_path in zip_files:
            try:
                obs = self._parse_yearbook_zip(
                    zip_path, native_id, start, end,
                )
                observations.extend(obs)
            except (zipfile.BadZipFile, OSError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Failed to read yearbook ZIP {zip_path}: {exc}",
                ) from exc

        return observations

    def _parse_yearbook_zip(
        self,
        zip_path: Path,
        native_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Extract and parse CSV files from a yearbook ZIP archive."""
        observations: list[Observation] = []

        with zipfile.ZipFile(zip_path, "r") as zf:
            for name in zf.namelist():
                if not name.lower().endswith(".csv"):
                    continue
                try:
                    raw = zf.read(name)
                    text = raw.decode("utf-8-sig", errors="replace")
                    obs = self._parse_csv_text(
                        text, native_id, start, end,
                    )
                    observations.extend(obs)
                except (KeyError, UnicodeDecodeError) as exc:
                    logger.warning(
                        "miteco_zip_entry_error",
                        zip_file=str(zip_path),
                        entry=name,
                        error=str(exc),
                    )

        return observations

    def _parse_csv_text(
        self,
        text: str,
        native_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse semicolon-delimited CSV text with Spanish headers.

        Expected format (semicolon-delimited):
            estacion;fecha;caudal[;calidad]
            9001;01/01/2020;12.34;0
        """
        observations: list[Observation] = []
        station_id = self._station_id(native_id)

        # Try semicolon first, then comma
        delimiter = ";"
        if ";" not in text.split("\n", 1)[0] and "," in text:
            delimiter = ","

        reader = csv.DictReader(
            io.StringIO(text), delimiter=delimiter,
        )
        if reader.fieldnames is None:
            return observations

        # Normalize headers to lowercase for matching
        lower_fields = {
            f.strip().lower(): f
            for f in reader.fieldnames
        }

        station_col = self._find_column(lower_fields, _COL_STATION)
        date_col = self._find_column(lower_fields, _COL_DATE)
        discharge_col = self._find_column(
            lower_fields, _COL_DISCHARGE,
        )

        if date_col is None or discharge_col is None:
            return observations

        for row in reader:
            obs = self._parse_csv_row(
                row,
                station_col=station_col,
                date_col=date_col,
                discharge_col=discharge_col,
                native_id=native_id,
                station_id=station_id,
                start=start,
                end=end,
            )
            if obs is not None:
                observations.append(obs)

        return observations

    def _parse_csv_row(
        self,
        row: dict,
        *,
        station_col: str | None,
        date_col: str,
        discharge_col: str,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> Observation | None:
        """Parse a single CSV row into an Observation."""
        # Filter by station if column exists
        if station_col is not None:
            row_station = (row.get(station_col) or "").strip()
            if row_station and row_station != native_id:
                return None

        date_str = (row.get(date_col) or "").strip()
        if not date_str:
            return None

        ts = self._parse_date(date_str)
        if ts is None:
            return None

        if ts < start or ts > end:
            return None

        raw_val = (row.get(discharge_col) or "").strip()
        discharge = self._parse_discharge(raw_val)

        # Check for quality column
        quality_str = None
        for key in row:
            if key and key.strip().lower() in ("calidad", "quality"):
                quality_str = (row[key] or "").strip()
                break

        quality = _QUALITY_MAP.get(quality_str, QualityFlag.RAW)
        if discharge is None:
            quality = QualityFlag.MISSING

        return Observation(
            station_id=station_id,
            timestamp=ts,
            discharge_m3s=discharge,
            quality=quality,
        )

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _find_column(
        lower_fields: dict[str, str],
        candidates: tuple[str, ...],
    ) -> str | None:
        """Find the original column name matching one of the candidates."""
        for candidate in candidates:
            if candidate in lower_fields:
                return lower_fields[candidate]
        return None

    @staticmethod
    def _parse_date(date_str: str) -> datetime | None:
        """Parse a date string in dd/mm/yyyy or yyyy-mm-dd format."""
        for fmt in (_DATE_FORMAT_DMY, "%Y-%m-%d"):
            try:
                return datetime.strptime(date_str, fmt).replace(
                    tzinfo=UTC,
                )
            except ValueError:
                continue
        return None

    @staticmethod
    def _parse_discharge(raw_val: str) -> float | None:
        """Parse a discharge value, handling Spanish decimal commas."""
        if not raw_val:
            return None

        # Replace comma decimal separator with dot
        cleaned = raw_val.replace(",", ".")

        try:
            value = float(str(cleaned))
        except ValueError:
            return None

        # Treat negative sentinel values as missing
        if value < 0:
            return None

        return value

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
