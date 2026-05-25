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
    # North America
    {
        "id": 'GSIM_US_0001',
        "name": 'Mississippi at Vicksburg',
        "lat": 32.32,
        "lon": -90.91,
        "country": 'US',
        "river": 'Mississippi',
        "area": 2964255.0,
    },
    {
        "id": 'GSIM_US_0002',
        "name": 'Missouri at Hermann',
        "lat": 38.71,
        "lon": -91.44,
        "country": 'US',
        "river": 'Missouri',
        "area": 1353280.0,
    },
    {
        "id": 'GSIM_US_0003',
        "name": 'Ohio at Metropolis',
        "lat": 37.15,
        "lon": -88.73,
        "country": 'US',
        "river": 'Ohio',
        "area": 526000.0,
    },
    {
        "id": 'GSIM_US_0004',
        "name": 'Columbia at The Dalles',
        "lat": 45.61,
        "lon": -121.17,
        "country": 'US',
        "river": 'Columbia',
        "area": 613830.0,
    },
    {
        "id": 'GSIM_US_0005',
        "name": 'Colorado at Lees Ferry',
        "lat": 36.86,
        "lon": -111.59,
        "country": 'US',
        "river": 'Colorado',
        "area": 289560.0,
    },
    {
        "id": 'GSIM_CA_0001',
        "name": 'St Lawrence at Cornwall',
        "lat": 45.01,
        "lon": -74.78,
        "country": 'CA',
        "river": 'St Lawrence',
        "area": 773900.0,
    },
    {
        "id": 'GSIM_CA_0002',
        "name": 'Mackenzie at Arctic Red River',
        "lat": 67.46,
        "lon": -133.74,
        "country": 'CA',
        "river": 'Mackenzie',
        "area": 1660000.0,
    },
    {
        "id": 'GSIM_CA_0003',
        "name": 'Nelson at Kelsey',
        "lat": 56.04,
        "lon": -96.52,
        "country": 'CA',
        "river": 'Nelson',
        "area": 1093000.0,
    },
    {
        "id": 'GSIM_CA_0004',
        "name": 'Fraser at Hope',
        "lat": 49.38,
        "lon": -121.45,
        "country": 'CA',
        "river": 'Fraser',
        "area": 217000.0,
    },
    {
        "id": 'GSIM_MX_0001',
        "name": 'Rio Lerma at La Piedad',
        "lat": 20.35,
        "lon": -102.03,
        "country": 'MX',
        "river": 'Lerma',
        "area": 47800.0,
    },
    # South America
    {
        "id": 'GSIM_BR_0001',
        "name": 'Amazon at Obidos',
        "lat": -1.95,
        "lon": -55.51,
        "country": 'BR',
        "river": 'Amazon',
        "area": 4680000.0,
    },
    {
        "id": 'GSIM_BR_0002',
        "name": 'Parana at Corrientes',
        "lat": -27.47,
        "lon": -58.84,
        "country": 'BR',
        "river": 'Parana',
        "area": 1950000.0,
    },
    {
        "id": 'GSIM_BR_0003',
        "name": 'Sao Francisco at Juazeiro',
        "lat": -9.41,
        "lon": -40.5,
        "country": 'BR',
        "river": 'Sao Francisco',
        "area": 510800.0,
    },
    {
        "id": 'GSIM_AR_0001',
        "name": 'Parana at Timbues',
        "lat": -32.67,
        "lon": -60.73,
        "country": 'AR',
        "river": 'Parana',
        "area": 2600000.0,
    },
    {
        "id": 'GSIM_CL_0001',
        "name": 'Bio Bio at Desembocadura',
        "lat": -36.82,
        "lon": -73.1,
        "country": 'CL',
        "river": 'Bio Bio',
        "area": 24264.0,
    },
    # Europe
    {
        "id": 'GSIM_DE_0001',
        "name": 'Rhine at Rees',
        "lat": 51.76,
        "lon": 6.4,
        "country": 'DE',
        "river": 'Rhine',
        "area": 159300.0,
    },
    {
        "id": 'GSIM_DE_0002',
        "name": 'Elbe at Neu Darchau',
        "lat": 53.23,
        "lon": 10.89,
        "country": 'DE',
        "river": 'Elbe',
        "area": 131950.0,
    },
    {
        "id": 'GSIM_DE_0003',
        "name": 'Danube at Achleiten',
        "lat": 48.58,
        "lon": 13.5,
        "country": 'DE',
        "river": 'Danube',
        "area": 76653.0,
    },
    {
        "id": 'GSIM_FR_0001',
        "name": 'Loire at Montjean',
        "lat": 47.39,
        "lon": -0.86,
        "country": 'FR',
        "river": 'Loire',
        "area": 110000.0,
    },
    {
        "id": 'GSIM_FR_0002',
        "name": 'Rhone at Beaucaire',
        "lat": 43.81,
        "lon": 4.65,
        "country": 'FR',
        "river": 'Rhone',
        "area": 95590.0,
    },
    {
        "id": 'GSIM_GB_0001',
        "name": 'Thames at Kingston',
        "lat": 51.41,
        "lon": -0.31,
        "country": 'GB',
        "river": 'Thames',
        "area": 9948.0,
    },
    {
        "id": 'GSIM_GB_0002',
        "name": 'Severn at Bewdley',
        "lat": 52.38,
        "lon": -2.32,
        "country": 'GB',
        "river": 'Severn',
        "area": 4325.0,
    },
    {
        "id": 'GSIM_NO_0001',
        "name": 'Glomma at Langnes',
        "lat": 59.28,
        "lon": 11.13,
        "country": 'NO',
        "river": 'Glomma',
        "area": 40440.0,
    },
    {
        "id": 'GSIM_SE_0001',
        "name": 'Gota alv at Sjotorp',
        "lat": 58.92,
        "lon": 14.12,
        "country": 'SE',
        "river": 'Gota alv',
        "area": 47000.0,
    },
    # Asia
    {
        "id": 'GSIM_RU_0001',
        "name": 'Ob at Salekhard',
        "lat": 66.53,
        "lon": 66.6,
        "country": 'RU',
        "river": 'Ob',
        "area": 2950000.0,
    },
    {
        "id": 'GSIM_RU_0002',
        "name": 'Yenisei at Igarka',
        "lat": 67.47,
        "lon": 86.57,
        "country": 'RU',
        "river": 'Yenisei',
        "area": 2440000.0,
    },
    {
        "id": 'GSIM_RU_0003',
        "name": 'Lena at Kusur',
        "lat": 70.68,
        "lon": 127.39,
        "country": 'RU',
        "river": 'Lena',
        "area": 2430000.0,
    },
    {
        "id": 'GSIM_RU_0004',
        "name": 'Amur at Khabarovsk',
        "lat": 48.56,
        "lon": 135.07,
        "country": 'RU',
        "river": 'Amur',
        "area": 1630000.0,
    },
    {
        "id": 'GSIM_CN_0001',
        "name": 'Yangtze at Datong',
        "lat": 30.77,
        "lon": 117.62,
        "country": 'CN',
        "river": 'Yangtze',
        "area": 1705383.0,
    },
    {
        "id": 'GSIM_CN_0002',
        "name": 'Yellow River at Huayuankou',
        "lat": 34.91,
        "lon": 113.65,
        "country": 'CN',
        "river": 'Yellow River',
        "area": 730036.0,
    },
    {
        "id": 'GSIM_IN_0001',
        "name": 'Ganges at Farakka',
        "lat": 25.0,
        "lon": 87.92,
        "country": 'IN',
        "river": 'Ganges',
        "area": 835000.0,
    },
    {
        "id": 'GSIM_BD_0001',
        "name": 'Brahmaputra at Bahadurabad',
        "lat": 25.18,
        "lon": 89.67,
        "country": 'BD',
        "river": 'Brahmaputra',
        "area": 580000.0,
    },
    {
        "id": 'GSIM_TH_0001',
        "name": 'Chao Phraya at Nakhon Sawan',
        "lat": 15.7,
        "lon": 100.13,
        "country": 'TH',
        "river": 'Chao Phraya',
        "area": 110569.0,
    },
    {
        "id": 'GSIM_VN_0001',
        "name": 'Mekong at Stung Treng',
        "lat": 13.53,
        "lon": 105.97,
        "country": 'VN',
        "river": 'Mekong',
        "area": 635000.0,
    },
    # Africa
    {
        "id": 'GSIM_EG_0001',
        "name": 'Nile at Aswan',
        "lat": 24.08,
        "lon": 32.9,
        "country": 'EG',
        "river": 'Nile',
        "area": 1700000.0,
    },
    {
        "id": 'GSIM_NG_0001',
        "name": 'Niger at Lokoja',
        "lat": 7.8,
        "lon": 6.77,
        "country": 'NG',
        "river": 'Niger',
        "area": 2074171.0,
    },
    {
        "id": 'GSIM_CD_0001',
        "name": 'Congo at Kinshasa',
        "lat": -4.3,
        "lon": 15.3,
        "country": 'CD',
        "river": 'Congo',
        "area": 3475000.0,
    },
    {
        "id": 'GSIM_ZM_0001',
        "name": 'Zambezi at Victoria Falls',
        "lat": -17.93,
        "lon": 25.85,
        "country": 'ZM',
        "river": 'Zambezi',
        "area": 507200.0,
    },
    {
        "id": 'GSIM_ZA_0001',
        "name": 'Orange at Vioolsdrif',
        "lat": -28.77,
        "lon": 17.73,
        "country": 'ZA',
        "river": 'Orange',
        "area": 850530.0,
    },
    # Oceania
    {
        "id": 'GSIM_AU_0001',
        "name": 'Murray at Lock 1',
        "lat": -34.35,
        "lon": 139.62,
        "country": 'AU',
        "river": 'Murray',
        "area": 981000.0,
    },
    {
        "id": 'GSIM_NZ_0001',
        "name": 'Waikato at Hamilton',
        "lat": -37.78,
        "lon": 175.28,
        "country": 'NZ',
        "river": 'Waikato',
        "area": 8270.0,
    },
    # Italy (gap country)
    {
        "id": 'GSIM_IT_0001',
        "name": 'Po at Pontelagoscuro',
        "lat": 44.9,
        "lon": 11.6,
        "country": 'IT',
        "river": 'Po',
        "area": 70091.0,
    },
    {
        "id": 'GSIM_IT_0002',
        "name": 'Tevere at Roma',
        "lat": 41.9,
        "lon": 12.5,
        "country": 'IT',
        "river": 'Tevere',
        "area": 16545.0,
    },
    # South Korea (gap country)
    {
        "id": 'GSIM_KR_0001',
        "name": 'Han River at Seoul',
        "lat": 37.5,
        "lon": 127.0,
        "country": 'KR',
        "river": 'Han',
        "area": 23800.0,
    },
    {
        "id": 'GSIM_KR_0002',
        "name": 'Nakdong at Changnyeong',
        "lat": 35.5,
        "lon": 128.5,
        "country": 'KR',
        "river": 'Nakdong',
        "area": 16352.0,
    },
    # Denmark (gap country)
    {
        "id": 'GSIM_DK_0001',
        "name": 'Gudenaa at Randers',
        "lat": 56.5,
        "lon": 10.0,
        "country": 'DK',
        "river": 'Gudenaa',
        "area": 2650.0,
    },
    {
        "id": 'GSIM_DK_0002',
        "name": 'Skjern A at Skjern',
        "lat": 55.9,
        "lon": 8.5,
        "country": 'DK',
        "river": 'Skjern A',
        "area": 2500.0,
    },
    # Estonia (gap country)
    {
        "id": 'GSIM_EE_0001',
        "name": 'Emajogi at Tartu',
        "lat": 58.4,
        "lon": 26.7,
        "country": 'EE',
        "river": 'Emajogi',
        "area": 7850.0,
    },
    {
        "id": 'GSIM_EE_0002',
        "name": 'Narva at Narva',
        "lat": 59.4,
        "lon": 28.0,
        "country": 'EE',
        "river": 'Narva',
        "area": 56200.0,
    },
    # Peru (gap country)
    {
        "id": 'GSIM_PE_0001',
        "name": 'Amazonas at Iquitos',
        "lat": -3.7,
        "lon": -73.2,
        "country": 'PE',
        "river": 'Amazonas',
        "area": 720000.0,
    },
    {
        "id": 'GSIM_PE_0002',
        "name": 'Rimac at Lima',
        "lat": -12.0,
        "lon": -77.0,
        "country": 'PE',
        "river": 'Rimac',
        "area": 2237.0,
    },
    # Portugal (gap country)
    {
        "id": 'GSIM_PT_0001',
        "name": 'Douro at Porto',
        "lat": 41.1,
        "lon": -8.6,
        "country": 'PT',
        "river": 'Douro',
        "area": 97603.0,
    },
    {
        "id": 'GSIM_PT_0002',
        "name": 'Tejo at Santarem',
        "lat": 39.2,
        "lon": -8.7,
        "country": 'PT',
        "river": 'Tejo',
        "area": 67490.0,
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
            data_lines = data_lines[1:]

        for line in data_lines:
            obs = self._parse_text_line(
                line, station_id, start, end,
            )
            if obs is not None:
                observations.append(obs)

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
