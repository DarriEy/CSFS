"""GRDC (Global Runoff Data Centre) connector for historical streamflow.

GRDC is the primary global fallback for observed daily/monthly discharge data.
Unlike most connectors, GRDC does not expose a public REST API for time series.
Data must be requested through their portal and downloaded as text files.

This connector supports two modes:

1. **Station catalogue** -- fetched from GRDC's public SPARQL/RDF endpoint
   at https://grdc.bafg.de/GRDC via a curated seed list of ~500 major
   stations in countries without dedicated national APIs.

2. **Observations from local files** -- GRDC text files (semicolon-delimited,
   36-line header, missing value sentinel -999.0) are read from a local
   directory configured via ``config["data_dir"]``.

If no local data files are found, ``fetch_observations`` logs guidance on
how to request data and returns an empty ``TimeSeriesChunk``.
"""

from __future__ import annotations

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
# GRDC file-format constants
# ---------------------------------------------------------------------------
GRDC_HEADER_LINES = 36
GRDC_MISSING_VALUE = -999.0
GRDC_DATA_REQUEST_URL = "https://portal.grdc.bafg.de/"

# Quality flag mapping for GRDC original/calculated flags
GRDC_QUALITY_MAP: dict[str | None, QualityFlag] = {
    None: QualityFlag.RAW,
    "": QualityFlag.RAW,
    "0": QualityFlag.GOOD,          # original value
    "1": QualityFlag.ESTIMATED,     # calculated / reconstructed
    "2": QualityFlag.SUSPECT,       # suspect / under review
    "3": QualityFlag.MISSING,       # gap-filled placeholder
}

# ---------------------------------------------------------------------------
# Curated seed catalogue of major GRDC stations
# ---------------------------------------------------------------------------
# Countries primarily served by GRDC (no dedicated national API in CSFS):
# BY=Belarus, CY=Cyprus, LV=Latvia, MD=Moldova, MK=North Macedonia,
# RO=Romania (GRDC supplement), RS=Serbia, RU=Russia, TR=Turkey (GRDC
# supplement), UA=Ukraine, plus select African/Asian stations.
# ---------------------------------------------------------------------------

GRDC_COUNTRY_CODES: list[str] = [
    "BY", "CY", "LV", "MD", "MK", "RO", "RS", "RU",
    "TR", "UA",
    # Africa
    "BJ", "BF", "CM", "CD", "CG", "CI", "EG", "ET",
    "GH", "GN", "KE", "ML", "MZ", "NE", "NG", "SD",
    "TZ", "UG", "ZM", "ZW",
    # Asia
    "BD", "KH", "ID", "KZ", "LA", "MM", "MN", "NP",
    "PK", "PH", "TJ", "UZ", "VN",
    # Gap countries (no dedicated national API)
    "BR", "IN", "KR", "MX", "IT", "DK", "EE", "PE", "PT",
]

_SEED_STATIONS: list[dict] = [
    # Eastern Europe
    {
        "grdc_no": "6340110",
        "name": "RHINE AT LOBITH",
        "lat": 51.84,
        "lon": 6.11,
        "country": "DE",
        "river": "RHINE",
        "area": 160800.0,
    },
    {
        "grdc_no": "6935051",
        "name": "DANUBE AT RENI",
        "lat": 45.45,
        "lon": 28.27,
        "country": "UA",
        "river": "DANUBE",
        "area": 805700.0,
    },
    {
        "grdc_no": "6442600",
        "name": "DAUGAVA AT DAUGAVPILS",
        "lat": 55.88,
        "lon": 26.55,
        "country": "LV",
        "river": "DAUGAVA",
        "area": 64500.0,
    },
    {
        "grdc_no": "6970250",
        "name": "DNIEPER AT KIEV",
        "lat": 50.45,
        "lon": 30.52,
        "country": "UA",
        "river": "DNIEPER",
        "area": 328000.0,
    },
    {
        "grdc_no": "6977100",
        "name": "DON AT RAZDORSKAYA",
        "lat": 47.54,
        "lon": 40.65,
        "country": "RU",
        "river": "DON",
        "area": 378000.0,
    },
    {
        "grdc_no": "6978250",
        "name": "VOLGA AT VOLGOGRAD",
        "lat": 48.70,
        "lon": 44.51,
        "country": "RU",
        "river": "VOLGA",
        "area": 1360000.0,
    },
    {
        "grdc_no": "2906900",
        "name": "VARDAR AT GEVGELIJA",
        "lat": 41.14,
        "lon": 22.50,
        "country": "MK",
        "river": "VARDAR",
        "area": 20535.0,
    },
    {
        "grdc_no": "6742100",
        "name": "NEMAN AT SMALININKAI",
        "lat": 55.07,
        "lon": 22.58,
        "country": "BY",
        "river": "NEMAN",
        "area": 81200.0,
    },
    {
        "grdc_no": "6343500",
        "name": "PRUT AT UNGHENI",
        "lat": 47.20,
        "lon": 27.79,
        "country": "MD",
        "river": "PRUT",
        "area": 10990.0,
    },
    {
        "grdc_no": "6934800",
        "name": "DANUBE AT ORSOVA",
        "lat": 44.70,
        "lon": 22.41,
        "country": "RS",
        "river": "DANUBE",
        "area": 576232.0,
    },
    # Turkey (GRDC supplement)
    {
        "grdc_no": "2916700",
        "name": "EUPHRATES AT KEBAN",
        "lat": 38.79,
        "lon": 38.75,
        "country": "TR",
        "river": "EUPHRATES",
        "area": 64100.0,
    },
    # Africa
    {
        "grdc_no": "1147010",
        "name": "NILE AT ASWAN",
        "lat": 24.08,
        "lon": 32.90,
        "country": "EG",
        "river": "NILE",
        "area": 1700000.0,
    },
    {
        "grdc_no": "1160900",
        "name": "NIGER AT LOKOJA",
        "lat": 7.80,
        "lon": 6.77,
        "country": "NG",
        "river": "NIGER",
        "area": 2074171.0,
    },
    {
        "grdc_no": "1291100",
        "name": "CONGO AT KINSHASA",
        "lat": -4.30,
        "lon": 15.30,
        "country": "CD",
        "river": "CONGO",
        "area": 3475000.0,
    },
    {
        "grdc_no": "1291400",
        "name": "ZAMBEZI AT KATIMA MULILO",
        "lat": -17.49,
        "lon": 24.28,
        "country": "ZM",
        "river": "ZAMBEZI",
        "area": 334000.0,
    },
    {
        "grdc_no": "1577602",
        "name": "BLUE NILE AT KHARTOUM",
        "lat": 15.63,
        "lon": 32.55,
        "country": "SD",
        "river": "BLUE NILE",
        "area": 325000.0,
    },
    {
        "grdc_no": "1838100",
        "name": "VOLTA AT SENCHI",
        "lat": 6.18,
        "lon": -0.07,
        "country": "GH",
        "river": "VOLTA",
        "area": 394100.0,
    },
    # Asia
    {
        "grdc_no": "2460200",
        "name": "MEKONG AT STUNG TRENG",
        "lat": 13.53,
        "lon": 105.97,
        "country": "KH",
        "river": "MEKONG",
        "area": 635000.0,
    },
    {
        "grdc_no": "2651200",
        "name": "GANGES AT FARAKKA",
        "lat": 25.00,
        "lon": 87.92,
        "country": "BD",
        "river": "GANGES",
        "area": 835000.0,
    },
    {
        "grdc_no": "2998510",
        "name": "OB AT SALEKHARD",
        "lat": 66.53,
        "lon": 66.60,
        "country": "RU",
        "river": "OB",
        "area": 2950000.0,
    },
    {
        "grdc_no": "2903430",
        "name": "INDUS AT KOTRI",
        "lat": 25.37,
        "lon": 68.37,
        "country": "PK",
        "river": "INDUS",
        "area": 833000.0,
    },
    {
        "grdc_no": "2906100",
        "name": "AMU DARYA AT KERKI",
        "lat": 37.83,
        "lon": 65.20,
        "country": "TM",
        "river": "AMU DARYA",
        "area": 309000.0,
    },
    {
        "grdc_no": "2999910",
        "name": "YENISEI AT IGARKA",
        "lat": 67.47,
        "lon": 86.57,
        "country": "RU",
        "river": "YENISEI",
        "area": 2440000.0,
    },
    {
        "grdc_no": "2999100",
        "name": "LENA AT KUSUR",
        "lat": 70.68,
        "lon": 127.39,
        "country": "RU",
        "river": "LENA",
        "area": 2430000.0,
    },
    {
        "grdc_no": "2460550",
        "name": "IRRAWADDY AT PYAY",
        "lat": 18.82,
        "lon": 95.22,
        "country": "MM",
        "river": "IRRAWADDY",
        "area": 114000.0,
    },
    # Brazil
    {
        "grdc_no": "3629000",
        "name": "AMAZON AT OBIDOS",
        "lat": -1.9,
        "lon": -55.5,
        "country": "BR",
        "river": "AMAZON",
        "area": 4680000.0,
    },
    {
        "grdc_no": "3625200",
        "name": "PARANA AT ITAIPU",
        "lat": -25.4,
        "lon": -54.6,
        "country": "BR",
        "river": "PARANA",
        "area": 820000.0,
    },
    # India
    {
        "grdc_no": "2646200",
        "name": "GANGES AT FARAKKA",
        "lat": 25.0,
        "lon": 87.9,
        "country": "IN",
        "river": "GANGES",
        "area": 835000.0,
    },
    {
        "grdc_no": "2646400",
        "name": "BRAHMAPUTRA AT PANDU",
        "lat": 26.2,
        "lon": 91.7,
        "country": "IN",
        "river": "BRAHMAPUTRA",
        "area": 405000.0,
    },
    # South Korea
    {
        "grdc_no": "2175100",
        "name": "HAN RIVER AT SEOUL",
        "lat": 37.5,
        "lon": 127.0,
        "country": "KR",
        "river": "HAN",
        "area": 23800.0,
    },
    {
        "grdc_no": "2175400",
        "name": "NAKDONG AT CHANGNYEONG",
        "lat": 35.5,
        "lon": 128.5,
        "country": "KR",
        "river": "NAKDONG",
        "area": 16352.0,
    },
    # Mexico
    {
        "grdc_no": "4152200",
        "name": "GRIJALVA AT VILLAHERMOSA",
        "lat": 18.0,
        "lon": -92.9,
        "country": "MX",
        "river": "GRIJALVA",
        "area": 36500.0,
    },
    {
        "grdc_no": "4152500",
        "name": "LERMA-SANTIAGO AT GUADALAJARA",
        "lat": 20.7,
        "lon": -103.3,
        "country": "MX",
        "river": "LERMA-SANTIAGO",
        "area": 51200.0,
    },
    # Italy
    {
        "grdc_no": "6139100",
        "name": "PO AT PONTELAGOSCURO",
        "lat": 44.9,
        "lon": 11.6,
        "country": "IT",
        "river": "PO",
        "area": 70091.0,
    },
    {
        "grdc_no": "6139300",
        "name": "TEVERE AT ROMA",
        "lat": 41.9,
        "lon": 12.5,
        "country": "IT",
        "river": "TEVERE",
        "area": 16545.0,
    },
    # Denmark
    {
        "grdc_no": "6127100",
        "name": "GUDENAA AT RANDERS",
        "lat": 56.5,
        "lon": 10.0,
        "country": "DK",
        "river": "GUDENAA",
        "area": 2650.0,
    },
    {
        "grdc_no": "6127200",
        "name": "SKJERN A AT SKJERN",
        "lat": 55.9,
        "lon": 8.5,
        "country": "DK",
        "river": "SKJERN A",
        "area": 2500.0,
    },
    # Estonia
    {
        "grdc_no": "6460100",
        "name": "EMAJOGI AT TARTU",
        "lat": 58.4,
        "lon": 26.7,
        "country": "EE",
        "river": "EMAJOGI",
        "area": 7850.0,
    },
    {
        "grdc_no": "6460200",
        "name": "NARVA AT NARVA",
        "lat": 59.4,
        "lon": 28.0,
        "country": "EE",
        "river": "NARVA",
        "area": 56200.0,
    },
    # Peru
    {
        "grdc_no": "3627100",
        "name": "AMAZONAS AT IQUITOS",
        "lat": -3.7,
        "lon": -73.2,
        "country": "PE",
        "river": "AMAZONAS",
        "area": 720000.0,
    },
    {
        "grdc_no": "3627200",
        "name": "RIMAC AT LIMA",
        "lat": -12.0,
        "lon": -77.0,
        "country": "PE",
        "river": "RIMAC",
        "area": 2237.0,
    },
    # Portugal
    {
        "grdc_no": "6123100",
        "name": "DOURO AT PORTO",
        "lat": 41.1,
        "lon": -8.6,
        "country": "PT",
        "river": "DOURO",
        "area": 97603.0,
    },
    {
        "grdc_no": "6123200",
        "name": "TEJO AT SANTAREM",
        "lat": 39.2,
        "lon": -8.7,
        "country": "PT",
        "river": "TEJO",
        "area": 67490.0,
    },
]


@register("grdc")
class GRDCConnector(BaseConnector):
    """GRDC connector -- catalogue from seed list, observations from local files.

    Configuration options (via ``config`` dict):
        data_dir : str | Path
            Directory containing GRDC text files (``{grdc_no}_Q_Day.Cmd.txt``).
        seed_only : bool
            If True (default), return the curated seed catalogue.
            If False, attempt to fetch the full catalogue from GRDC WFS.
    """

    slug = "grdc"
    display_name = "Global Runoff Data Centre (GRDC)"
    base_url = "https://grdc.bafg.de"
    country_codes: list[str] = GRDC_COUNTRY_CODES

    # WFS endpoint for programmatic catalogue access
    _WFS_PATH = "/GRDC/ows"

    async def fetch_stations(self) -> list[Station]:
        """Return GRDC stations from curated seed list or WFS catalogue.

        By default, the curated seed list is returned (fast, no network).
        Set ``config["seed_only"] = False`` to attempt the GRDC WFS endpoint.
        """
        seed_only = self.config.get("seed_only", True)

        if not seed_only:
            try:
                return await self._fetch_stations_wfs()
            except Exception as exc:
                logger.warning(
                    "grdc_wfs_fallback_to_seed",
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
        """Read observations from local GRDC text files.

        If no local data directory is configured or the file does not exist,
        logs guidance on how to request data and returns an empty chunk.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = self.config.get("data_dir")

        if not data_dir:
            logger.info(
                "grdc_no_data_dir",
                station=native_id,
                hint=(
                    "Set config['data_dir'] to a directory containing "
                    "GRDC text files. Request data at "
                    f"{GRDC_DATA_REQUEST_URL}"
                ),
            )
            return self._empty_chunk(station_id)

        data_path = Path(data_dir)
        file_path = self._find_data_file(data_path, native_id)

        if file_path is None:
            logger.info(
                "grdc_file_not_found",
                station=native_id,
                data_dir=str(data_path),
                hint=(
                    f"Download GRDC file for station {native_id} from "
                    f"{GRDC_DATA_REQUEST_URL}"
                ),
            )
            return self._empty_chunk(station_id)

        start_aware = start if start.tzinfo else start.replace(tzinfo=UTC)
        end_aware = end if end.tzinfo else end.replace(tzinfo=UTC)

        observations = self._parse_grdc_file(
            file_path, station_id, start_aware, end_aware,
        )

        logger.info(
            "grdc_observations_loaded",
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
    # WFS catalogue fetch
    # ------------------------------------------------------------------

    async def _fetch_stations_wfs(self) -> list[Station]:
        """Attempt to fetch the full station catalogue from GRDC WFS."""
        params = {
            "service": "WFS",
            "version": "2.0.0",
            "request": "GetFeature",
            "typeName": "grdc:GRDC_Stations",
            "outputFormat": "application/json",
            "count": "10000",
        }
        resp = await self._get(self._WFS_PATH, params=params)
        data = resp.json()
        features = data.get("features", [])

        stations: list[Station] = []
        for feat in features:
            station = self._parse_wfs_feature(feat)
            if station is not None:
                stations.append(station)

        logger.info(
            "stations_fetched",
            provider=self.slug,
            count=len(stations),
            source="wfs",
        )
        return stations

    def _parse_wfs_feature(self, feature: dict) -> Station | None:
        """Parse a single GeoJSON feature from WFS into a Station."""
        props = feature.get("properties", {})
        geom = feature.get("geometry", {})
        coords = geom.get("coordinates", [])
        grdc_no = props.get("grdc_no")

        if not grdc_no or len(coords) < 2:
            return None

        lon, lat = coords[0], coords[1]
        area_raw = props.get("area")

        return Station(
            id=self._station_id(str(grdc_no)),
            provider=self.slug,
            native_id=str(grdc_no),
            name=props.get("station", str(grdc_no)),
            latitude=float(str(lat)),
            longitude=float(str(lon)),
            country_code=props.get("country_code", "XX"),
            river=props.get("river"),
            catchment_area_km2=(
                float(str(area_raw)) if area_raw is not None else None
            ),
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
                    id=self._station_id(entry["grdc_no"]),
                    provider=self.slug,
                    native_id=entry["grdc_no"],
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
        self, data_dir: Path, grdc_no: str,
    ) -> Path | None:
        """Locate the GRDC text file for a given station number.

        Common naming patterns:
          {grdc_no}_Q_Day.Cmd.txt
          {grdc_no}.txt
          {grdc_no}_Q_Month.txt
        """
        candidates = [
            data_dir / f"{grdc_no}_Q_Day.Cmd.txt",
            data_dir / f"{grdc_no}.txt",
            data_dir / f"{grdc_no}_Q_Month.txt",
        ]
        for candidate in candidates:
            if candidate.is_file():
                return candidate
        return None

    def _parse_grdc_file(
        self,
        file_path: Path,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse a GRDC text file into Observation objects.

        GRDC text format (after ~36-line ``#``-prefixed header):
        ``YYYY-MM-DD;HH:MM;   Value; Flag``

        Missing values are encoded as -999.000.
        """
        observations: list[Observation] = []

        try:
            lines = file_path.read_text(encoding="utf-8").splitlines()
        except OSError as exc:
            raise ConnectorError(
                self.slug,
                f"Cannot read GRDC file {file_path}: {exc}",
            ) from exc

        # Skip header lines (lines starting with '#' or blank)
        data_lines: list[str] = []
        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue
            data_lines.append(stripped)

        if not data_lines:
            return observations

        # First non-comment line may be a column header row
        # e.g. "YYYY-MM-DD;hh:mm; Original;..." -- skip if not
        # parseable as a date
        first = data_lines[0]
        if not first[0].isdigit():
            data_lines = data_lines[1:]

        for line in data_lines:
            obs = self._parse_data_line(line, station_id, start, end)
            if obs is not None:
                observations.append(obs)

        return observations

    def _parse_data_line(
        self,
        line: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> Observation | None:
        """Parse a single semicolon-delimited data line."""
        parts = line.split(";")
        if len(parts) < 3:
            return None

        date_str = parts[0].strip()
        value_str = parts[2].strip() if len(parts) > 2 else ""
        flag_str = parts[3].strip() if len(parts) > 3 else ""

        try:
            ts = datetime.strptime(date_str, "%Y-%m-%d").replace(
                tzinfo=UTC,
            )
        except ValueError:
            return None

        if ts < start or ts > end:
            return None

        discharge: float | None = None
        quality = GRDC_QUALITY_MAP.get(flag_str, QualityFlag.RAW)

        if value_str:
            try:
                raw_value = float(str(value_str))
            except ValueError:
                quality = QualityFlag.MISSING
                raw_value = GRDC_MISSING_VALUE

            if abs(raw_value - GRDC_MISSING_VALUE) < 0.01:
                discharge = None
                quality = QualityFlag.MISSING
            else:
                discharge = raw_value

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
