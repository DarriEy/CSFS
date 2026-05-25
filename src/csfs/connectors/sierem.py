"""SIEREM (IRD African Hydrology) connector.

SIEREM (Systeme d'Informations Environnementales sur les Ressources en
Eau et leur Modelisation) is an IRD database of African hydrology
concentrated in francophone West and Central Africa.

- ~1,046 flow series from ORSTOM-era stations
- Available via IRD DataVerse (dataverse.ird.fr)
- DOI: 10.23708/L4XD4B

This connector follows the file-based pattern (similar to GRDC/Caravan):
stations are provided via a curated seed list; observations are read from
locally downloaded SIEREM data files.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

SIEREM_DOI = "10.23708/L4XD4B"
SIEREM_DATAVERSE_URL = "https://dataverse.ird.fr"
SIEREM_DATASET_API = (
    f"{SIEREM_DATAVERSE_URL}/api/datasets/:persistentId/"
    f"?persistentId=doi:{SIEREM_DOI}"
)
SIEREM_MISSING_VALUE = -999.0

SIEREM_COUNTRY_CODES: list[str] = [
    "BF", "BJ", "CF", "CG", "CI", "CM", "GA", "GN",
    "ML", "MR", "NE", "SN", "TD", "TG",
]

# ---------------------------------------------------------------------------
# Curated seed catalogue — ~40 major stations
# ---------------------------------------------------------------------------

_SEED_STATIONS: list[dict] = [
    # ---- Niger River basin ----
    {
        "native_id": "1134000103",
        "name": "NIGER A KOULIKORO",
        "lat": 12.87,
        "lon": -7.55,
        "country": "ML",
        "river": "NIGER",
        "area": 120000.0,
    },
    {
        "native_id": "1270010700",
        "name": "NIGER A DIRE",
        "lat": 16.27,
        "lon": -3.38,
        "country": "ML",
        "river": "NIGER",
        "area": 340000.0,
    },
    {
        "native_id": "1134500103",
        "name": "NIGER A NIAMEY",
        "lat": 13.52,
        "lon": -2.09,
        "country": "NE",
        "river": "NIGER",
        "area": 700000.0,
    },
    {
        "native_id": "1271100103",
        "name": "BANI A DOUNA",
        "lat": 13.22,
        "lon": -5.90,
        "country": "ML",
        "river": "BANI",
        "area": 101600.0,
    },
    {
        "native_id": "1271400103",
        "name": "BANI A SOFARA",
        "lat": 14.07,
        "lon": -4.23,
        "country": "ML",
        "river": "BANI",
        "area": 125400.0,
    },
    # ---- Senegal River basin ----
    {
        "native_id": "1130300105",
        "name": "SENEGAL A BAKEL",
        "lat": 14.90,
        "lon": -12.46,
        "country": "SN",
        "river": "SENEGAL",
        "area": 218000.0,
    },
    {
        "native_id": "1110400103",
        "name": "SENEGAL A KAYES",
        "lat": 14.45,
        "lon": -11.44,
        "country": "ML",
        "river": "SENEGAL",
        "area": 157400.0,
    },
    {
        "native_id": "1111200103",
        "name": "FALEME A KIDIRA",
        "lat": 14.47,
        "lon": -12.21,
        "country": "SN",
        "river": "FALEME",
        "area": 28900.0,
    },
    {
        "native_id": "1112000103",
        "name": "BAFING A DAKA SAIDOU",
        "lat": 11.95,
        "lon": -10.62,
        "country": "GN",
        "river": "BAFING",
        "area": 15700.0,
    },
    # ---- Volta River basin ----
    {
        "native_id": "1240100103",
        "name": "VOLTA NOIRE A BOROMO",
        "lat": 11.75,
        "lon": -2.93,
        "country": "BF",
        "river": "VOLTA NOIRE",
        "area": 30100.0,
    },
    {
        "native_id": "1240200103",
        "name": "VOLTA BLANCHE A WAYEN",
        "lat": 12.38,
        "lon": -1.08,
        "country": "BF",
        "river": "VOLTA BLANCHE",
        "area": 20300.0,
    },
    {
        "native_id": "1240800103",
        "name": "MOUHOUN A SAMANDENI",
        "lat": 11.48,
        "lon": -4.07,
        "country": "BF",
        "river": "MOUHOUN",
        "area": 4580.0,
    },
    {
        "native_id": "1241100103",
        "name": "PENDJARI A PORGA",
        "lat": 11.05,
        "lon": 0.98,
        "country": "BJ",
        "river": "PENDJARI",
        "area": 22280.0,
    },
    # ---- Congo / Oubangui basin ----
    {
        "native_id": "1430100103",
        "name": "OUBANGUI A BANGUI",
        "lat": 4.37,
        "lon": 18.58,
        "country": "CF",
        "river": "OUBANGUI",
        "area": 500000.0,
    },
    {
        "native_id": "1147010004",
        "name": "CONGO A BRAZZAVILLE",
        "lat": -4.27,
        "lon": 15.28,
        "country": "CG",
        "river": "CONGO",
        "area": 3475000.0,
    },
    {
        "native_id": "1431200103",
        "name": "SANGHA A OUESSO",
        "lat": 1.62,
        "lon": 16.05,
        "country": "CG",
        "river": "SANGHA",
        "area": 158350.0,
    },
    {
        "native_id": "1431700103",
        "name": "LIKOUALA AUX HERBES A BOTOUALI",
        "lat": -0.90,
        "lon": 17.38,
        "country": "CG",
        "river": "LIKOUALA AUX HERBES",
        "area": 24800.0,
    },
    # ---- Chari / Logone basin ----
    {
        "native_id": "1300100103",
        "name": "CHARI A NDJAMENA",
        "lat": 12.12,
        "lon": 15.03,
        "country": "TD",
        "river": "CHARI",
        "area": 600000.0,
    },
    {
        "native_id": "1300700103",
        "name": "LOGONE A MOUNDOU",
        "lat": 8.57,
        "lon": 16.07,
        "country": "TD",
        "river": "LOGONE",
        "area": 33970.0,
    },
    {
        "native_id": "1301100103",
        "name": "LOGONE A BONGOR",
        "lat": 10.28,
        "lon": 15.37,
        "country": "TD",
        "river": "LOGONE",
        "area": 73700.0,
    },
    {
        "native_id": "1301500103",
        "name": "CHARI A SARH",
        "lat": 9.15,
        "lon": 18.38,
        "country": "TD",
        "river": "CHARI",
        "area": 193000.0,
    },
    # ---- Ivory Coast / Bandama / Comoe ----
    {
        "native_id": "1250100103",
        "name": "BANDAMA A BRIMBO",
        "lat": 6.57,
        "lon": -4.97,
        "country": "CI",
        "river": "BANDAMA",
        "area": 60200.0,
    },
    {
        "native_id": "1250400103",
        "name": "COMOE A ANIASSUE",
        "lat": 6.62,
        "lon": -3.72,
        "country": "CI",
        "river": "COMOE",
        "area": 66500.0,
    },
    {
        "native_id": "1250700103",
        "name": "SASSANDRA A GUESSABO",
        "lat": 6.30,
        "lon": -6.78,
        "country": "CI",
        "river": "SASSANDRA",
        "area": 29400.0,
    },
    # ---- Cameroon ----
    {
        "native_id": "1048700103",
        "name": "SANAGA A EDEA",
        "lat": 3.78,
        "lon": 10.07,
        "country": "CM",
        "river": "SANAGA",
        "area": 131500.0,
    },
    {
        "native_id": "1320500103",
        "name": "NYONG A MBALMAYO",
        "lat": 3.52,
        "lon": 11.50,
        "country": "CM",
        "river": "NYONG",
        "area": 13555.0,
    },
    {
        "native_id": "1321100103",
        "name": "BENOUE A GAROUA",
        "lat": 9.30,
        "lon": 13.40,
        "country": "CM",
        "river": "BENOUE",
        "area": 64000.0,
    },
    # ---- Gabon ----
    {
        "native_id": "1410100103",
        "name": "OGOOUE A LAMBARENE",
        "lat": -0.70,
        "lon": 10.22,
        "country": "GA",
        "river": "OGOOUE",
        "area": 203500.0,
    },
    {
        "native_id": "1410400103",
        "name": "OGOOUE A NDJOLE",
        "lat": -0.18,
        "lon": 10.77,
        "country": "GA",
        "river": "OGOOUE",
        "area": 158000.0,
    },
    # ---- Guinea ----
    {
        "native_id": "1200100103",
        "name": "KONKOURE A PONT DE TELIMELE",
        "lat": 10.90,
        "lon": -12.08,
        "country": "GN",
        "river": "KONKOURE",
        "area": 10250.0,
    },
    {
        "native_id": "1200400103",
        "name": "NIGER A FARANAH",
        "lat": 10.03,
        "lon": -10.75,
        "country": "GN",
        "river": "NIGER",
        "area": 3180.0,
    },
    # ---- Mauritania ----
    {
        "native_id": "1130100103",
        "name": "SENEGAL A MATAM",
        "lat": 15.65,
        "lon": -13.25,
        "country": "MR",
        "river": "SENEGAL",
        "area": 230000.0,
    },
    # ---- Niger (country) ----
    {
        "native_id": "1270900103",
        "name": "NIGER A MALANVILLE",
        "lat": 11.87,
        "lon": 3.39,
        "country": "NE",
        "river": "NIGER",
        "area": 1000000.0,
    },
    {
        "native_id": "1271700103",
        "name": "SIRBA A GARBE-KOUROU",
        "lat": 13.74,
        "lon": 1.59,
        "country": "NE",
        "river": "SIRBA",
        "area": 38750.0,
    },
    # ---- Senegal (country) ----
    {
        "native_id": "1120100103",
        "name": "GAMBIE A KEDOUGOU",
        "lat": 12.55,
        "lon": -12.18,
        "country": "SN",
        "river": "GAMBIE",
        "area": 7540.0,
    },
    {
        "native_id": "1120400103",
        "name": "CASAMANCE A KOLDA",
        "lat": 12.88,
        "lon": -14.95,
        "country": "SN",
        "river": "CASAMANCE",
        "area": 3700.0,
    },
    # ---- Togo ----
    {
        "native_id": "1260100103",
        "name": "MONO A NANGBETO",
        "lat": 7.42,
        "lon": 1.45,
        "country": "TG",
        "river": "MONO",
        "area": 15700.0,
    },
    {
        "native_id": "1260400103",
        "name": "OTI A MANGO",
        "lat": 10.37,
        "lon": 0.47,
        "country": "TG",
        "river": "OTI",
        "area": 35650.0,
    },
    # ---- Burkina Faso (additional) ----
    {
        "native_id": "1240500103",
        "name": "NAKAMBE A WAYEN",
        "lat": 12.38,
        "lon": -1.08,
        "country": "BF",
        "river": "NAKAMBE",
        "area": 20800.0,
    },
    # ---- Benin (additional) ----
    {
        "native_id": "1241400103",
        "name": "OUEME A SAVE",
        "lat": 8.03,
        "lon": 2.48,
        "country": "BJ",
        "river": "OUEME",
        "area": 23600.0,
    },
]

# ---------------------------------------------------------------------------
# Quality flag mapping for SIEREM data files
# ---------------------------------------------------------------------------
# SIEREM files typically use numeric flags similar to ORSTOM conventions.
_SIEREM_QUALITY_MAP: dict[str | None, QualityFlag] = {
    None: QualityFlag.RAW,
    "": QualityFlag.RAW,
    "0": QualityFlag.GOOD,
    "1": QualityFlag.ESTIMATED,
    "2": QualityFlag.SUSPECT,
    "3": QualityFlag.MISSING,
}


@register("sierem")
class SIEREMConnector(BaseConnector):
    """SIEREM connector -- catalogue from seed list, observations from files.

    Configuration options (via ``config`` dict):
        data_dir : str | Path
            Directory containing downloaded SIEREM data files.
        verify_doi : bool
            If True, queries the DataVerse API to verify the dataset
            record on ``fetch_stations``.  Defaults to False.
    """

    slug = "sierem"
    display_name = "SIEREM (IRD African Hydrology)"
    base_url = "https://dataverse.ird.fr"
    country_codes: list[str] = SIEREM_COUNTRY_CODES

    # ------------------------------------------------------------------
    # Station catalogue
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return SIEREM stations from the curated seed list.

        Optionally verifies the IRD DataVerse dataset record when
        ``config["verify_doi"]`` is True.
        """
        if self.config.get("verify_doi", False):
            await self._verify_dataverse_record()

        stations = self._build_seed_stations()
        logger.info(
            "stations_fetched",
            provider=self.slug,
            count=len(stations),
            source="seed",
        )
        return stations

    # ------------------------------------------------------------------
    # Observations (file-based)
    # ------------------------------------------------------------------

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Read observations from local SIEREM data files.

        If no ``data_dir`` is configured or the file is missing, logs
        download instructions pointing to IRD DataVerse and returns an
        empty chunk.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = self.config.get("data_dir")

        if not data_dir:
            logger.info(
                "sierem_no_data_dir",
                station=native_id,
                hint=(
                    "Set config['data_dir'] to a directory containing "
                    "downloaded SIEREM data files. Download from "
                    f"{SIEREM_DATAVERSE_URL} (DOI: {SIEREM_DOI})"
                ),
            )
            return self._empty_chunk(station_id)

        data_path = Path(data_dir)
        file_path = self._find_data_file(data_path, native_id)

        if file_path is None:
            logger.info(
                "sierem_file_not_found",
                station=native_id,
                data_dir=str(data_path),
                hint=(
                    f"Download SIEREM file for station {native_id} from "
                    f"{SIEREM_DATAVERSE_URL} (DOI: {SIEREM_DOI})"
                ),
            )
            return self._empty_chunk(station_id)

        start_aware = start if start.tzinfo else start.replace(tzinfo=UTC)
        end_aware = end if end.tzinfo else end.replace(tzinfo=UTC)

        observations = self._parse_sierem_file(
            file_path, station_id, start_aware, end_aware,
        )

        logger.info(
            "sierem_observations_loaded",
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
    # DataVerse API verification
    # ------------------------------------------------------------------

    async def _verify_dataverse_record(self) -> None:
        """Query IRD DataVerse API to verify the SIEREM dataset record."""
        try:
            resp = await self._get(
                "/api/datasets/:persistentId/",
                params={"persistentId": f"doi:{SIEREM_DOI}"},
            )
            data = resp.json()
            status = data.get("status", "UNKNOWN")
            logger.info(
                "sierem_dataverse_verified",
                status=status,
                doi=SIEREM_DOI,
            )
        except Exception as exc:
            logger.warning(
                "sierem_dataverse_verify_failed",
                error=str(exc),
                doi=SIEREM_DOI,
            )

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
    # Local file parsing
    # ------------------------------------------------------------------

    def _find_data_file(
        self, data_dir: Path, native_id: str,
    ) -> Path | None:
        """Locate the SIEREM data file for a given station.

        Common naming patterns:
          {native_id}.csv
          {native_id}.txt
          {native_id}_Q.csv
        """
        candidates = [
            data_dir / f"{native_id}.csv",
            data_dir / f"{native_id}.txt",
            data_dir / f"{native_id}_Q.csv",
        ]
        for candidate in candidates:
            if candidate.is_file():
                return candidate
        return None

    def _parse_sierem_file(
        self,
        file_path: Path,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse a SIEREM data file into Observation objects.

        Expected CSV/tab format with columns: date, discharge, [flag]
        Lines starting with '#' are skipped as comments.
        Delimiter is auto-detected (comma, semicolon, or tab).
        """
        try:
            lines = file_path.read_text(encoding="utf-8").splitlines()
        except OSError as exc:
            raise ConnectorError(
                self.slug,
                f"Cannot read SIEREM file {file_path}: {exc}",
            ) from exc

        observations: list[Observation] = []
        delimiter: str | None = None

        for line in lines:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue

            # Auto-detect delimiter from first data-like line
            if delimiter is None:
                delimiter = self._detect_delimiter(stripped)

            # Skip header rows
            if not stripped[0].isdigit():
                continue

            obs = self._parse_data_line(
                stripped, delimiter, station_id, start, end,
            )
            if obs is not None:
                observations.append(obs)

        return observations

    def _parse_data_line(
        self,
        line: str,
        delimiter: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> Observation | None:
        """Parse a single delimited data line."""
        parts = line.split(delimiter)
        if len(parts) < 2:
            return None

        date_str = parts[0].strip()
        value_str = parts[1].strip()
        flag_str = parts[2].strip() if len(parts) > 2 else ""

        ts = self._parse_date(date_str)
        if ts is None:
            return None

        if ts < start or ts > end:
            return None

        discharge: float | None = None
        quality = _SIEREM_QUALITY_MAP.get(flag_str, QualityFlag.RAW)

        if value_str:
            try:
                raw_value = float(str(value_str))
            except ValueError:
                quality = QualityFlag.MISSING
                raw_value = SIEREM_MISSING_VALUE

            if abs(raw_value - SIEREM_MISSING_VALUE) < 0.01:
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

    @staticmethod
    def _parse_date(date_str: str) -> datetime | None:
        """Parse a date string, trying common SIEREM formats."""
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(date_str, fmt).replace(
                    tzinfo=UTC,
                )
            except ValueError:
                continue
        return None

    @staticmethod
    def _detect_delimiter(line: str) -> str:
        """Auto-detect CSV delimiter from a data line."""
        for delim in (";", ",", "\t"):
            if delim in line:
                return delim
        return ","

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
