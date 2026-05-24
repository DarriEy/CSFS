# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Nepal ICIMOD RDS connector — discharge stations in Nepal.

ICIMOD's Regional Database System (RDS) hosts discharge data for
14 stations across Nepal's major river basins (Koshi, Narayani/
Gandaki, Karnali, Bagmati, and others).  The data is freely
downloadable from https://rds.icimod.org.

This connector supports two modes:

1. **API/web fetch** — attempts to download data from the ICIMOD
   RDS portal using known metadata IDs.

2. **Local CSV files** — reads downloaded ICIMOD CSV files from a
   local directory configured via ``config["data_dir"]``.

If neither web endpoints nor local files are available, empty chunks
with download guidance are returned.
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
# Constants
# ---------------------------------------------------------------------------
ICIMOD_DATASET_URL = (
    "https://rds.icimod.org/home/datadetail?metadataid=34423"
)
ICIMOD_METADATA_ID = "34423"

# ---------------------------------------------------------------------------
# Curated seed catalogue of 14 ICIMOD Nepal discharge stations
# ---------------------------------------------------------------------------
_SEED_STATIONS: list[dict] = [
    # Koshi basin
    {
        "native_id": "koshi_chatara",
        "name": "Chatara (Koshi)",
        "lat": 26.867,
        "lon": 87.157,
        "river": "Koshi",
        "area": 54100.0,
    },
    {
        "native_id": "koshi_barahkshetra",
        "name": "Barahkshetra (Koshi)",
        "lat": 26.872,
        "lon": 87.155,
        "river": "Koshi",
        "area": 54000.0,
    },
    {
        "native_id": "tamor_mulghat",
        "name": "Mulghat (Tamor)",
        "lat": 26.952,
        "lon": 87.320,
        "river": "Tamor",
        "area": 5900.0,
    },
    {
        "native_id": "dudhkoshi_rabuwa",
        "name": "Rabuwa Bazar (Dudh Koshi)",
        "lat": 27.272,
        "lon": 86.638,
        "river": "Dudh Koshi",
        "area": 4100.0,
    },
    # Narayani / Gandaki basin
    {
        "native_id": "narayani_devghat",
        "name": "Devghat (Narayani)",
        "lat": 27.708,
        "lon": 84.433,
        "river": "Narayani",
        "area": 31100.0,
    },
    {
        "native_id": "kaligandaki_seti",
        "name": "Setibeni (Kali Gandaki)",
        "lat": 28.117,
        "lon": 83.483,
        "river": "Kali Gandaki",
        "area": 7600.0,
    },
    {
        "native_id": "marsyangdi_bimalnagar",
        "name": "Bimalnagar (Marsyangdi)",
        "lat": 27.943,
        "lon": 84.442,
        "river": "Marsyangdi",
        "area": 4100.0,
    },
    {
        "native_id": "budhigandaki_arughat",
        "name": "Arughat (Budhi Gandaki)",
        "lat": 28.052,
        "lon": 84.819,
        "river": "Budhi Gandaki",
        "area": 3800.0,
    },
    # Karnali basin
    {
        "native_id": "karnali_chisapani",
        "name": "Chisapani (Karnali)",
        "lat": 28.643,
        "lon": 81.290,
        "river": "Karnali",
        "area": 43900.0,
    },
    {
        "native_id": "bheri_jamu",
        "name": "Jamu (Bheri)",
        "lat": 28.833,
        "lon": 81.383,
        "river": "Bheri",
        "area": 12100.0,
    },
    # Bagmati basin
    {
        "native_id": "bagmati_pandheradovan",
        "name": "Pandheradovan (Bagmati)",
        "lat": 27.583,
        "lon": 85.217,
        "river": "Bagmati",
        "area": 2700.0,
    },
    {
        "native_id": "bagmati_karmaiya",
        "name": "Karmaiya (Bagmati)",
        "lat": 27.117,
        "lon": 85.533,
        "river": "Bagmati",
        "area": 3200.0,
    },
    # Babai and Rapti
    {
        "native_id": "babai_chepang",
        "name": "Chepang (Babai)",
        "lat": 28.350,
        "lon": 81.667,
        "river": "Babai",
        "area": 3400.0,
    },
    {
        "native_id": "rapti_jalkundi",
        "name": "Jalkundi (West Rapti)",
        "lat": 27.900,
        "lon": 82.583,
        "river": "West Rapti",
        "area": 6200.0,
    },
]


@register("nepal_icimod")
class NepalICIMODConnector(BaseConnector):
    """Connector for ICIMOD RDS Nepal discharge stations.

    Configuration options (via ``config`` dict):
        data_dir : str | Path
            Directory containing downloaded ICIMOD CSV files.
    """

    slug = "nepal_icimod"
    display_name = "ICIMOD RDS Nepal"
    base_url = "https://rds.icimod.org"
    country_codes: list[str] = ["NP"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return seed list of ICIMOD Nepal discharge stations."""
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
        """Fetch observations via ICIMOD API or local CSV files.

        Tries the ICIMOD RDS API first, then local CSV fallback.
        Returns empty chunk with guidance if none work.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        # Try ICIMOD RDS API endpoint
        chunk = await self._try_fetch_api(
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
            "nepal_icimod_no_data_source",
            station=native_id,
            hint=(
                "Download ICIMOD discharge data from "
                f"{ICIMOD_DATASET_URL} and set "
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
                    country_code="NP",
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
    # ICIMOD RDS API
    # ------------------------------------------------------------------

    async def _try_fetch_api(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk | None:
        """Try fetching from ICIMOD RDS API."""
        try:
            resp = await self._get(
                "/Home/DataDetail",
                params={
                    "metadataId": ICIMOD_METADATA_ID,
                    "station": native_id,
                    "from": start.strftime("%Y-%m-%d"),
                    "to": end.strftime("%Y-%m-%d"),
                },
            )
            data = resp.json()
            return self._parse_api_response(
                data, station_id,
            )
        except (ConnectorError, Exception) as exc:
            logger.warning(
                "icimod_api_failed",
                provider=self.slug,
                station=native_id,
                error=str(exc),
            )
            return None

    def _parse_api_response(
        self,
        data: dict | list,
        station_id: str,
    ) -> TimeSeriesChunk | None:
        """Parse ICIMOD API JSON response."""
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

        observations = self._parse_obs_entries(obs_list, station_id)
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
    ) -> list[Observation]:
        """Parse observation entries from API response."""
        observations: list[Observation] = []
        for entry in entries:
            try:
                ts = self._parse_timestamp(entry)
                if ts is None:
                    continue

                value = (
                    entry.get("discharge")
                    or entry.get("value")
                    or entry.get("discharge_m3s")
                )
                discharge = (
                    float(str(value))
                    if value is not None
                    else None
                )

                quality_raw = str(
                    entry.get("quality", ""),
                ).lower().strip()
                quality = self._map_quality(
                    quality_raw, discharge,
                )

                observations.append(
                    Observation(
                        station_id=station_id,
                        timestamp=ts,
                        discharge_m3s=discharge,
                        quality=quality,
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
        """Try reading observations from local ICIMOD CSV files."""
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
            obs = self._parse_icimod_csv(
                csv_file, native_id, station_id,
                start_aware, end_aware,
            )
            observations.extend(obs)

        if not observations:
            return None

        observations.sort(key=lambda o: o.timestamp)

        logger.info(
            "nepal_icimod_csv_loaded",
            station=native_id,
            count=len(observations),
        )

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    def _parse_icimod_csv(
        self,
        file_path: Path,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse an ICIMOD CSV file for a specific station."""
        observations: list[Observation] = []

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
        date_col = self._find_date_column(header)
        discharge_col = self._find_discharge_column(header)
        station_col = self._find_station_column(header)

        if date_col is None or discharge_col is None:
            return observations

        for line in lines[1:]:
            parts = line.strip().split(",")
            max_col = max(
                date_col, discharge_col,
                station_col if station_col is not None else 0,
            )
            if len(parts) <= max_col:
                continue

            # Filter by station if station column exists
            if station_col is not None:
                row_station = parts[station_col].strip().lower()
                if (
                    native_id.lower() not in row_station
                    and row_station not in native_id.lower()
                ):
                    continue

            obs = self._parse_csv_row(
                parts, date_col, discharge_col,
                station_id, start, end,
            )
            if obs is not None:
                observations.append(obs)

        return observations

    def _parse_csv_row(
        self,
        parts: list[str],
        date_col: int,
        discharge_col: int,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> Observation | None:
        """Parse a single CSV data row."""
        date_str = parts[date_col].strip()
        ts = self._parse_date(date_str)
        if ts is None:
            return None

        if ts < start or ts > end:
            return None

        val_str = parts[discharge_col].strip()
        discharge: float | None = None
        quality = QualityFlag.MISSING

        if val_str and val_str.lower() not in ("", "na", "nan", "-"):
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

    @staticmethod
    def _find_date_column(header: list[str]) -> int | None:
        """Find the date column index."""
        for i, col in enumerate(header):
            col_lower = col.strip().lower()
            if col_lower in (
                "date", "datetime", "timestamp", "time",
            ):
                return i
        return None

    @staticmethod
    def _find_discharge_column(header: list[str]) -> int | None:
        """Find the discharge column index."""
        for i, col in enumerate(header):
            col_lower = col.strip().lower()
            if col_lower in (
                "discharge", "discharge_m3s", "q_m3s", "flow",
                "discharge_cumecs", "value",
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
            entry.get("date")
            or entry.get("timestamp")
            or entry.get("dateTime")
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

        for fmt in ("%Y-%m-%d", "%d-%m-%Y", "%d/%m/%Y"):
            try:
                return datetime.strptime(raw_str, fmt)
            except ValueError:
                continue

        return None

    @staticmethod
    def _parse_date(date_str: str) -> datetime | None:
        """Try multiple date formats."""
        for fmt in (
            "%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y",
        ):
            try:
                return datetime.strptime(
                    date_str, fmt,
                ).replace(tzinfo=UTC)
            except ValueError:
                continue
        return None

    @staticmethod
    def _map_quality(
        raw: str, discharge: float | None,
    ) -> QualityFlag:
        """Map quality string to QualityFlag."""
        mapping: dict[str, QualityFlag] = {
            "good": QualityFlag.GOOD,
            "suspect": QualityFlag.SUSPECT,
            "estimated": QualityFlag.ESTIMATED,
            "missing": QualityFlag.MISSING,
        }
        if raw in mapping:
            return mapping[raw]
        return (
            QualityFlag.RAW
            if discharge is not None
            else QualityFlag.MISSING
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
