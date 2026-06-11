# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Pakistan IRSA/WAPDA connector — dam inflow/outflow and river flow data.

Pakistan's Indus River System Authority (IRSA) publishes daily river
inflow/outflow data for major dams and barrages.  WAPDA (Water and Power
Development Authority) also hosts some data on its website.

This connector supports three modes:

1. **Station catalogue** — a curated seed list of ~15 major dam/river
   stations across the Indus basin.

2. **Web scraping** — attempts to fetch daily tables from IRSA's
   ASP.NET page or WAPDA's river-flow endpoint.

3. **Local CSV files** — reads Kaggle-format CSV files from a local
   directory configured via ``config["data_dir"]``.  Columns are
   typically ``Date, Tarbela_Inflow, Mangla_Inflow, ...`` in cusecs;
   values are converted to m3/s using the exact factor 0.028316846592.

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
CUSEC_TO_M3S = 0.028316846592  # exact: 1 ft³ = 0.3048³ m³
IRSA_DATA_URL = "http://pakirsa.gov.pk/DailyData.aspx"
WAPDA_FLOW_URL = (
    "https://www.wapda.gov.pk/index.php/river-flow-data"
)

# ---------------------------------------------------------------------------
# Curated seed catalogue
# ---------------------------------------------------------------------------
_SEED_STATIONS: list[dict] = [
    {
        "native_id": "tarbela",
        "name": "Tarbela Dam",
        "lat": 34.089,
        "lon": 72.693,
        "river": "Indus",
        "area": 168000.0,
    },
    {
        "native_id": "mangla",
        "name": "Mangla Dam",
        "lat": 33.146,
        "lon": 73.645,
        "river": "Jhelum",
        "area": 33400.0,
    },
    {
        "native_id": "chashma",
        "name": "Chashma Barrage",
        "lat": 32.443,
        "lon": 71.380,
        "river": "Indus",
        "area": 214000.0,
    },
    {
        "native_id": "marala",
        "name": "Marala Headworks",
        "lat": 32.673,
        "lon": 74.459,
        "river": "Chenab",
        "area": 26900.0,
    },
    {
        "native_id": "balloki",
        "name": "Balloki Headworks",
        "lat": 31.221,
        "lon": 73.862,
        "river": "Ravi",
        "area": 8900.0,
    },
    {
        "native_id": "guddu",
        "name": "Guddu Barrage",
        "lat": 28.424,
        "lon": 69.727,
        "river": "Indus",
        "area": 633000.0,
    },
    {
        "native_id": "sukkur",
        "name": "Sukkur Barrage",
        "lat": 27.694,
        "lon": 68.857,
        "river": "Indus",
        "area": 689000.0,
    },
    {
        "native_id": "kotri",
        "name": "Kotri Barrage",
        "lat": 25.366,
        "lon": 68.311,
        "river": "Indus",
        "area": 833000.0,
    },
    {
        "native_id": "kalabagh",
        "name": "Kalabagh",
        "lat": 32.962,
        "lon": 71.546,
        "river": "Indus",
        "area": 218000.0,
    },
    {
        "native_id": "jinnah",
        "name": "Jinnah Barrage",
        "lat": 32.647,
        "lon": 71.541,
        "river": "Indus",
        "area": 219000.0,
    },
    {
        "native_id": "rasul",
        "name": "Rasul Barrage",
        "lat": 32.681,
        "lon": 73.517,
        "river": "Jhelum",
        "area": 35000.0,
    },
    {
        "native_id": "trimmu",
        "name": "Trimmu Headworks",
        "lat": 31.143,
        "lon": 72.157,
        "river": "Chenab",
        "area": 65000.0,
    },
    {
        "native_id": "panjnad",
        "name": "Panjnad Headworks",
        "lat": 29.340,
        "lon": 71.019,
        "river": "Panjnad",
        "area": 136000.0,
    },
    {
        "native_id": "islam",
        "name": "Islam Headworks",
        "lat": 29.398,
        "lon": 71.805,
        "river": "Sutlej",
        "area": 124000.0,
    },
]

# Mapping from Kaggle CSV column names to station native_id
_KAGGLE_COLUMN_MAP: dict[str, str] = {
    "Tarbela_Inflow": "tarbela",
    "Tarbela_Outflow": "tarbela",
    "Mangla_Inflow": "mangla",
    "Mangla_Outflow": "mangla",
    "Chashma_Inflow": "chashma",
    "Chashma_Outflow": "chashma",
    "Marala": "marala",
    "Balloki": "balloki",
    "Guddu": "guddu",
    "Sukkur": "sukkur",
    "Kotri": "kotri",
    "Kalabagh": "kalabagh",
    "Jinnah": "jinnah",
    "Rasul": "rasul",
    "Trimmu": "trimmu",
    "Panjnad": "panjnad",
    "Islam": "islam",
}


@register("pakistan_wapda")
class PakistanWAPDAConnector(BaseConnector):
    """Connector for Pakistan IRSA/WAPDA river flow data.

    Configuration options (via ``config`` dict):
        data_dir : str | Path
            Directory containing Kaggle CSV files with daily
            inflow/outflow data in cusecs.
    """

    slug = "pakistan_wapda"
    display_name = "Pakistan IRSA/WAPDA"
    base_url = "https://www.wapda.gov.pk"
    country_codes: list[str] = ["PK"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return seed list of major Pakistan dam/river stations."""
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
        """Fetch observations via web endpoints or local CSV files.

        Tries IRSA web tables first, then WAPDA, then local CSV
        fallback.  Returns empty chunk with guidance if none work.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        # Try IRSA web endpoint
        chunk = await self._try_fetch_irsa(
            native_id, station_id, start, end,
        )
        if chunk is not None:
            return chunk

        # Try WAPDA web endpoint
        chunk = await self._try_fetch_wapda(
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
            "pakistan_wapda_no_data_source",
            station=native_id,
            hint=(
                "Set config['data_dir'] to a directory containing "
                "Kaggle CSV files with Pakistan river flow data, "
                "or ensure IRSA/WAPDA endpoints are reachable."
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
                    country_code="PK",
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
    # IRSA web endpoint
    # ------------------------------------------------------------------

    async def _try_fetch_irsa(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk | None:
        """Try fetching from IRSA daily data ASP.NET form."""
        try:
            resp = await self._get(
                IRSA_DATA_URL,
                params={
                    "station": native_id,
                    "from": start.strftime("%Y-%m-%d"),
                    "to": end.strftime("%Y-%m-%d"),
                },
            )
            return self._parse_irsa_html(
                resp.text, station_id, start, end,
            )
        except (ConnectorError, Exception) as exc:
            logger.warning(
                "irsa_endpoint_failed",
                provider=self.slug,
                station=native_id,
                error=str(exc),
            )
            return None

    def _parse_irsa_html(
        self,
        html: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk | None:
        """Parse IRSA HTML table for discharge data.

        The IRSA page uses ASP.NET ViewState-based tables.  We look
        for rows containing date and flow values in cusecs.
        """
        observations: list[Observation] = []
        lines = html.splitlines()

        for line in lines:
            stripped = line.strip()
            if not stripped:
                continue
            # Look for table cell patterns with dates and values
            obs = self._parse_irsa_table_row(
                stripped, station_id, start, end,
            )
            if obs is not None:
                observations.append(obs)

        if not observations:
            return None

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    def _parse_irsa_table_row(
        self,
        row_text: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> Observation | None:
        """Try to extract a date and cusec value from HTML row text."""
        # Simple heuristic: look for date-like pattern and numeric val
        import re

        date_match = re.search(
            r"(\d{1,2})[/-](\d{1,2})[/-](\d{4})", row_text,
        )
        if not date_match:
            return None

        try:
            day = int(date_match.group(1))
            month = int(date_match.group(2))
            year = int(date_match.group(3))
            ts = datetime(year, month, day, tzinfo=UTC)
        except (ValueError, OverflowError):
            return None

        start_aware = (
            start if start.tzinfo else start.replace(tzinfo=UTC)
        )
        end_aware = (
            end if end.tzinfo else end.replace(tzinfo=UTC)
        )
        if ts < start_aware or ts > end_aware:
            return None

        # Look for numeric values (cusecs)
        nums = re.findall(r"[\d,]+\.?\d*", row_text)
        if len(nums) < 2:
            return None

        try:
            # Skip the date numbers, take first flow value
            cusec_str = nums[-1].replace(",", "")
            cusecs = float(str(cusec_str))
            discharge = cusecs * CUSEC_TO_M3S
        except (ValueError, IndexError):
            return None

        return Observation(
            station_id=station_id,
            timestamp=ts,
            discharge_m3s=discharge,
            quality=QualityFlag.RAW,
        )

    # ------------------------------------------------------------------
    # WAPDA web endpoint
    # ------------------------------------------------------------------

    async def _try_fetch_wapda(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk | None:
        """Try fetching from WAPDA river flow data page."""
        try:
            resp = await self._get(
                "/index.php/river-flow-data",
                params={"station": native_id},
            )
            return self._parse_irsa_html(
                resp.text, station_id, start, end,
            )
        except (ConnectorError, Exception) as exc:
            logger.warning(
                "wapda_endpoint_failed",
                provider=self.slug,
                station=native_id,
                error=str(exc),
            )
            return None

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
        """Try reading observations from local Kaggle CSV files.

        Expected CSV columns:
        ``Date, Tarbela_Inflow, Mangla_Inflow, ...`` in cusecs.
        """
        data_dir = self.config.get("data_dir")
        if not data_dir:
            return None

        data_path = Path(data_dir)
        if not data_path.is_dir():
            return None

        csv_files = list(data_path.glob("*.csv"))
        if not csv_files:
            return None

        observations: list[Observation] = []
        for csv_file in csv_files:
            obs = self._parse_kaggle_csv(
                csv_file, native_id, station_id, start, end,
            )
            observations.extend(obs)

        if not observations:
            return None

        observations.sort(key=lambda o: o.timestamp)

        logger.info(
            "pakistan_wapda_csv_loaded",
            station=native_id,
            count=len(observations),
        )

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    def _parse_kaggle_csv(
        self,
        file_path: Path,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse a Kaggle CSV file for a specific station."""
        observations: list[Observation] = []

        start_aware = (
            start if start.tzinfo else start.replace(tzinfo=UTC)
        )
        end_aware = (
            end if end.tzinfo else end.replace(tzinfo=UTC)
        )

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
        # Find columns for this station
        col_indices = self._find_station_columns(
            header, native_id,
        )
        if not col_indices:
            return observations

        date_col = self._find_date_column(header)
        if date_col is None:
            return observations

        for line in lines[1:]:
            parts = line.strip().split(",")
            if len(parts) <= max(date_col, max(col_indices)):
                continue

            obs = self._parse_csv_row(
                parts, date_col, col_indices,
                station_id, start_aware, end_aware,
            )
            if obs is not None:
                observations.append(obs)

        return observations

    def _find_station_columns(
        self, header: list[str], native_id: str,
    ) -> list[int]:
        """Find CSV column indices matching a station native ID."""
        indices: list[int] = []
        for i, col_name in enumerate(header):
            col_clean = col_name.strip()
            mapped = _KAGGLE_COLUMN_MAP.get(col_clean)
            if (
                mapped == native_id
                or col_clean.lower() == native_id.lower()
            ):
                indices.append(i)
        return indices

    def _find_date_column(
        self, header: list[str],
    ) -> int | None:
        """Find the date column index in a CSV header."""
        for i, col in enumerate(header):
            if col.strip().lower() in ("date", "datetime"):
                return i
        return None

    def _parse_csv_row(
        self,
        parts: list[str],
        date_col: int,
        value_cols: list[int],
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

        # Take the first non-empty value column (prefer inflow)
        discharge: float | None = None
        for col_idx in value_cols:
            val_str = parts[col_idx].strip()
            if val_str and val_str.lower() not in ("", "na", "nan"):
                try:
                    cusecs = float(str(val_str))
                    discharge = cusecs * CUSEC_TO_M3S
                    break
                except ValueError:
                    continue

        return Observation(
            station_id=station_id,
            timestamp=ts,
            discharge_m3s=discharge,
            quality=(
                QualityFlag.RAW
                if discharge is not None
                else QualityFlag.MISSING
            ),
        )

    @staticmethod
    def _parse_date(date_str: str) -> datetime | None:
        """Try multiple date formats common in Kaggle datasets."""
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%m/%d/%Y", "%d-%m-%Y"):
            try:
                return datetime.strptime(
                    date_str, fmt,
                ).replace(tzinfo=UTC)
            except ValueError:
                continue
        return None

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
