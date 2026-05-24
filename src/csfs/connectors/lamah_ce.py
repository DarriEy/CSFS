"""LamaH-CE connector — Central European large-sample hydrology dataset.

LamaH-CE (Large-Sample Data for Hydrology and Environmental Sciences for
Central Europe) is a research dataset hosted on Zenodo (record 5153305).
It covers 859 gauged basins across Central Europe (primarily the Upper
Danube region in Austria, Germany, and the Czech Republic), with daily
and hourly resolution over 35+ years.

This connector operates in two modes:

1. **Station catalogue** -- a curated seed list of ~30 representative
   LamaH basins is always available.  Optionally queries the Zenodo API
   for record metadata.

2. **Observations from local files** -- LamaH CSV files are
   semicolon-delimited with a ``date`` column and a ``qobs`` (discharge
   m3/s) column.  Files are read from ``config["data_dir"]``.
   If no local files are found, the connector logs download instructions
   pointing to the Zenodo record.
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
ZENODO_RECORD_ID = "5153305"
ZENODO_RECORD_URL = (
    f"https://zenodo.org/records/{ZENODO_RECORD_ID}"
)

# ---------------------------------------------------------------------------
# Curated seed stations — representative LamaH-CE basins
# Format: (gauge_id, name, lat, lon, country, river, area_km2)
# ---------------------------------------------------------------------------
_SEED_STATIONS: list[
    tuple[str, str, float, float, str, str, float | None]
] = [
    ("1", "Achleiten", 48.58, 13.50, "AT", "Danube", 76653.0),
    ("10", "Wasserburg", 48.06, 12.23, "DE", "Inn", 11983.0),
    ("50", "Passau-Ingling", 48.58, 13.46, "DE", "Inn", 26063.0),
    ("79", "Hofkirchen", 48.68, 13.12, "DE", "Danube", 47496.0),
    ("105", "Bratislava", 48.14, 17.11, "AT", "Danube", 131338.0),
    ("120", "Linz", 48.31, 14.29, "AT", "Danube", 79490.0),
    ("140", "Wien-Nussdorf", 48.24, 16.36, "AT", "Danube", 101731.0),
    ("160", "Kienstock", 48.39, 15.47, "AT", "Danube", 95970.0),
    ("200", "Steyr", 48.04, 14.42, "AT", "Steyr", 1431.0),
    ("210", "Wels", 48.16, 14.03, "AT", "Traun", 3426.0),
    ("230", "Gmunden", 47.92, 13.80, "AT", "Traun", 1576.0),
    ("250", "Bad Ischl", 47.71, 13.63, "AT", "Traun", 794.0),
    ("300", "Steyregg", 48.29, 14.36, "AT", "Aist", 619.0),
    ("320", "Steyr-Ortskai", 48.04, 14.42, "AT", "Enns", 5963.0),
    ("350", "Liezen", 47.57, 14.24, "AT", "Enns", 2060.0),
    ("400", "Korneuburg", 48.36, 16.33, "AT", "Danube", 96045.0),
    ("450", "Angern", 48.36, 16.93, "AT", "March", 26658.0),
    ("500", "Opava", 49.94, 17.90, "CZ", "Opava", 2037.0),
    ("520", "Olomouc", 49.59, 17.25, "CZ", "Morava", 3322.0),
    ("550", "Stramberk", 49.59, 18.11, "CZ", "Sedlnice", 38.7),
    ("600", "Rosenheim", 47.86, 12.13, "DE", "Mangfall", 1096.0),
    ("650", "Oberaudorf", 47.65, 12.17, "DE", "Inn", 9712.0),
    ("700", "Burghausen", 48.17, 12.83, "DE", "Salzach", 6649.0),
    ("750", "Salzburg", 47.80, 13.04, "AT", "Salzach", 4256.0),
    ("800", "Golling", 47.60, 13.17, "AT", "Salzach", 3220.0),
    ("820", "Mittersill", 47.28, 12.48, "AT", "Salzach", 1133.0),
    ("850", "Bruck-Mur", 47.41, 15.28, "AT", "Mur", 6434.0),
    ("870", "Murau", 47.11, 14.17, "AT", "Mur", 1393.0),
    ("890", "Graz-Andritz", 47.10, 15.42, "AT", "Mur", 7483.0),
    ("900", "Leoben", 47.38, 15.10, "AT", "Mur", 4530.0),
]


@register("lamah_ce")
class LamaHCEConnector(BaseConnector):
    """Connector for LamaH-CE — local file-based with seed catalogue.

    Configuration options (via ``config`` dict):
        data_dir : str | Path
            Directory containing LamaH CSV files (``{gauge_id}.csv``).
    """

    slug = "lamah_ce"
    display_name = "LamaH-CE (Central Europe)"
    base_url = "https://zenodo.org/api"
    country_codes = ["AT", "DE", "CZ"]

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return LamaH-CE stations from the curated seed list.

        Optionally queries the Zenodo API for record metadata to
        verify dataset availability, but always returns the seed list.
        """
        stations = self._build_seed_stations()

        try:
            await self._check_zenodo_record()
        except Exception:
            logger.debug(
                "zenodo_metadata_check_skipped",
                provider=self.slug,
            )

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
        """Read observations from local LamaH CSV files.

        If no local data directory is configured or the file is not
        found, logs guidance on how to download from Zenodo and returns
        an empty TimeSeriesChunk.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = self.config.get("data_dir")

        if not data_dir:
            logger.info(
                "lamah_no_data_dir",
                station=native_id,
                hint=(
                    "Set config['data_dir'] to a directory containing "
                    "LamaH-CE CSV files. Download from "
                    f"{ZENODO_RECORD_URL}"
                ),
            )
            return self._empty_chunk(station_id)

        data_path = Path(data_dir)
        file_path = self._find_data_file(data_path, native_id)

        if file_path is None:
            logger.info(
                "lamah_file_not_found",
                station=native_id,
                data_dir=str(data_path),
                hint=(
                    f"Download LamaH-CE data for gauge {native_id} "
                    f"from {ZENODO_RECORD_URL}"
                ),
            )
            return self._empty_chunk(station_id)

        start_aware = (
            start if start.tzinfo else start.replace(tzinfo=UTC)
        )
        end_aware = (
            end if end.tzinfo else end.replace(tzinfo=UTC)
        )

        observations = self._parse_lamah_csv(
            file_path, station_id, start_aware, end_aware,
        )

        logger.info(
            "lamah_observations_loaded",
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

    # -----------------------------------------------------------------
    # Zenodo metadata check
    # -----------------------------------------------------------------

    async def _check_zenodo_record(self) -> dict:
        """Query Zenodo API to verify the LamaH-CE record exists.

        This is a lightweight metadata check, not a data download.
        """
        resp = await self._get(
            f"/records/{ZENODO_RECORD_ID}",
        )
        result: dict = resp.json()
        logger.debug(
            "zenodo_record_found",
            provider=self.slug,
            title=result.get("metadata", {}).get("title", ""),
        )
        return result

    # -----------------------------------------------------------------
    # Seed catalogue
    # -----------------------------------------------------------------

    def _build_seed_stations(self) -> list[Station]:
        """Build Station objects from the curated seed list."""
        stations: list[Station] = []
        for entry in _SEED_STATIONS:
            (
                gauge_id, name, lat, lon,
                country, river, area,
            ) = entry
            stations.append(Station(
                id=self._station_id(gauge_id),
                provider=self.slug,
                native_id=gauge_id,
                name=name,
                latitude=float(str(lat)),
                longitude=float(str(lon)),
                country_code=country,
                river=river,
                catchment_area_km2=(
                    float(str(area))
                    if area is not None
                    else None
                ),
            ))
        return stations

    # -----------------------------------------------------------------
    # Local file parsing
    # -----------------------------------------------------------------

    def _find_data_file(
        self,
        data_dir: Path,
        gauge_id: str,
    ) -> Path | None:
        """Locate the LamaH CSV file for a given gauge ID.

        Common naming patterns:
          {gauge_id}.csv
          ID_{gauge_id}.csv
          {gauge_id}_daily.csv
        """
        candidates = [
            data_dir / f"{gauge_id}.csv",
            data_dir / f"ID_{gauge_id}.csv",
            data_dir / f"{gauge_id}_daily.csv",
        ]
        for candidate in candidates:
            if candidate.is_file():
                return candidate
        return None

    def _parse_lamah_csv(
        self,
        file_path: Path,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse a LamaH-CE CSV file into Observation objects.

        LamaH CSV format: semicolon-delimited, with a ``date`` column
        and a ``qobs`` (discharge m3/s) column.
        """
        observations: list[Observation] = []

        try:
            lines = file_path.read_text(
                encoding="utf-8",
            ).splitlines()
        except OSError as exc:
            raise ConnectorError(
                self.slug,
                f"Cannot read LamaH file {file_path}: {exc}",
            ) from exc

        if not lines:
            return observations

        # Detect delimiter
        delimiter = ";"
        if ";" not in lines[0] and "," in lines[0]:
            delimiter = ","

        # Parse header to find column indices
        header_parts = [
            h.strip().lower() for h in lines[0].split(delimiter)
        ]

        date_idx = self._find_col_index(
            header_parts, ("date", "datum", "yyyy-mm-dd"),
        )
        qobs_idx = self._find_col_index(
            header_parts, ("qobs", "q", "discharge", "caudal"),
        )

        # If header not recognized, try positional: col 0=date, col 1=qobs
        has_header = date_idx is not None
        if date_idx is None:
            date_idx = 0
        if qobs_idx is None:
            qobs_idx = 1

        start_line = 1 if has_header else 0

        for line in lines[start_line:]:
            stripped = line.strip()
            if not stripped or stripped.startswith("#"):
                continue

            obs = self._parse_data_line(
                stripped, delimiter, date_idx, qobs_idx,
                station_id, start, end,
            )
            if obs is not None:
                observations.append(obs)

        return observations

    def _parse_data_line(
        self,
        line: str,
        delimiter: str,
        date_idx: int,
        qobs_idx: int,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> Observation | None:
        """Parse a single data line from a LamaH CSV."""
        parts = line.split(delimiter)
        max_idx = max(date_idx, qobs_idx)
        if len(parts) <= max_idx:
            return None

        date_str = parts[date_idx].strip()
        value_str = parts[qobs_idx].strip()

        try:
            ts = self._parse_date(date_str)
        except ValueError:
            return None

        if ts < start or ts > end:
            return None

        discharge: float | None = None
        quality = QualityFlag.RAW

        if value_str and value_str.lower() not in ("na", "nan", "-"):
            try:
                discharge = float(str(value_str))
            except ValueError:
                quality = QualityFlag.MISSING

        if discharge is None:
            quality = QualityFlag.MISSING

        return Observation(
            station_id=station_id,
            timestamp=ts,
            discharge_m3s=discharge,
            quality=quality,
        )

    @staticmethod
    def _parse_date(date_str: str) -> datetime:
        """Parse a date string in common LamaH formats."""
        for fmt in ("%Y-%m-%d", "%Y%m%d", "%d.%m.%Y"):
            try:
                return datetime.strptime(
                    date_str, fmt,
                ).replace(tzinfo=UTC)
            except ValueError:
                continue
        raise ValueError(f"Unparseable date: {date_str}")

    @staticmethod
    def _find_col_index(
        header: list[str],
        candidates: tuple[str, ...],
    ) -> int | None:
        """Find the first matching column index."""
        for name in candidates:
            if name in header:
                return header.index(name)
        return None

    def _empty_chunk(self, station_id: str) -> TimeSeriesChunk:
        """Return an empty TimeSeriesChunk for a station."""
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )
