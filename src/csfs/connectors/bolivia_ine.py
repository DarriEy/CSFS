# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Bolivia INE connector — Instituto Nacional de Estadística.

Bolivia's INE hosts hydrological data through its NADA/Microdata
catalog (https://anda.ine.gob.bo).  The "Estadisticas de Caudales y
Niveles de Rios" dataset (catalog #209) covers 19 stations on 15
rivers, including Desaguadero, Beni, Mamoré, Pilcomayo, Grande,
Ichilo, and Chapare.

This connector supports three modes:

1. **NADA Catalog API** — tries to download data files from the
   catalog endpoint ``/index.php/catalog/{id}/download/{resource}``.

2. **Local file fallback** — reads pre-downloaded CSV files from
   ``config["data_dir"]``.

3. **GRDC cross-reference** — four Bolivian stations are available
   from the Global Runoff Data Centre; these can be fetched via
   the GRDC connector if configured.

Endpoints used
--------------
* Catalog listing:
  GET /index.php/catalog/209  — dataset metadata page.

* Data download:
  GET /index.php/catalog/209/download/{resource_id}

The connector is built defensively — the NADA catalog may require
authentication or serve HTML instead of data files.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from pathlib import Path

import httpx
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
# NADA catalog constants
# ---------------------------------------------------------------------------
CATALOG_ID = 209
CATALOG_PATH = f"/index.php/catalog/{CATALOG_ID}"

# ---------------------------------------------------------------------------
# Curated seed stations — 19 stations on 15 Bolivian rivers.
# Source: INE "Estadísticas de Caudales y Niveles de Ríos"
# Format: (native_id, name, lat, lon, river, area_km2 | None)
# ---------------------------------------------------------------------------
_SEED_STATIONS: list[tuple[str, str, float, float, str, float | None]] = [
    ("BO-001", "Desaguadero - Puente Internacional",
     -16.56, -69.04, "Desaguadero", 29500.0),
    ("BO-002", "Calacoto", -17.28, -68.64, "Desaguadero", None),
    ("BO-003", "Rurrenabaque", -14.44, -67.53, "Beni", 69966.0),
    ("BO-004", "Angosto del Bala", -14.53, -67.56, "Beni", None),
    ("BO-005", "Puerto Varador", -16.83, -64.80, "Mamoré", None),
    ("BO-006", "Puerto Villarroel",
     -16.87, -64.78, "Ichilo", 7230.0),
    ("BO-007", "Puerto Grether", -16.48, -65.37, "Chapare", None),
    ("BO-008", "Villamontes", -21.26, -63.47, "Pilcomayo", 96570.0),
    ("BO-009", "Puente Arce", -19.56, -65.28, "Pilcomayo", None),
    ("BO-010", "Abapo", -18.91, -63.39, "Grande", 59843.0),
    ("BO-011", "Puente Arce (Grande)",
     -18.45, -64.62, "Grande", None),
    ("BO-012", "Miguelito", -17.97, -63.32, "Yapacaní", None),
    ("BO-013", "Ivirgarzama", -17.00, -64.97, "Ivirgarzama", None),
    ("BO-014", "Puerto Siles", -13.33, -63.80, "Mamoré", None),
    ("BO-015", "Guayaramerín", -10.83, -65.35, "Mamoré", 599847.0),
    ("BO-016", "Riberalta", -10.99, -66.10, "Beni", 283000.0),
    ("BO-017", "Cachuela Esperanza",
     -10.54, -65.60, "Beni", None),
    ("BO-018", "Santa Rosa de Yacuma",
     -14.99, -66.80, "Yacuma", None),
    ("BO-019", "Miguillas", -16.67, -67.44, "La Paz", None),
]

# GRDC station numbers for Bolivia (for cross-reference)
_GRDC_BOLIVIAN_STATIONS: list[tuple[str, str]] = [
    ("3629400", "Rurrenabaque"),
    ("3629150", "Guayaramerín"),
    ("3629500", "Angosto del Bala"),
    ("3629300", "Cachuela Esperanza"),
]


@register("bolivia_ine")
class BoliviaIneConnector(BaseConnector):
    """Connector for Bolivia's INE hydrological data.

    Configuration options (via ``config`` dict):
        data_dir : str | Path
            Local directory with pre-downloaded CSV files.
        catalog_id : int
            NADA catalog ID (default 209).
    """

    slug = "bolivia_ine"
    display_name = "INE Bolivia (Caudales y Niveles)"
    base_url = "https://anda.ine.gob.bo"
    country_codes: list[str] = ["BO"]

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return station catalogue from INE or seed list fallback."""
        # Try NADA catalog API
        stations = await self._try_fetch_catalog_stations()
        if stations:
            logger.info(
                "stations_fetched",
                provider=self.slug,
                count=len(stations),
                source="catalog_api",
            )
            return stations

        # Fall back to seed list
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
        """Fetch discharge observations for a station.

        Tries NADA catalog download, then local CSV files.
        Returns an empty chunk if neither source is available.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        # Try NADA catalog download
        chunk = await self._try_fetch_catalog_data(
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
            "bolivia_ine_no_data",
            station=native_id,
            hint=(
                "Set config['data_dir'] to a directory containing "
                "INE CSV files, or check "
                "https://anda.ine.gob.bo/index.php/catalog/209"
            ),
        )
        return self._empty_chunk(station_id)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent observations (last 30 days).

        INE data is typically historical; a wider window is used.
        """
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(days=30),
            end=now,
        )

    # -----------------------------------------------------------------
    # NADA catalog discovery
    # -----------------------------------------------------------------

    async def _try_fetch_catalog_stations(
        self,
    ) -> list[Station] | None:
        """Try fetching station info from the NADA catalog API."""
        cat_id = self.config.get("catalog_id", CATALOG_ID)
        try:
            resp = await self._get(
                f"/index.php/catalog/{cat_id}",
                params={"format": "json"},
            )
            data = resp.json()
            if isinstance(data, dict):
                variables = data.get(
                    "variables", data.get("resources", []),
                )
                if isinstance(variables, list):
                    return self._parse_catalog_stations(variables)
            return None
        except (
            ConnectorError,
            httpx.HTTPStatusError,
            ValueError,
        ):
            return None

    def _parse_catalog_stations(
        self, data: list[dict],
    ) -> list[Station] | None:
        """Parse station information from NADA catalog metadata."""
        stations: list[Station] = []
        for entry in data:
            native_id = str(entry.get("id", entry.get("name", "")))
            if not native_id:
                continue
            name = entry.get("label", entry.get("name", native_id))
            lat = entry.get("latitude", entry.get("lat"))
            lon = entry.get("longitude", entry.get("lon"))
            if lat is None or lon is None:
                continue
            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=str(name),
                    latitude=float(str(lat)),
                    longitude=float(str(lon)),
                    country_code="BO",
                    river=entry.get("river"),
                ))
            except (ValueError, KeyError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    station=native_id,
                    error=str(exc),
                )
                continue
        return stations if stations else None

    def _build_seed_stations(self) -> list[Station]:
        """Build Station objects from the curated seed list."""
        stations: list[Station] = []
        for row in _SEED_STATIONS:
            native_id, name, lat, lon, river, area = row
            stations.append(Station(
                id=self._station_id(native_id),
                provider=self.slug,
                native_id=native_id,
                name=name,
                latitude=float(str(lat)),
                longitude=float(str(lon)),
                country_code="BO",
                river=river,
                catchment_area_km2=(
                    float(str(area)) if area is not None else None
                ),
            ))
        return stations

    # -----------------------------------------------------------------
    # NADA catalog data download
    # -----------------------------------------------------------------

    async def _try_fetch_catalog_data(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk | None:
        """Try downloading observation data from NADA catalog."""
        cat_id = self.config.get("catalog_id", CATALOG_ID)

        # Try several resource ID patterns
        for resource_id in (native_id, "data", "caudales"):
            path = (
                f"/index.php/catalog/{cat_id}"
                f"/download/{resource_id}"
            )
            try:
                resp = await self._get(path)
                content_type = resp.headers.get(
                    "content-type", "",
                )

                # If we got HTML, the catalog may need auth
                if "text/html" in content_type:
                    continue

                return self._parse_csv_text(
                    resp.text, station_id, native_id, start, end,
                )
            except (ConnectorError, httpx.HTTPStatusError):
                continue
            except Exception as exc:
                logger.warning(
                    "ine_catalog_parse_error",
                    provider=self.slug,
                    station=native_id,
                    resource=resource_id,
                    error=str(exc),
                )
                continue
        return None

    # -----------------------------------------------------------------
    # Local CSV fallback
    # -----------------------------------------------------------------

    def _try_fetch_local_csv(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk | None:
        """Try reading observations from local CSV files.

        Searches ``config["data_dir"]`` for files matching the
        station's native ID or name.
        """
        data_dir = self.config.get("data_dir")
        if not data_dir:
            return None

        data_path = Path(data_dir)
        if not data_path.is_dir():
            return None

        # Look for files matching this station
        csv_files = list(data_path.glob("*.csv"))
        if not csv_files:
            return None

        all_obs: list[Observation] = []
        for csv_file in csv_files:
            obs = self._parse_local_csv_file(
                csv_file, station_id, native_id, start, end,
            )
            all_obs.extend(obs)

        if not all_obs:
            return None

        all_obs.sort(key=lambda o: o.timestamp)
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=all_obs,
            fetched_at=datetime.now(UTC),
        )

    def _parse_local_csv_file(
        self,
        file_path: Path,
        station_id: str,
        native_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse a local CSV file for observations."""
        try:
            text = file_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ConnectorError(
                self.slug,
                f"Cannot read CSV file {file_path}: {exc}",
            ) from exc

        return self._parse_csv_lines(
            text, station_id, native_id, start, end,
        )

    # -----------------------------------------------------------------
    # CSV parsing
    # -----------------------------------------------------------------

    def _parse_csv_text(
        self,
        text: str,
        station_id: str,
        native_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Parse CSV text into observations."""
        observations = self._parse_csv_lines(
            text, station_id, native_id, start, end,
        )
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    def _parse_csv_lines(
        self,
        text: str,
        station_id: str,
        native_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse CSV lines and extract observations for a station.

        Supports two CSV layouts:

        1. Wide format: ``date,station1,station2,...``
        2. Long format: ``station,date,value``
        """
        start_aware = (
            start if start.tzinfo else start.replace(tzinfo=UTC)
        )
        end_aware = (
            end if end.tzinfo else end.replace(tzinfo=UTC)
        )

        lines = text.strip().splitlines()
        if len(lines) < 2:
            return []

        header = lines[0].strip().split(",")
        header_lower = [h.strip().lower() for h in header]

        # Detect format
        if "station" in header_lower or "estacion" in header_lower:
            return self._parse_long_format(
                lines[1:], header_lower, station_id,
                native_id, start_aware, end_aware,
            )

        return self._parse_wide_format(
            lines[1:], header, header_lower, station_id,
            native_id, start_aware, end_aware,
        )

    def _parse_long_format(
        self,
        data_lines: list[str],
        header_lower: list[str],
        station_id: str,
        native_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse long-format CSV (station, date, value per row)."""
        # Find column indices
        station_col = self._find_col(
            header_lower, ("station", "estacion", "codigo"),
        )
        date_col = self._find_col(
            header_lower, ("date", "fecha", "datetime"),
        )
        value_col = self._find_col(
            header_lower, ("discharge", "caudal", "value", "q"),
        )

        if any(c is None for c in (station_col, date_col, value_col)):
            return []

        observations: list[Observation] = []
        for line in data_lines:
            parts = line.strip().split(",")
            min_cols = max(station_col or 0, date_col or 0, value_col or 0) + 1
            if len(parts) < min_cols:
                continue

            row_station = parts[station_col].strip()  # type: ignore[index]
            if row_station != native_id:
                continue

            ts = self._parse_date(parts[date_col].strip())  # type: ignore[index]
            if ts is None:
                continue
            if ts < start or ts > end:
                continue

            val_str = parts[value_col].strip()  # type: ignore[index]
            discharge, quality = self._parse_value(val_str)

            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=discharge,
                quality=quality,
            ))

        return observations

    def _parse_wide_format(
        self,
        data_lines: list[str],
        header: list[str],
        header_lower: list[str],
        station_id: str,
        native_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse wide-format CSV (date, station1, station2, ...)."""
        date_col = self._find_col(
            header_lower, ("date", "fecha", "datetime"),
        )
        if date_col is None:
            return []

        # Find value column for this station
        value_col: int | None = None
        for i, col_name in enumerate(header):
            col_clean = col_name.strip()
            if (
                col_clean == native_id
                or col_clean.lower() == native_id.lower()
            ):
                value_col = i
                break

        if value_col is None:
            return []

        observations: list[Observation] = []
        for line in data_lines:
            parts = line.strip().split(",")
            if len(parts) <= max(date_col, value_col):
                continue

            ts = self._parse_date(parts[date_col].strip())
            if ts is None:
                continue
            if ts < start or ts > end:
                continue

            val_str = parts[value_col].strip()
            discharge, quality = self._parse_value(val_str)

            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=discharge,
                quality=quality,
            ))

        return observations

    # -----------------------------------------------------------------
    # Helpers
    # -----------------------------------------------------------------

    @staticmethod
    def _find_col(
        header: list[str],
        candidates: tuple[str, ...],
    ) -> int | None:
        """Find the first column index matching any candidate name."""
        for i, col in enumerate(header):
            if col in candidates:
                return i
        return None

    @staticmethod
    def _parse_value(
        val_str: str,
    ) -> tuple[float | None, QualityFlag]:
        """Parse a numeric value string into discharge and quality."""
        if not val_str or val_str.lower() in (
            "", "na", "nan", "-", "nd",
        ):
            return None, QualityFlag.MISSING
        try:
            discharge = float(str(val_str))
            return discharge, QualityFlag.RAW
        except ValueError:
            return None, QualityFlag.MISSING

    @staticmethod
    def _parse_date(date_str: str) -> datetime | None:
        """Try multiple date formats common in Bolivian datasets."""
        for fmt in (
            "%Y-%m-%d",
            "%d/%m/%Y",
            "%d-%m-%Y",
            "%Y/%m/%d",
            "%m/%d/%Y",
        ):
            try:
                return datetime.strptime(
                    date_str, fmt,
                ).replace(tzinfo=UTC)
            except ValueError:
                continue
        return None

    def _empty_chunk(self, station_id: str) -> TimeSeriesChunk:
        """Return an empty TimeSeriesChunk for a station."""
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )

    @staticmethod
    def grdc_station_ids() -> list[tuple[str, str]]:
        """Return GRDC station numbers for Bolivian stations.

        Useful for cross-referencing with the GRDC connector.
        Each tuple is ``(grdc_number, station_name)``.
        """
        return list(_GRDC_BOLIVIAN_STATIONS)
