"""Spain CEDEX Anuario de Aforos connector — historical discharge archive.

CEDEX (Centro de Estudios y Experimentacion de Obras Publicas) maintains
Spain's national historical discharge archive, the *Anuario de Aforos*.
It covers ~1,440 stations across all 10 demarcaciones (river basin districts)
with records from approximately 1900 to 2022.

This is a **historical download site**, not a real-time API.  The connector
is built defensively: endpoints may change or be unavailable.

Endpoints used
--------------
* Station inventory:
  GET /inventario.asp?format=json
  Returns station metadata including codigo, nombre, latitud, longitud,
  rio, cuenca, and superficie_km2.

* Observations:
  GET /datos.asp?estacion={codigo}&variable=AFLIQ&formato=csv
  Returns CSV with fecha (date) and caudal (discharge m3/s) columns.
  AFLIQ = daily mean discharge.
"""

from __future__ import annotations

import csv
import io
from datetime import UTC, datetime

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


@register("spain_cedex")
class SpainCEDEXConnector(BaseConnector):
    """Connector for Spain's CEDEX Anuario de Aforos historical archive."""

    slug = "spain_cedex"
    display_name = "CEDEX Anuario de Aforos (Spain)"
    base_url = "https://ceh.cedex.es/anuarioaforos"
    country_codes = ["ES"]

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Fetch station inventory from CEDEX.

        Attempts the JSON inventory endpoint.  On failure, returns an
        empty list with a warning — the site is historical and may be
        intermittently unavailable.
        """
        try:
            resp = await self._get(
                "/inventario.asp",
                params={"format": "json"},
            )
        except httpx.HTTPStatusError as exc:
            raise ConnectorError(
                self.slug,
                "Failed to fetch station inventory: "
                f"HTTP {exc.response.status_code}",
            ) from exc

        return self._parse_stations(resp.json())

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch daily discharge observations for *station_id*.

        The CEDEX endpoint returns CSV data for variable AFLIQ
        (daily mean discharge).
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        params: dict[str, str] = {
            "estacion": native_id,
            "variable": "AFLIQ",
            "formato": "csv",
        }

        try:
            resp = await self._get("/datos.asp", params=params)
        except httpx.HTTPStatusError as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch observations for {native_id}: "
                f"HTTP {exc.response.status_code}",
            ) from exc

        return self._parse_observations_csv(
            resp.text, station_id, start, end,
        )

    # -----------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------

    def _parse_stations(self, data: list[dict] | dict) -> list[Station]:
        """Parse the CEDEX station inventory JSON into Station models.

        The API may return a bare list or wrap it under a key.
        Both forms are handled defensively.
        """
        items: list[dict] = (
            data
            if isinstance(data, list)
            else data.get("estaciones", data.get("stations", []))
        )

        stations: list[Station] = []
        for entry in items:
            native_id = str(entry.get("codigo", "")).strip()
            if not native_id:
                continue

            lat = entry.get("latitud")
            lon = entry.get("longitud")
            if lat is None or lon is None:
                logger.warning(
                    "station_missing_coords",
                    provider=self.slug,
                    station=native_id,
                )
                continue

            try:
                area_raw = entry.get("superficie_km2")
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=entry.get("nombre", native_id),
                    latitude=float(str(lat)),
                    longitude=float(str(lon)),
                    country_code="ES",
                    river=entry.get("rio"),
                    catchment_area_km2=(
                        float(str(area_raw))
                        if area_raw is not None
                        else None
                    ),
                ))
            except (ValueError, KeyError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    station=native_id,
                    error=str(exc),
                )
                continue

        return stations

    def _parse_observations_csv(
        self,
        csv_text: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Parse CEDEX CSV response into a TimeSeriesChunk.

        Expected CSV columns: fecha, caudal (and possibly others).
        The CSV may have a header row or use semicolons as delimiters.
        """
        observations: list[Observation] = []

        start_aware = (
            start if start.tzinfo else start.replace(tzinfo=UTC)
        )
        end_aware = (
            end if end.tzinfo else end.replace(tzinfo=UTC)
        )

        lines = csv_text.strip().splitlines()
        if not lines:
            return self._build_chunk(station_id, observations)

        # Detect delimiter: semicolon or comma
        delimiter = ";"
        if ";" not in lines[0] and "," in lines[0]:
            delimiter = ","

        reader = csv.reader(io.StringIO(csv_text), delimiter=delimiter)

        header: list[str] = []
        for row in reader:
            stripped = [c.strip().lower() for c in row]
            # Detect the header row by looking for known column names
            if not header:
                if "fecha" in stripped or "date" in stripped:
                    header = stripped
                    continue
                # If first row looks like a date, there's no header
                if row and self._looks_like_date(row[0].strip()):
                    header = ["fecha", "caudal"]
                    obs = self._parse_csv_row(
                        row, header, station_id,
                        start_aware, end_aware,
                    )
                    if obs is not None:
                        observations.append(obs)
                    continue
                # Skip non-data preamble lines
                continue

            obs = self._parse_csv_row(
                row, header, station_id, start_aware, end_aware,
            )
            if obs is not None:
                observations.append(obs)

        return self._build_chunk(station_id, observations)

    def _parse_csv_row(
        self,
        row: list[str],
        header: list[str],
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> Observation | None:
        """Parse a single CSV row into an Observation."""
        if not row or len(row) < 2:
            return None

        # Map columns by header names
        fecha_idx = self._col_index(header, ("fecha", "date"))
        caudal_idx = self._col_index(header, ("caudal", "discharge", "q"))

        if fecha_idx is None or fecha_idx >= len(row):
            return None
        if caudal_idx is None or caudal_idx >= len(row):
            return None

        date_str = row[fecha_idx].strip()
        value_str = row[caudal_idx].strip()

        try:
            ts = self._parse_date(date_str)
        except ValueError:
            return None

        if ts < start or ts > end:
            return None

        discharge: float | None = None
        quality = QualityFlag.RAW

        if value_str and value_str != "-":
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
    def _col_index(
        header: list[str],
        candidates: tuple[str, ...],
    ) -> int | None:
        """Find the column index for any of the candidate names."""
        for name in candidates:
            if name in header:
                return header.index(name)
        return None

    @staticmethod
    def _looks_like_date(text: str) -> bool:
        """Check if a string looks like a date (YYYY-MM-DD or DD/MM/YYYY)."""
        if not text:
            return False
        return (
            len(text) >= 8
            and (text[4:5] in ("-", "/") or text[2:3] in ("-", "/"))
        )

    @staticmethod
    def _parse_date(date_str: str) -> datetime:
        """Parse a date string in common CEDEX formats.

        Supported formats: YYYY-MM-DD, DD/MM/YYYY, YYYY/MM/DD.
        """
        for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%Y/%m/%d"):
            try:
                return datetime.strptime(
                    date_str, fmt,
                ).replace(tzinfo=UTC)
            except ValueError:
                continue
        raise ValueError(f"Unparseable date: {date_str}")

    def _build_chunk(
        self,
        station_id: str,
        observations: list[Observation],
    ) -> TimeSeriesChunk:
        """Build a TimeSeriesChunk from parsed observations."""
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )
