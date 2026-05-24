"""R-ArcticNET v4.0 connector for Russian Arctic streamflow.

R-ArcticNET provides monthly mean discharge for ~2,804 Russian gauging
stations across five major Arctic drainage basins: Ob, Yenisei, Lena,
Barents, and Pacific/Arctic Coast.

Data is freely downloadable as tab-delimited text files from:
    https://www.r-arcticnet.sr.unh.edu/v4.0

Each region has two files:
  - ``{Region}_Attributes.txt`` -- station metadata (PointID, Code,
    Name, Lat, Long, drainage area, record period, etc.)
  - ``{Region}_Discharge.txt`` -- monthly discharge in m3/s with
    columns: PointID, Code, Year, Jan..Dec, Annual

Missing discharge values may appear as blank fields or -9999.

This connector downloads and parses all five regional file pairs,
building Station and Observation objects.  Individual regional
downloads that fail are logged and skipped so that partial data
can still be returned.

References
----------
- R-ArcticNET v4.0: https://www.r-arcticnet.sr.unh.edu/v4.0
- Lammers et al., University of New Hampshire
"""

from __future__ import annotations

import contextlib
from datetime import UTC, datetime

import httpx
import structlog

from csfs.connectors.base import BaseConnector
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

_BASE_URL = "https://www.r-arcticnet.sr.unh.edu"

_MISSING_VALUE = -9999.0

_MONTH_COLUMNS = [
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec",
]

# Regional file paths relative to v4.0/
_REGIONS: list[dict[str, str]] = [
    {
        "name": "Ob",
        "attributes": "/v4.0/Ob/Ob_Attributes.txt",
        "discharge": "/v4.0/Ob/Ob_Discharge.txt",
    },
    {
        "name": "Yenisei",
        "attributes": "/v4.0/Yenisei/Yenisei_Attributes.txt",
        "discharge": "/v4.0/Yenisei/Yenisei_Discharge.txt",
    },
    {
        "name": "Barents",
        "attributes": "/v4.0/Barents/Barents_Attributes.txt",
        "discharge": "/v4.0/Barents/Barents_Discharge.txt",
    },
    {
        "name": "Lena",
        "attributes": "/v4.0/Lena/Lena_Attributes.txt",
        "discharge": "/v4.0/Lena/Lena_Discharge.txt",
    },
    {
        "name": "PacificArcticCoast",
        "attributes": (
            "/v4.0/PacificArcticCoast"
            "/PacificArcticCoast_Attributes.txt"
        ),
        "discharge": (
            "/v4.0/PacificArcticCoast"
            "/PacificArcticCoast_Discharge.txt"
        ),
    },
]


# ---------------------------------------------------------------------------
# Connector
# ---------------------------------------------------------------------------


@register("russia_arcticnet")
class RussiaArcticNETConnector(BaseConnector):
    """R-ArcticNET v4.0 connector for Russian Arctic discharge.

    Downloads tab-delimited attribute and discharge files for five
    Arctic drainage regions, parsing them into CSFS Station and
    Observation models.

    Configuration options (via ``config`` dict):
        None required -- all data is freely downloadable.
    """

    slug = "russia_arcticnet"
    display_name = "R-ArcticNET v4.0 (Russian Arctic)"
    base_url = _BASE_URL
    country_codes: list[str] = ["RU"]

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        self._stations_cache: list[Station] | None = None

    # ------------------------------------------------------------------
    # Station catalogue
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Download and parse all five regional attribute files.

        Results are cached after the first successful call.  Regions
        that fail to download are logged and skipped.
        """
        if self._stations_cache is not None:
            return self._stations_cache

        all_stations: list[Station] = []

        for region in _REGIONS:
            try:
                text = await self._download_text(
                    region["attributes"],
                )
                stations = self._parse_attributes(
                    text, region["name"],
                )
                all_stations.extend(stations)
                logger.info(
                    "region_stations_loaded",
                    provider=self.slug,
                    region=region["name"],
                    count=len(stations),
                )
            except Exception as exc:
                logger.warning(
                    "region_attributes_failed",
                    provider=self.slug,
                    region=region["name"],
                    error=str(exc),
                )

        self._stations_cache = all_stations
        logger.info(
            "stations_fetched",
            provider=self.slug,
            count=len(all_stations),
        )
        return all_stations

    # ------------------------------------------------------------------
    # Observations
    # ------------------------------------------------------------------

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Download discharge files and extract monthly observations.

        Searches all five regional discharge files for rows matching
        the station's native PointID.  Each row contains one year of
        monthly values which are expanded into individual Observation
        objects (timestamped to the first of each month).
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        start_aware = (
            start if start.tzinfo else start.replace(tzinfo=UTC)
        )
        end_aware = (
            end if end.tzinfo else end.replace(tzinfo=UTC)
        )

        observations: list[Observation] = []

        for region in _REGIONS:
            try:
                text = await self._download_text(
                    region["discharge"],
                )
                obs = self._parse_discharge(
                    text,
                    native_id,
                    station_id,
                    start_aware,
                    end_aware,
                )
                observations.extend(obs)
            except Exception as exc:
                logger.warning(
                    "region_discharge_failed",
                    provider=self.slug,
                    region=region["name"],
                    error=str(exc),
                )

        # Sort chronologically
        observations.sort(key=lambda o: o.timestamp)

        logger.info(
            "observations_fetched",
            provider=self.slug,
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
    # Download helper
    # ------------------------------------------------------------------

    async def _download_text(self, path: str) -> str:
        """Download a text file, handling encoding quirks."""
        try:
            resp = await self._get(path)
        except httpx.HTTPStatusError as exc:
            raise ValueError(
                f"HTTP {exc.response.status_code} for {path}"
            ) from exc
        return resp.text

    # ------------------------------------------------------------------
    # Attribute parsing
    # ------------------------------------------------------------------

    def _parse_attributes(
        self,
        text: str,
        region: str,
    ) -> list[Station]:
        """Parse a tab-delimited attributes file into Station objects.

        Expected columns (tab-separated):
        PointID, Code, Name, Lat, Long, X_Ease, Y_Ease, DArea,
        Hydrozone, Gauge_altitude, MinOfYear, MaxOfYear,
        CountOfYear, PercentOfCoverage
        """
        stations: list[Station] = []
        lines = text.strip().splitlines()

        if not lines:
            return stations

        # First line is the header
        header = lines[0].split("\t")
        col_idx = {
            col.strip(): i for i, col in enumerate(header)
        }

        required = {"PointID", "Lat", "Long"}
        if not required.issubset(col_idx):
            logger.warning(
                "attributes_missing_columns",
                region=region,
                header=header[:5],
            )
            return stations

        for line in lines[1:]:
            if not line.strip():
                continue
            station = self._parse_attribute_row(
                line, col_idx, region,
            )
            if station is not None:
                stations.append(station)

        return stations

    def _parse_attribute_row(
        self,
        line: str,
        col_idx: dict[str, int],
        region: str,
    ) -> Station | None:
        """Parse a single tab-delimited attribute row."""
        fields = line.split("\t")

        def _get(col: str) -> str:
            idx = col_idx.get(col)
            if idx is None or idx >= len(fields):
                return ""
            return fields[idx].strip()

        point_id = _get("PointID")
        if not point_id:
            return None

        try:
            lat = float(str(_get("Lat")))
            lon = float(str(_get("Long")))
        except (ValueError, TypeError):
            return None

        name = _get("Name") or point_id
        native_id = point_id

        area_str = _get("DArea")
        catchment_area: float | None = None
        if area_str:
            with contextlib.suppress(ValueError, TypeError):
                catchment_area = float(str(area_str))

        alt_str = _get("Gauge_altitude")
        elevation: float | None = None
        if alt_str:
            with contextlib.suppress(ValueError, TypeError):
                elevation = float(str(alt_str))

        # Extract river name from station name if possible
        river = self._extract_river(name)

        # Build record period from MinOfYear / MaxOfYear
        record_start: datetime | None = None
        record_end: datetime | None = None
        min_year = _get("MinOfYear")
        max_year = _get("MaxOfYear")
        if min_year:
            with contextlib.suppress(ValueError, TypeError):
                record_start = datetime(
                    int(float(str(min_year))), 1, 1,
                    tzinfo=UTC,
                )
        if max_year:
            with contextlib.suppress(ValueError, TypeError):
                record_end = datetime(
                    int(float(str(max_year))), 12, 31,
                    tzinfo=UTC,
                )

        return Station(
            id=self._station_id(native_id),
            provider=self.slug,
            native_id=native_id,
            name=name,
            latitude=lat,
            longitude=lon,
            country_code="RU",
            river=river,
            catchment_area_km2=catchment_area,
            elevation_m=elevation,
            record_start=record_start,
            record_end=record_end,
        )

    @staticmethod
    def _extract_river(name: str) -> str | None:
        """Try to extract a river name from the station name.

        R-ArcticNET names often follow patterns like
        ``"River Name - Station Location"`` or
        ``"R. Name at Location"``.
        """
        if not name:
            return None
        # Common separators in R-ArcticNET station names
        for sep in (" - ", " at ", " u/", " v/"):
            if sep in name:
                return name.split(sep)[0].strip()
        return None

    # ------------------------------------------------------------------
    # Discharge parsing
    # ------------------------------------------------------------------

    def _parse_discharge(
        self,
        text: str,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse a tab-delimited discharge file for one station.

        Expected columns (tab-separated):
        PointID, Code, Year, Jan, Feb, ..., Dec, Annual

        Returns Observation objects for months within [start, end].
        """
        observations: list[Observation] = []
        lines = text.strip().splitlines()

        if not lines:
            return observations

        header = lines[0].split("\t")
        col_idx = {
            col.strip(): i for i, col in enumerate(header)
        }

        if "PointID" not in col_idx or "Year" not in col_idx:
            return observations

        pid_col = col_idx["PointID"]
        year_col = col_idx["Year"]

        for line in lines[1:]:
            if not line.strip():
                continue
            fields = line.split("\t")
            if len(fields) <= year_col:
                continue

            row_pid = fields[pid_col].strip()
            if row_pid != native_id:
                continue

            try:
                year = int(float(str(fields[year_col].strip())))
            except (ValueError, TypeError):
                continue

            for month_idx, month_name in enumerate(
                _MONTH_COLUMNS, start=1,
            ):
                m_col = col_idx.get(month_name)
                if m_col is None or m_col >= len(fields):
                    continue

                ts = datetime(
                    year, month_idx, 1, tzinfo=UTC,
                )

                if ts < start or ts > end:
                    continue

                raw = fields[m_col].strip()
                obs = self._parse_discharge_value(
                    raw, station_id, ts,
                )
                observations.append(obs)

        return observations

    @staticmethod
    def _parse_discharge_value(
        raw: str,
        station_id: str,
        ts: datetime,
    ) -> Observation:
        """Convert a raw discharge string to an Observation."""
        if not raw or raw == "":
            return Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=None,
                quality=QualityFlag.MISSING,
            )

        try:
            value = float(str(raw))
        except ValueError:
            return Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=None,
                quality=QualityFlag.MISSING,
            )

        if abs(value - _MISSING_VALUE) < 0.01:
            return Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=None,
                quality=QualityFlag.MISSING,
            )

        return Observation(
            station_id=station_id,
            timestamp=ts,
            discharge_m3s=value,
            quality=QualityFlag.RAW,
        )
