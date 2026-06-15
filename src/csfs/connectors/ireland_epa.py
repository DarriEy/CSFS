# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Ireland EPA HydroNet connector — National Hydrometric Programme.

EPA Ireland provides station metadata and time-series data (ZIP/CSV) through
the HydroNet portal (powered by WISKI). Stations are managed by several
regional inspectorates (offices), and the data download URLs require a
3-letter regional code (e.g., DUB, ATH, COR).

Primary source: https://epawebapp.epa.ie/hydronet/
"""

from __future__ import annotations

import csv
import io
import zipfile
from datetime import UTC, datetime, timedelta

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import (
    Observation,
    QualityFlag,
    Station,
    TimeSeriesChunk,
)
from csfs.core.registry import register

logger = structlog.get_logger()

_BASE_URL = "https://epawebapp.epa.ie/Hydronet"
_STATIONS_JSON = f"{_BASE_URL}/output/internet/layers/20/index.json"
_DOWNLOAD_FMT = "{base}/output/internet/stations/{region}/{no}/{param}/complete_15min.zip"
_DOWNLOAD_DAY_FMT = "{base}/output/internet/stations/{region}/{no}/{param}/complete_daymean.zip"

# Regional inspectorate codes used in URLs
_REGION_CODES = ["DUB", "ATH", "COR", "KIL", "CAS", "MON", "LIM", "GAL", "SLI"]

def _map_quality(raw: str | None) -> QualityFlag:
    """Map EPA quality strings to CSFS quality flags."""
    if not raw:
        return QualityFlag.RAW
    val = raw.strip().lower()
    if val in ("good", "valid", "1"):
        return QualityFlag.GOOD
    if val in ("suspect", "doubtful", "2"):
        return QualityFlag.SUSPECT
    if val in ("estimated", "3"):
        return QualityFlag.ESTIMATED
    if val in ("missing", ""):
        return QualityFlag.MISSING
    return QualityFlag.RAW


@register("ireland_epa")
class IrelandEPAConnector(BaseConnector):
    """Connector for Ireland's EPA HydroNet data."""

    slug = "ireland_epa"
    display_name = "Ireland EPA HydroNet"
    base_url = _BASE_URL
    country_codes = ["IE"]

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        self._region_cache: dict[str, str] = {}

    async def fetch_stations(self) -> list[Station]:
        """Fetch and filter EPA-responsible stations from HydroNet index."""
        try:
            resp = await self._get(_STATIONS_JSON)
            data = resp.json()
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch station index: {exc}",
            ) from exc

        stations: list[Station] = []
        for entry in data:
            # We focus on EPA-responsible stations with flow data
            body = entry.get("L1_RESPONSIBLE_BODY", "")
            if "Environmental Protection Agency" not in body:
                continue
                
            avail = entry.get("L1_DATA_AVAILABLE", "")
            if "Flow" not in avail:
                continue

            native_id = str(entry.get("metadata_station_no", "")).strip()
            if not native_id:
                continue

            stations.append(Station(
                id=self._station_id(native_id),
                provider=self.slug,
                native_id=native_id,
                name=entry.get("metadata_station_name", "Unknown"),
                latitude=float(entry.get("metadata_station_latitude", 0.0)),
                longitude=float(entry.get("metadata_station_longitude", 0.0)),
                country_code="IE",
                river=entry.get("L1_river_name"),
                catchment_area_km2=_parse_area(entry.get("metadata_CATCHMENT_SIZE")),
                is_active=entry.get("L1_station_status") == "Active",
            ))

        logger.info(
            "stations_fetched",
            provider=self.slug,
            count=len(stations),
        )
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations from a ZIP/CSV download."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        
        region = await self._get_region_code(native_id)
        if not region:
            logger.warning(
                "region_code_not_found",
                provider=self.slug,
                station=native_id,
            )
            return self._empty_chunk(station_id)

        # Try 15-minute data first, then daily mean
        url = _DOWNLOAD_FMT.format(
            base=self.base_url,
            region=region,
            no=native_id,
            param="Q",
        )
        
        try:
            resp = await self._get(url)
        except Exception:
            # Fallback to daily mean
            url = _DOWNLOAD_DAY_FMT.format(
                base=self.base_url,
                region=region,
                no=native_id,
                param="Q",
            )
            try:
                resp = await self._get(url)
            except Exception as exc:
                logger.warning(
                    "download_failed",
                    provider=self.slug,
                    station=native_id,
                    error=str(exc),
                )
                return self._empty_chunk(station_id)

        return self._parse_zip_response(resp.content, station_id, start, end)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent observations."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(days=30),
            end=now,
        )

    async def _get_region_code(self, native_id: str) -> str | None:
        """Discover and cache the regional code for a station."""
        if native_id in self._region_cache:
            return self._region_cache[native_id]
            
        for code in _REGION_CODES:
            url = _DOWNLOAD_DAY_FMT.format(
                base=self.base_url,
                region=code,
                no=native_id,
                param="Q",
            )
            try:
                # Use a head request to minimize traffic
                resp = await self.client.head(url, timeout=5)
                if resp.status_code == 200:
                    self._region_cache[native_id] = code
                    return code
            except Exception:
                continue
        return None

    def _parse_zip_response(
        self,
        content: bytes,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Extract CSV from ZIP and parse observations."""
        try:
            with zipfile.ZipFile(io.BytesIO(content)) as zf:
                # Find the first .csv file
                csv_files = [f for f in zf.namelist() if f.lower().endswith(".csv")]
                if not csv_files:
                    return self._empty_chunk(station_id)
                    
                with zf.open(csv_files[0]) as f:
                    csv_text = f.read().decode("utf-8-sig")
        except Exception as exc:
            raise DataFormatError(self.slug, f"Failed to unzip/read CSV: {exc}") from exc

        observations: list[Observation] = []
        # EPA HydroNet CSVs are SEMICOLON-delimited: several `#`-prefixed
        # metadata lines, then a `#`-prefixed column header
        # (`#Timestamp;Value;Quality Code Name`), then the data rows. Locate
        # that header, strip the leading `#`, and parse the rows after it.
        lines = csv_text.splitlines()
        header_idx = next(
            (i for i, ln in enumerate(lines) if ln.lstrip("#").startswith("Timestamp")),
            None,
        )
        if header_idx is None:
            return self._empty_chunk(station_id)
        fieldnames = [c.strip() for c in lines[header_idx].lstrip("#").split(";")]
        data_lines = [
            ln for ln in lines[header_idx + 1:] if ln and not ln.startswith("#")
        ]
        reader = csv.DictReader(data_lines, fieldnames=fieldnames, delimiter=";")

        for row in reader:
            try:
                ts_str = row.get("Timestamp")
                if not ts_str:
                    continue

                ts = datetime.fromisoformat(ts_str.strip())
                if ts.tzinfo is None:
                    ts = ts.replace(tzinfo=UTC)

                if ts < start or ts > end:
                    continue

                val_str = row.get("Value")
                discharge = None if not val_str or val_str.strip() == "" else float(val_str.strip())

                quality_raw = row.get("Quality Code Name")
                quality = (
                    QualityFlag.MISSING
                    if discharge is None
                    else _map_quality(quality_raw)
                )

                observations.append(Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=quality,
                ))
            except (ValueError, TypeError):
                continue
                
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    def _empty_chunk(self, station_id: str) -> TimeSeriesChunk:
        """Return an empty TimeSeriesChunk."""
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )


def _parse_area(raw: str | None) -> float | None:
    """Parse '3.80 km²' strings into float."""
    if not raw:
        return None
    try:
        # Remove 'km²' and spaces
        clean = raw.replace("km²", "").replace("km2", "").strip()
        return float(clean)
    except (ValueError, TypeError):
        return None
