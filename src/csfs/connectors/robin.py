"""ROBIN connector -- Reference Observatory of Basins (Global).

ROBIN provides daily streamflow for 2,386 near-natural catchments globally,
specifically selected for climate change detection.

References
----------
- DOI (EIDC): 10.5285/3b077711-f183-42f1-bac6-c892922c81f4
- Paper: Turner et al. (2025) – ROBIN
"""

from __future__ import annotations

import csv
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

_EIDC_RECORD_URL = "https://doi.org/10.5285/3b077711-f183-42f1-bac6-c892922c81f4"

@register("robin")
class ROBINConnector(BaseConnector):
    """Connector for ROBIN (Global reference basins)."""

    slug = "robin"
    display_name = "ROBIN (Global Reference Basins)"
    base_url = "https://catalogue.ceh.ac.uk"
    country_codes = ["global"]

    async def fetch_stations(self) -> list[Station]:
        """Fetch stations from metadata. Uses a small seed for now."""
        stations: list[Station] = [
            Station(
                id=self._station_id("UK_01001"),
                provider=self.slug,
                native_id="UK_01001",
                name="Thames sample",
                latitude=51.5,
                longitude=-0.5,
                country_code="GB",
            )
        ]
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = self.config.get("data_dir")

        if not data_dir:
            return self._empty_chunk(station_id)

        file_path = Path(data_dir) / f"{native_id}.csv"
        if not file_path.exists():
            return self._empty_chunk(station_id)

        observations: list[Observation] = []
        try:
            with open(file_path, newline="", encoding="utf-8") as f:
                reader = csv.DictReader(f)
                for row in reader:
                    date_str = row.get("date")
                    val = row.get("discharge") or row.get("q")
                    if not date_str or val is None:
                        continue
                        
                    try:
                        ts = datetime.strptime(date_str, "%Y-%m-%d").replace(tzinfo=UTC)
                    except ValueError:
                        continue

                    if start <= ts <= end:
                        observations.append(
                            Observation(
                                station_id=station_id,
                                timestamp=ts,
                                discharge_m3s=float(val),
                                quality=QualityFlag.RAW,
                            )
                        )
        except Exception as exc:
            raise ConnectorError(self.slug, f"Failed to parse ROBIN file: {exc}") from exc

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    def _empty_chunk(self, station_id: str) -> TimeSeriesChunk:
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )
