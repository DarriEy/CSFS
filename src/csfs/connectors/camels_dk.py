"""CAMELS-DK connector -- Danish hydrometeorological dataset (Zenodo).

CAMELS-DK provides streamflow for 3,330 catchments in Denmark (2025 update).

References
----------
- Paper: ... (2025) – CAMELS-DK
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

@register("camels_dk")
class CAMELSDKConnector(BaseConnector):
    """Connector for CAMELS-DK (Denmark)."""

    slug = "camels_dk"
    display_name = "CAMELS-DK (Denmark)"
    base_url = "https://zenodo.org/api"
    country_codes = ["DK"]

    async def fetch_stations(self) -> list[Station]:
        """Fetch stations. Uses a seed list."""
        stations: list[Station] = [
            Station(
                id=self._station_id("DK_21000040"),
                provider=self.slug,
                native_id="DK_21000040",
                name="Denmark sample",
                latitude=56.0,
                longitude=10.0,
                country_code="DK",
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
            raise ConnectorError(self.slug, f"Failed to parse CAMELS-DK file: {exc}") from exc

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
