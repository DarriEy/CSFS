# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""UK NRFA connector — National River Flow Archive.

The NRFA (operated by UKCEH) provides well-documented JSON APIs for
station metadata and gauged daily flow (GDF) time series at
https://nrfaapps.ceh.ac.uk/nrfa/ws.
"""

from __future__ import annotations

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


@register("uk_nrfa")
class UKNRFAConnector(BaseConnector):
    """Connector for the UK National River Flow Archive."""

    slug = "uk_nrfa"
    display_name = "UK NRFA"
    base_url = "https://nrfaapps.ceh.ac.uk/nrfa/ws"
    country_codes = ["GB"]

    async def fetch_stations(self) -> list[Station]:
        """Return all NRFA gauging stations."""
        try:
            resp = await self._get(
                "/station-info",
                params={
                    "station": "*",
                    "format": "json-object",
                    "fields": "id,name,latitude,longitude,river,catchment-area",
                },
            )
            data = resp.json()
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch station list: {exc}",
            ) from exc

        items = data.get("data", [])
        if not isinstance(items, list):
            raise DataFormatError(
                self.slug,
                "Station list response 'data' field is not a list",
            )

        stations: list[Station] = []
        for entry in items:
            try:
                native_id = str(entry.get("id", ""))
                if not native_id:
                    continue

                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=str(entry.get("name", "Unknown")),
                    latitude=float(entry.get("latitude", 0.0)),
                    longitude=float(entry.get("longitude", 0.0)),
                    country_code="GB",
                    river=entry.get("river"),
                    catchment_area_km2=_safe_float(entry.get("catchment-area")),
                ))
            except (ValueError, TypeError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    entry=str(entry)[:200],
                    error=str(exc),
                )
                continue
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch gauged daily flow for a station over a time range."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        try:
            resp = await self._get(
                "/time-series",
                params={
                    "format": "json-object",
                    "data-type": "gdf",
                    "station": native_id,
                    "start-date": start.strftime("%Y-%m-%d"),
                    "end-date": end.strftime("%Y-%m-%d"),
                },
            )
            data = resp.json()
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch observations for station {native_id}: {exc}",
            ) from exc

        # NRFA data-stream is a flat list: [date, value, date, value, ...]
        stream = data.get("data-stream", [])
        if not isinstance(stream, list):
            return self._empty_chunk(station_id)

        observations: list[Observation] = []
        # Iterate in pairs
        for i in range(0, len(stream), 2):
            try:
                date_str = stream[i]
                value = stream[i+1]
                
                ts = datetime.fromisoformat(str(date_str)).replace(tzinfo=UTC)
                
                discharge = _safe_float(value)
                quality = (
                    QualityFlag.GOOD
                    if discharge is not None
                    else QualityFlag.MISSING
                )

                observations.append(Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=quality,
                ))
            except (IndexError, ValueError, TypeError):
                continue

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent 60 days of gauged daily flow.
        
        Note: NRFA is not a real-time service; 'latest' data may be several
        months or years old depending on publishing lag.
        """
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(days=60),
            end=now,
        )

    def _empty_chunk(self, station_id: str) -> TimeSeriesChunk:
        """Return an empty TimeSeriesChunk for a station."""
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )


def _safe_float(value: object) -> float | None:
    """Safely convert a value to float."""
    if value is None:
        return None
    try:
        return float(str(value))
    except (ValueError, TypeError):
        return None
