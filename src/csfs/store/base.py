"""Abstract interface for the observation store."""

from __future__ import annotations

from abc import ABC, abstractmethod
from datetime import datetime

from csfs.core.models import Station, TimeSeriesChunk


class BaseStore(ABC):
    """Interface for persisting stations and observations."""

    @abstractmethod
    async def upsert_stations(self, stations: list[Station]) -> int:
        """Insert or update stations. Returns count of new/updated rows."""

    @abstractmethod
    async def append_observations(self, chunk: TimeSeriesChunk) -> int:
        """Append observations, deduplicating by (station_id, timestamp). Returns rows written."""

    @abstractmethod
    async def get_stations(
        self,
        provider: str | None = None,
        country_code: str | None = None,
        bbox: tuple[float, float, float, float] | None = None,
    ) -> list[Station]:
        """Query stations with optional filters."""

    @abstractmethod
    async def get_observations(
        self,
        station_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[dict]:
        """Return observations as list of dicts (for JSON serialization)."""

    @abstractmethod
    async def get_latest_timestamp(self, station_id: str) -> datetime | None:
        """Return the most recent observation timestamp for incremental fetches."""
