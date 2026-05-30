# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
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
        limit: int | None = None,
        offset: int = 0,
    ) -> list[Station]:
        """Query stations with optional filters and pagination."""

    @abstractmethod
    async def get_observations(
        self,
        station_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int | None = None,
        offset: int = 0,
    ) -> list[dict]:
        """Return observations as list of dicts (for JSON serialization)."""

    @abstractmethod
    async def get_latest_timestamp(self, station_id: str) -> datetime | None:
        """Return the most recent observation timestamp for incremental fetches."""

    @abstractmethod
    async def record_acquisition(
        self,
        provider: str,
        started_at: datetime,
        duration_s: float,
        status: str,
        stations: int = 0,
        observations: int = 0,
        fetched: int = 0,
        failed: int = 0,
        retried: int = 0,
        recovered: int = 0,
        error_message: str | None = None,
    ) -> None:
        """Persist the outcome of one provider acquisition run."""

    @abstractmethod
    async def get_acquisition_history(
        self,
        provider: str | None = None,
        limit: int = 20,
    ) -> list[dict]:
        """Return recent acquisition log entries, newest first."""
