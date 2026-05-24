"""Abstract base class for all data provider connectors."""

from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncIterator
from datetime import datetime

import httpx
import structlog
from tenacity import (
    retry,
    retry_if_exception_type,
    stop_after_attempt,
    wait_exponential,
)

from csfs.core.exceptions import ConnectorError, RateLimitError
from csfs.core.models import Station, TimeSeriesChunk

logger = structlog.get_logger()


class BaseConnector(ABC):
    """Interface that every provider connector must implement.

    Subclasses handle the specifics of one data provider: authentication,
    URL construction, response parsing, and rate-limit handling.
    """

    slug: str  # e.g. "usgs", "uk_ea"
    display_name: str  # e.g. "USGS NWIS"
    base_url: str
    country_codes: list[str]  # ISO 3166-1 alpha-2

    def __init__(self, config: dict | None = None) -> None:
        self.config = config or {}
        self._client: httpx.AsyncClient | None = None

    async def __aenter__(self) -> BaseConnector:
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(60.0, connect=10.0),
            headers={"User-Agent": "CSFS/0.1 (https://github.com/csfs)"},
            follow_redirects=True,
        )
        return self

    async def __aexit__(self, *exc) -> None:
        if self._client:
            await self._client.aclose()
            self._client = None

    @property
    def client(self) -> httpx.AsyncClient:
        if self._client is None:
            raise ConnectorError(self.slug, "Connector used outside async context manager")
        return self._client

    @abstractmethod
    async def fetch_stations(self) -> list[Station]:
        """Return all stations available from this provider."""

    @abstractmethod
    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch observations for a single station over a time range."""

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent observations. Override if provider has a dedicated endpoint."""
        now = datetime.utcnow()
        from datetime import timedelta

        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    async def fetch_bulk(
        self,
        station_ids: list[str],
        start: datetime,
        end: datetime,
    ) -> AsyncIterator[TimeSeriesChunk]:
        """Fetch observations for multiple stations. Override for providers with bulk endpoints."""
        for sid in station_ids:
            try:
                chunk = await self.fetch_observations(sid, start, end)
                yield chunk
            except ConnectorError:
                logger.warning("fetch_failed", provider=self.slug, station=sid)

    @retry(
        retry=retry_if_exception_type((RateLimitError, httpx.RemoteProtocolError, httpx.ConnectError, httpx.ReadTimeout)),
        wait=wait_exponential(multiplier=1, min=2, max=60),
        stop=stop_after_attempt(5),
    )
    async def _get(self, path: str, params: dict | None = None) -> httpx.Response:
        """HTTP GET with automatic retry on rate limits and connection errors."""
        resp = await self.client.get(path, params=params)
        if resp.status_code == 429:
            raise RateLimitError(self.slug, "Rate limited")
        if resp.status_code not in (200, 206):
            resp.raise_for_status()
        return resp

    def _station_id(self, native_id: str) -> str:
        """Build the canonical CSFS station ID."""
        return f"{self.slug}:{native_id}"
