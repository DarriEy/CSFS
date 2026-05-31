"""Tests for BaseConnector — fetch_bulk, _get, context manager, error handling."""

from __future__ import annotations

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk


class StubConnector(BaseConnector):
    """Minimal concrete connector for testing base class behavior."""

    slug = "stub"
    display_name = "Stub"
    base_url = "https://stub.example.com"
    country_codes = ["XX"]

    def __init__(self, obs_map: dict | None = None, **kwargs):
        super().__init__(**kwargs)
        self._obs_map = obs_map or {}

    async def fetch_stations(self) -> list[Station]:
        return []

    async def fetch_observations(
        self, station_id: str, start: datetime, end: datetime,
    ) -> TimeSeriesChunk:
        if station_id in self._obs_map:
            result = self._obs_map[station_id]
            if isinstance(result, Exception):
                raise result
            return result
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime(2024, 6, 1, 12, 0),
        )


def _chunk(station_id: str, n_obs: int = 1) -> TimeSeriesChunk:
    return TimeSeriesChunk(
        station_id=station_id,
        provider="stub",
        observations=[
            Observation(
                station_id=station_id,
                timestamp=datetime(2024, 6, 1, i, 0),
                discharge_m3s=float(i),
                quality=QualityFlag.RAW,
            )
            for i in range(n_obs)
        ],
        fetched_at=datetime(2024, 6, 1, 12, 0),
    )


@pytest.mark.asyncio
async def test_context_manager_lifecycle():
    conn = StubConnector()
    assert conn._client is None

    async with conn:
        assert conn._client is not None
        assert isinstance(conn.client, httpx.AsyncClient)

    assert conn._client is None


@pytest.mark.asyncio
async def test_client_outside_context_raises():
    conn = StubConnector()
    with pytest.raises(ConnectorError, match="outside async context manager"):
        _ = conn.client


def test_station_id():
    conn = StubConnector()
    assert conn._station_id("12345") == "stub:12345"


@pytest.mark.asyncio
async def test_fetch_bulk_yields_chunks():
    obs_map = {
        "stub:1": _chunk("stub:1", 2),
        "stub:2": _chunk("stub:2", 3),
    }
    async with StubConnector(obs_map=obs_map) as conn:
        chunks = []
        async for chunk in conn.fetch_bulk(
            ["stub:1", "stub:2"],
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        ):
            chunks.append(chunk)

    assert len(chunks) == 2
    assert len(chunks[0].observations) == 2
    assert len(chunks[1].observations) == 3


@pytest.mark.asyncio
async def test_fetch_bulk_catches_connector_error():
    obs_map = {
        "stub:1": ConnectorError("stub", "test failure"),
        "stub:2": _chunk("stub:2"),
    }
    async with StubConnector(obs_map=obs_map) as conn:
        chunks = []
        async for chunk in conn.fetch_bulk(
            ["stub:1", "stub:2"],
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        ):
            chunks.append(chunk)

    assert len(chunks) == 1
    assert chunks[0].station_id == "stub:2"


@pytest.mark.asyncio
async def test_fetch_bulk_catches_unexpected_error():
    obs_map = {
        "stub:1": RuntimeError("something unexpected"),
        "stub:2": _chunk("stub:2"),
    }
    async with StubConnector(obs_map=obs_map) as conn:
        chunks = []
        async for chunk in conn.fetch_bulk(
            ["stub:1", "stub:2"],
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        ):
            chunks.append(chunk)

    assert len(chunks) == 1
    assert chunks[0].station_id == "stub:2"


@pytest.mark.asyncio
async def test_fetch_bulk_catches_httpx_errors():
    """httpx errors from connectors calling client.get() directly are caught."""
    for exc_cls in (httpx.ConnectError, httpx.ReadTimeout, httpx.RemoteProtocolError):
        if exc_cls == httpx.ReadTimeout:
            exc = exc_cls("timeout")
        elif exc_cls == httpx.RemoteProtocolError:
            exc = exc_cls("protocol error")
        else:
            exc = exc_cls("connection failed")
        obs_map = {"stub:1": exc}
        async with StubConnector(obs_map=obs_map) as conn:
            chunks = []
            async for chunk in conn.fetch_bulk(
                ["stub:1"],
                start=datetime(2024, 6, 1),
                end=datetime(2024, 6, 2),
            ):
                chunks.append(chunk)
        assert len(chunks) == 0


@pytest.mark.asyncio
@respx.mock
async def test_get_returns_200():
    respx.get("https://stub.example.com/data").mock(
        return_value=httpx.Response(200, json={"ok": True})
    )

    async with StubConnector() as conn:
        resp = await conn._get("/data")

    assert resp.status_code == 200
    assert resp.json() == {"ok": True}


@pytest.mark.asyncio
@respx.mock
async def test_get_returns_206():
    respx.get("https://stub.example.com/data").mock(
        return_value=httpx.Response(206, content=b"partial")
    )

    async with StubConnector() as conn:
        resp = await conn._get("/data")

    assert resp.status_code == 206


@pytest.mark.asyncio
@respx.mock
async def test_get_raises_rate_limit_on_429():
    """429 triggers retries; after exhaustion tenacity wraps the RateLimitError."""
    from tenacity import RetryError, wait_none

    respx.get("https://stub.example.com/data").mock(
        return_value=httpx.Response(429)
    )

    async with StubConnector() as conn:
        original_wait = conn._get.retry.wait
        conn._get.retry.wait = wait_none()
        try:
            with pytest.raises(RetryError):
                await conn._get("/data")
        finally:
            conn._get.retry.wait = original_wait


@pytest.mark.asyncio
@respx.mock
async def test_get_raises_on_server_error():
    respx.get("https://stub.example.com/data").mock(
        return_value=httpx.Response(500)
    )

    async with StubConnector() as conn:
        with pytest.raises(httpx.HTTPStatusError):
            await conn._get("/data")


@pytest.mark.asyncio
async def test_fetch_latest_delegates_to_fetch_observations():
    chunk = _chunk("stub:1", 3)
    obs_map = {"stub:1": chunk}
    async with StubConnector(obs_map=obs_map) as conn:
        result = await conn.fetch_latest("stub:1")

    assert len(result.observations) == 3


@pytest.mark.asyncio
async def test_config_default_empty():
    conn = StubConnector()
    assert conn.config == {}

    conn2 = StubConnector(config={"key": "val"})
    assert conn2.config == {"key": "val"}


# -- request concurrency cap --------------------------------------------------

class _CappedConnector(StubConnector):
    """A StubConnector that limits in-flight requests."""

    slug = "capped"
    base_url = "https://capped.example.com"
    max_concurrent_requests = 2


async def _peak_concurrency(conn, n=8):
    """Fire n concurrent _get calls; return the peak in-flight count."""
    import asyncio

    state = {"now": 0, "peak": 0}

    async def fake_get(path, **kwargs):
        state["now"] += 1
        state["peak"] = max(state["peak"], state["now"])
        await asyncio.sleep(0.02)
        state["now"] -= 1
        return httpx.Response(200, json={})

    conn._client.get = fake_get  # type: ignore[method-assign]
    await asyncio.gather(*[conn._get("/x") for _ in range(n)])
    return state["peak"]


@pytest.mark.asyncio
async def test_request_semaphore_caps_concurrency():
    async with _CappedConnector() as conn:
        assert conn._request_sem is not None
        peak = await _peak_concurrency(conn)
    assert peak <= 2


@pytest.mark.asyncio
async def test_no_cap_allows_full_concurrency():
    async with StubConnector() as conn:
        assert conn._request_sem is None
        peak = await _peak_concurrency(conn)
    assert peak > 2  # unbounded by the connector


def test_sepa_sets_a_request_cap():
    from csfs.connectors.scotland_sepa import ScotlandSepaConnector

    assert ScotlandSepaConnector.max_concurrent_requests == 2
