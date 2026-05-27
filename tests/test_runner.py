"""Tests for the acquisition runner — retry logic and acquisition logging."""

from __future__ import annotations

from datetime import datetime
from unittest.mock import AsyncMock, patch

import pytest

from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.store.duckdb_store import DuckDBStore


def _make_stations(n: int) -> list[Station]:
    return [
        Station(
            id=f"test:{i:04d}",
            provider="test",
            native_id=f"{i:04d}",
            name=f"Station {i}",
            latitude=0.0,
            longitude=0.0,
            country_code="XX",
        )
        for i in range(n)
    ]


def _make_chunk(station_id: str) -> TimeSeriesChunk:
    return TimeSeriesChunk(
        station_id=station_id,
        provider="test",
        observations=[
            Observation(
                station_id=station_id,
                timestamp=datetime(2024, 6, 1, 0, 0),
                discharge_m3s=10.0,
                quality=QualityFlag.RAW,
            ),
        ],
        fetched_at=datetime(2024, 6, 1, 12, 0),
    )


@pytest.fixture
async def store(tmp_path):
    db = tmp_path / "test.duckdb"
    async with DuckDBStore(db) as s:
        yield s


@pytest.mark.asyncio
async def test_retry_recovers_transient_failures(store: DuckDBStore):
    """Stations that fail on first attempt but succeed on retry are counted as recovered."""
    from csfs.scheduler.runner import run_acquisition

    stations = _make_stations(3)
    call_counts: dict[str, int] = {}

    async def mock_fetch_observations(station_id, start, end):
        call_counts.setdefault(station_id, 0)
        call_counts[station_id] += 1
        if station_id == "test:0001" and call_counts[station_id] == 1:
            raise ConnectionError("transient failure")
        return _make_chunk(station_id)

    mock_conn = AsyncMock()
    mock_conn.fetch_stations = AsyncMock(return_value=stations)
    mock_conn.fetch_observations = mock_fetch_observations
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)

    def mock_cls(**kw):
        return mock_conn

    with (
        patch("csfs.scheduler.runner.discover"),
        patch("csfs.scheduler.runner.list_providers", return_value=["test"]),
        patch("csfs.scheduler.runner.get_connector", return_value=mock_cls),
    ):
        results = await run_acquisition(store, providers=["test"], lookback_hours=24)

    r = results["test"]
    assert r["status"] == "ok"
    assert r["retried"] == 1
    assert r["recovered"] == 1
    assert r["failed"] == 0
    assert r["observations"] == 3


@pytest.mark.asyncio
async def test_permanent_failure_stays_failed(store: DuckDBStore):
    """Stations that fail on both attempts remain in the failed count."""
    from csfs.scheduler.runner import run_acquisition

    stations = _make_stations(2)

    async def mock_fetch_observations(station_id, start, end):
        if station_id == "test:0001":
            raise ConnectionError("permanent failure")
        return _make_chunk(station_id)

    mock_conn = AsyncMock()
    mock_conn.fetch_stations = AsyncMock(return_value=stations)
    mock_conn.fetch_observations = mock_fetch_observations
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)

    def mock_cls(**kw):
        return mock_conn

    with (
        patch("csfs.scheduler.runner.discover"),
        patch("csfs.scheduler.runner.list_providers", return_value=["test"]),
        patch("csfs.scheduler.runner.get_connector", return_value=mock_cls),
    ):
        results = await run_acquisition(store, providers=["test"], lookback_hours=24)

    r = results["test"]
    assert r["status"] == "degraded"
    assert r["retried"] == 1
    assert r["recovered"] == 0
    assert r["failed"] == 1


@pytest.mark.asyncio
async def test_acquisition_log_persisted(store: DuckDBStore):
    """Acquisition results are written to acquisition_log table."""
    from csfs.scheduler.runner import run_acquisition

    stations = _make_stations(2)

    async def mock_fetch_observations(station_id, start, end):
        return _make_chunk(station_id)

    mock_conn = AsyncMock()
    mock_conn.fetch_stations = AsyncMock(return_value=stations)
    mock_conn.fetch_observations = mock_fetch_observations
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)

    def mock_cls(**kw):
        return mock_conn

    with (
        patch("csfs.scheduler.runner.discover"),
        patch("csfs.scheduler.runner.list_providers", return_value=["test"]),
        patch("csfs.scheduler.runner.get_connector", return_value=mock_cls),
    ):
        await run_acquisition(store, providers=["test"], lookback_hours=24)

    history = await store.get_acquisition_history(provider="test")
    assert len(history) == 1
    assert history[0]["status"] == "ok"
    assert history[0]["observations"] == 2
    assert history[0]["duration_s"] > 0


@pytest.mark.asyncio
async def test_no_stations_discovered(store: DuckDBStore):
    """Provider returning zero stations logs warning, status ok with zero obs."""
    from csfs.scheduler.runner import run_acquisition

    mock_conn = AsyncMock()
    mock_conn.fetch_stations = AsyncMock(return_value=[])
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)

    with (
        patch("csfs.scheduler.runner.discover"),
        patch("csfs.scheduler.runner.list_providers", return_value=["test"]),
        patch("csfs.scheduler.runner.get_connector", return_value=lambda **kw: mock_conn),
    ):
        results = await run_acquisition(store, providers=["test"], lookback_hours=24)

    assert results["test"]["status"] == "ok"
    assert results["test"]["observations"] == 0


@pytest.mark.asyncio
async def test_all_stations_fail_gives_error_status(store: DuckDBStore):
    """When every station fails, status is 'error'."""
    from csfs.scheduler.runner import run_acquisition

    stations = _make_stations(3)

    async def mock_fetch_observations(station_id, start, end):
        raise ConnectionError("all fail")

    mock_conn = AsyncMock()
    mock_conn.fetch_stations = AsyncMock(return_value=stations)
    mock_conn.fetch_observations = mock_fetch_observations
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)

    with (
        patch("csfs.scheduler.runner.discover"),
        patch("csfs.scheduler.runner.list_providers", return_value=["test"]),
        patch("csfs.scheduler.runner.get_connector", return_value=lambda **kw: mock_conn),
    ):
        results = await run_acquisition(store, providers=["test"], lookback_hours=24)

    assert results["test"]["status"] == "error"
    assert results["test"]["failed"] == 3


@pytest.mark.asyncio
async def test_many_failures_logged_with_summary(store: DuckDBStore):
    """More than 5 failures triggers summary warning; status degraded."""
    from csfs.scheduler.runner import run_acquisition

    stations = _make_stations(10)
    fail_ids = {f"test:{i:04d}" for i in range(7)}

    async def mock_fetch_observations(station_id, start, end):
        if station_id in fail_ids:
            raise ConnectionError("fail")
        return _make_chunk(station_id)

    mock_conn = AsyncMock()
    mock_conn.fetch_stations = AsyncMock(return_value=stations)
    mock_conn.fetch_observations = mock_fetch_observations
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)

    with (
        patch("csfs.scheduler.runner.discover"),
        patch("csfs.scheduler.runner.list_providers", return_value=["test"]),
        patch("csfs.scheduler.runner.get_connector", return_value=lambda **kw: mock_conn),
    ):
        results = await run_acquisition(store, providers=["test"], lookback_hours=24)

    r = results["test"]
    assert r["status"] == "degraded"
    assert r["failed"] == 7


@pytest.mark.asyncio
async def test_zero_observations_with_stations_is_degraded(store: DuckDBStore):
    """Stations found but zero observations yields degraded status."""
    from csfs.scheduler.runner import run_acquisition

    stations = _make_stations(2)

    async def mock_fetch_observations(station_id, start, end):
        return TimeSeriesChunk(
            station_id=station_id, provider="test",
            observations=[], fetched_at=datetime(2024, 6, 1, 12, 0),
        )

    mock_conn = AsyncMock()
    mock_conn.fetch_stations = AsyncMock(return_value=stations)
    mock_conn.fetch_observations = mock_fetch_observations
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)

    with (
        patch("csfs.scheduler.runner.discover"),
        patch("csfs.scheduler.runner.list_providers", return_value=["test"]),
        patch("csfs.scheduler.runner.get_connector", return_value=lambda **kw: mock_conn),
    ):
        results = await run_acquisition(store, providers=["test"], lookback_hours=24)

    assert results["test"]["status"] == "degraded"
    assert results["test"]["observations"] == 0


@pytest.mark.asyncio
async def test_outer_exception_records_error(store: DuckDBStore):
    """Exception in fetch_stations is caught, logged as error, and persisted."""
    from csfs.scheduler.runner import run_acquisition

    mock_conn = AsyncMock()
    mock_conn.fetch_stations = AsyncMock(side_effect=RuntimeError("boom"))
    mock_conn.__aenter__ = AsyncMock(return_value=mock_conn)
    mock_conn.__aexit__ = AsyncMock(return_value=False)

    with (
        patch("csfs.scheduler.runner.discover"),
        patch("csfs.scheduler.runner.list_providers", return_value=["test"]),
        patch("csfs.scheduler.runner.get_connector", return_value=lambda **kw: mock_conn),
    ):
        results = await run_acquisition(store, providers=["test"], lookback_hours=24)

    assert results["test"]["status"] == "error"
    assert "boom" in results["test"]["error"]

    history = await store.get_acquisition_history(provider="test")
    assert len(history) == 1
    assert history[0]["status"] == "error"
    assert "boom" in history[0]["error_message"]
