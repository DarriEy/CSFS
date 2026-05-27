"""Tests for DuckDB store."""

from datetime import UTC, datetime

import pytest

from csfs.core.models import Station, TimeSeriesChunk
from csfs.store.duckdb_store import DuckDBStore


@pytest.fixture
async def store(tmp_path):
    db = tmp_path / "test.duckdb"
    async with DuckDBStore(db) as s:
        yield s


@pytest.mark.asyncio
async def test_upsert_and_query_stations(store: DuckDBStore, sample_station: Station):
    n = await store.upsert_stations([sample_station])
    assert n == 1

    results = await store.get_stations(provider="usgs")
    assert len(results) == 1
    assert results[0].id == sample_station.id


@pytest.mark.asyncio
async def test_append_and_query_observations(
    store: DuckDBStore, sample_station: Station, sample_chunk: TimeSeriesChunk,
):
    await store.upsert_stations([sample_station])
    n = await store.append_observations(sample_chunk)
    assert n == 2

    obs = await store.get_observations("usgs:01646500")
    assert len(obs) == 2


@pytest.mark.asyncio
async def test_latest_timestamp(store: DuckDBStore, sample_station: Station, sample_chunk: TimeSeriesChunk):
    await store.upsert_stations([sample_station])
    await store.append_observations(sample_chunk)

    latest = await store.get_latest_timestamp("usgs:01646500")
    assert latest.replace(tzinfo=None) == datetime(2024, 6, 2, 0, 0)


@pytest.mark.asyncio
async def test_deduplication(store: DuckDBStore, sample_station: Station, sample_chunk: TimeSeriesChunk):
    await store.upsert_stations([sample_station])
    await store.append_observations(sample_chunk)
    await store.append_observations(sample_chunk)  # same data again

    obs = await store.get_observations("usgs:01646500")
    assert len(obs) == 2  # no duplicates


@pytest.mark.asyncio
async def test_record_and_query_acquisition_log(store: DuckDBStore):
    t1 = datetime(2024, 6, 1, 12, 0, tzinfo=UTC)
    t2 = datetime(2024, 6, 2, 12, 0, tzinfo=UTC)
    t3 = datetime(2024, 6, 3, 12, 0, tzinfo=UTC)

    await store.record_acquisition("usgs", t1, 45.0, "ok", stations=100, observations=5000, fetched=100, failed=0)
    await store.record_acquisition(
        "usgs", t2, 50.0, "degraded", stations=100,
        observations=3000, fetched=100, failed=10, retried=10, recovered=3,
    )
    await store.record_acquisition("uk_ea", t3, 30.0, "error", error_message="Connection refused")

    history = await store.get_acquisition_history()
    assert len(history) == 3
    assert history[0]["provider"] == "uk_ea"
    assert history[0]["status"] == "error"
    assert history[0]["error_message"] == "Connection refused"

    usgs_history = await store.get_acquisition_history(provider="usgs")
    assert len(usgs_history) == 2
    assert usgs_history[0]["status"] == "degraded"
    assert usgs_history[0]["recovered"] == 3
    assert usgs_history[1]["status"] == "ok"


@pytest.mark.asyncio
async def test_acquisition_log_limit(store: DuckDBStore):
    for i in range(10):
        t = datetime(2024, 6, 1, i, 0, tzinfo=UTC)
        await store.record_acquisition("usgs", t, 10.0, "ok", stations=50, observations=1000, fetched=50, failed=0)

    history = await store.get_acquisition_history(provider="usgs", limit=3)
    assert len(history) == 3
    newest = history[0]["started_at"]
    if hasattr(newest, 'astimezone'):
        newest = newest.astimezone(UTC)
    assert newest.hour == 9


@pytest.mark.asyncio
async def test_conn_outside_context_raises():
    store = DuckDBStore("nonexistent.duckdb")
    with pytest.raises(RuntimeError, match="outside async context manager"):
        _ = store.conn


@pytest.mark.asyncio
async def test_upsert_empty_stations(store: DuckDBStore):
    n = await store.upsert_stations([])
    assert n == 0


@pytest.mark.asyncio
async def test_append_empty_observations(store: DuckDBStore):
    chunk = TimeSeriesChunk(
        station_id="x:1", provider="x", observations=[], fetched_at=datetime(2024, 6, 1),
    )
    n = await store.append_observations(chunk)
    assert n == 0


@pytest.mark.asyncio
async def test_get_stations_by_country(store: DuckDBStore, sample_station: Station):
    await store.upsert_stations([sample_station])

    results = await store.get_stations(country_code="US")
    assert len(results) == 1

    results = await store.get_stations(country_code="GB")
    assert len(results) == 0


@pytest.mark.asyncio
async def test_get_stations_by_bbox(store: DuckDBStore, sample_station: Station):
    await store.upsert_stations([sample_station])

    results = await store.get_stations(bbox=(-78.0, 38.0, -76.0, 40.0))
    assert len(results) == 1

    results = await store.get_stations(bbox=(0.0, 0.0, 1.0, 1.0))
    assert len(results) == 0


@pytest.mark.asyncio
async def test_get_observations_with_time_range(
    store: DuckDBStore, sample_station: Station, sample_chunk: TimeSeriesChunk,
):
    await store.upsert_stations([sample_station])
    await store.append_observations(sample_chunk)

    obs = await store.get_observations(
        "usgs:01646500",
        start=datetime(2024, 6, 1, 12, 0),
    )
    assert len(obs) == 1

    obs = await store.get_observations(
        "usgs:01646500",
        end=datetime(2024, 6, 1, 12, 0),
    )
    assert len(obs) == 1

    obs = await store.get_observations(
        "usgs:01646500",
        start=datetime(2024, 6, 1, 0, 0),
        end=datetime(2024, 6, 2, 0, 0),
    )
    assert len(obs) == 2
