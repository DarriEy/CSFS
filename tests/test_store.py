"""Tests for DuckDB store."""

from datetime import UTC, datetime, timedelta

import pytest

from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
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
async def test_connector_health_stale(
    store: DuckDBStore, sample_station: Station, sample_chunk: TimeSeriesChunk,
):
    """A provider whose newest observation is old is flagged 'stale'."""
    await store.upsert_stations([sample_station])
    await store.append_observations(sample_chunk)  # obs from 2024

    health = await store.get_connector_health()
    assert len(health) == 1
    row = health[0]
    assert row["provider"] == "usgs"
    assert row["stations"] == 1
    assert row["observations"] == 2
    assert row["data_health"] == "stale"
    assert row["staleness_hours"] > 168


@pytest.mark.asyncio
async def test_connector_health_empty(store: DuckDBStore, sample_station: Station):
    """A provider with stations but no observations is flagged 'empty'."""
    await store.upsert_stations([sample_station])

    health = await store.get_connector_health()
    assert len(health) == 1
    assert health[0]["data_health"] == "empty"
    assert health[0]["observations"] == 0
    assert health[0]["latest_observation"] is None


@pytest.mark.asyncio
async def test_connector_health_fresh(store: DuckDBStore, sample_station: Station):
    """Recent observations yield 'ok' health."""
    await store.upsert_stations([sample_station])
    recent = datetime.now(UTC) - timedelta(hours=2)
    chunk = TimeSeriesChunk(
        station_id="usgs:01646500", provider="usgs",
        observations=[
            Observation(
                station_id="usgs:01646500", timestamp=recent,
                discharge_m3s=100.0, quality=QualityFlag.GOOD,
            ),
        ],
        fetched_at=recent,
    )
    await store.append_observations(chunk)

    health = await store.get_connector_health()
    assert health[0]["data_health"] == "ok"
    assert health[0]["staleness_hours"] < 168


@pytest.mark.asyncio
async def test_connector_health_merges_acquisition_log(
    store: DuckDBStore, sample_station: Station,
):
    """Acquisition-log outcomes are merged in, and log-only providers appear."""
    await store.upsert_stations([sample_station])
    t1 = datetime(2024, 6, 1, 12, 0, tzinfo=UTC)
    t2 = datetime(2024, 6, 2, 12, 0, tzinfo=UTC)
    await store.record_acquisition("usgs", t1, 45.0, "ok", stations=1, observations=10, fetched=1)
    await store.record_acquisition("usgs", t2, 50.0, "degraded", stations=1, fetched=1, failed=1)
    # A provider that only ever failed station discovery — no stations stored.
    await store.record_acquisition("uk_ea", t2, 5.0, "error", error_message="boom")

    health = {r["provider"]: r for r in await store.get_connector_health()}

    assert health["usgs"]["last_status"] == "degraded"  # most recent run
    assert health["usgs"]["total_runs"] == 2
    assert health["usgs"]["ok_runs"] == 1
    assert health["usgs"]["success_rate"] == 0.5
    assert health["usgs"]["last_ok_at"].astimezone(UTC) == t1

    # Log-only provider with no stored data shows up with 'none' data health.
    assert health["uk_ea"]["data_health"] == "none"
    assert health["uk_ea"]["last_status"] == "error"
    assert health["uk_ea"]["last_error"] == "boom"
    assert health["uk_ea"]["stations"] == 0


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
