"""Tests for DuckDB store."""

from datetime import datetime

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
