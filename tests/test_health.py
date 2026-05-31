"""Tests for the shared connector-health helpers (csfs.core.health)."""

import pytest

from csfs.core.health import (
    DEGRADED_DATA_HEALTH,
    DEGRADED_RUN_STATUS,
    degraded_connectors,
    gather_connector_health,
    is_degraded,
    summarize_health,
)
from csfs.store.duckdb_store import DuckDBStore


@pytest.fixture
async def store(tmp_path):
    db = tmp_path / "test.duckdb"
    async with DuckDBStore(db) as s:
        yield s


def test_summarize_health_counts_buckets():
    rows = [
        {"data_health": "ok"},
        {"data_health": "ok"},
        {"data_health": "stale"},
        {"data_health": "none"},
    ]
    assert summarize_health(rows) == {"ok": 2, "stale": 1, "none": 1}


def test_is_degraded_on_data_health():
    assert is_degraded({"data_health": "stale", "last_status": "ok"})
    assert is_degraded({"data_health": "empty", "last_status": None})
    assert not is_degraded({"data_health": "ok", "last_status": "ok"})


def test_is_degraded_on_run_status():
    # Fresh data but the last run errored — still degraded.
    assert is_degraded({"data_health": "ok", "last_status": "error"})
    assert is_degraded({"data_health": "ok", "last_status": "degraded"})


def test_is_degraded_respects_custom_buckets():
    row = {"data_health": "stale", "last_status": "ok"}
    # Only flag on 'empty' — a stale connector should now pass.
    assert not is_degraded(row, data_health=("empty",), run_status=())


def test_degraded_connectors_filters():
    rows = [
        {"provider": "a", "data_health": "ok", "last_status": "ok"},
        {"provider": "b", "data_health": "stale", "last_status": "ok"},
        {"provider": "c", "data_health": "ok", "last_status": "error"},
    ]
    flagged = {r["provider"] for r in degraded_connectors(rows)}
    assert flagged == {"b", "c"}


def test_degradation_constants_are_disjoint():
    assert not set(DEGRADED_DATA_HEALTH) & set(DEGRADED_RUN_STATUS)


@pytest.mark.asyncio
async def test_gather_includes_registered_roster(store: DuckDBStore):
    # Empty DB → every registered connector still appears as data_health "none".
    rows = await gather_connector_health(store, include_registered=True)
    assert len(rows) > 0
    assert all(r["data_health"] == "none" for r in rows)
    # Sorted by provider and unique.
    slugs = [r["provider"] for r in rows]
    assert slugs == sorted(slugs)
    assert len(slugs) == len(set(slugs))


@pytest.mark.asyncio
async def test_gather_without_roster_is_empty_on_empty_db(store: DuckDBStore):
    rows = await gather_connector_health(store, include_registered=False)
    assert rows == []
