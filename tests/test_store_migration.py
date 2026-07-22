# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Tests for the one-shot migration from the pre-multi-variable schema."""

from datetime import UTC, datetime

import duckdb
import pytest

from csfs.core.models import Observation, TimeSeriesChunk
from csfs.store.duckdb_store import DuckDBStore

# Frozen copy of the observations DDL as it shipped before the multi-variable
# schema (PK (station_id, timestamp), discharge_m3s value column).
_OLD_DDL = """
CREATE TABLE stations (
    id              VARCHAR PRIMARY KEY,
    provider        VARCHAR NOT NULL,
    native_id       VARCHAR NOT NULL,
    name            VARCHAR NOT NULL,
    latitude        DOUBLE NOT NULL,
    longitude       DOUBLE NOT NULL,
    country_code    VARCHAR(2) NOT NULL,
    river           VARCHAR,
    catchment_area_km2 DOUBLE,
    elevation_m     DOUBLE,
    is_active       BOOLEAN DEFAULT TRUE,
    updated_at      TIMESTAMP DEFAULT current_timestamp
);

CREATE TABLE observations (
    station_id      VARCHAR NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    discharge_m3s   DOUBLE,
    quality         VARCHAR NOT NULL DEFAULT 'raw',
    fetched_at      TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (station_id, timestamp)
);

CREATE INDEX idx_stations_provider ON stations (provider);
CREATE INDEX idx_observations_fetched ON observations (fetched_at);

CREATE TABLE acquisition_log (
    provider        VARCHAR NOT NULL,
    started_at      TIMESTAMPTZ NOT NULL,
    duration_s      DOUBLE NOT NULL,
    status          VARCHAR NOT NULL,
    stations        INTEGER NOT NULL DEFAULT 0,
    observations    INTEGER NOT NULL DEFAULT 0,
    fetched         INTEGER NOT NULL DEFAULT 0,
    failed          INTEGER NOT NULL DEFAULT 0,
    retried         INTEGER NOT NULL DEFAULT 0,
    recovered       INTEGER NOT NULL DEFAULT 0,
    error_message   VARCHAR,
    PRIMARY KEY (provider, started_at)
);
"""

_FETCHED_AT = datetime(2024, 6, 3, 8, 0)

_OLD_ROWS = [
    ("usgs:01646500", datetime(2024, 6, 1, tzinfo=UTC), 150.5, "good", _FETCHED_AT),
    ("usgs:01646500", datetime(2024, 6, 2, tzinfo=UTC), None, "missing", _FETCHED_AT),
    ("uk_ea:3400TH", datetime(2024, 6, 1, tzinfo=UTC), 12.25, "raw", _FETCHED_AT),
    # Historic lithuania rows carried water level in cm, not discharge —
    # under both the current slug and the pre-rename lithuania_meteo slug.
    ("lithuania_lhmt:LT001", datetime(2024, 6, 1, tzinfo=UTC), 231.0, "raw", _FETCHED_AT),
    ("lithuania_meteo:LT002", datetime(2024, 6, 1, tzinfo=UTC), -12.0, "raw", _FETCHED_AT),
]


@pytest.fixture
def old_schema_db(tmp_path):
    """A DuckDB file laid out exactly like a pre-multi-variable store."""
    db_path = tmp_path / "old.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(_OLD_DDL)
    conn.executemany(
        "INSERT INTO observations VALUES (?, ?, ?, ?, ?)", _OLD_ROWS
    )
    conn.close()
    return db_path


@pytest.mark.asyncio
async def test_migration_rebuilds_schema_and_backfills(old_schema_db):
    async with DuckDBStore(old_schema_db) as store:
        cols = {
            row[0]
            for row in store.conn.execute(
                "SELECT column_name FROM information_schema.columns "
                "WHERE table_name = 'observations'"
            ).fetchall()
        }
        assert {"variable", "resolution", "value"} <= cols
        assert "discharge_m3s" not in cols

        count = store.conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
        assert count == len(_OLD_ROWS)

        rows = await store.get_observations("usgs:01646500", variable=None)
        assert [r["value"] for r in rows] == [150.5, None]
        assert {r["variable"] for r in rows} == {"discharge"}
        assert {r["resolution"] for r in rows} == {"unknown"}
        # fetched_at survives the rebuild.
        fetched = store.conn.execute(
            "SELECT DISTINCT fetched_at FROM observations WHERE station_id LIKE 'usgs:%'"
        ).fetchall()
        assert fetched == [(_FETCHED_AT,)]


@pytest.mark.asyncio
async def test_migration_retags_lithuania_levels_as_stage(old_schema_db):
    async with DuckDBStore(old_schema_db) as store:
        rows = await store.get_observations("lithuania_lhmt:LT001", variable=None)
        assert len(rows) == 1
        assert rows[0]["variable"] == "stage"
        assert rows[0]["value"] == pytest.approx(2.31)  # cm -> m
        # And nothing masquerades as lithuania discharge any more.
        assert await store.get_observations("lithuania_lhmt:LT001") == []

        # The pre-rename slug is re-tagged too (negative = below gauge datum).
        old_slug = await store.get_observations("lithuania_meteo:LT002", variable=None)
        assert len(old_slug) == 1
        assert old_slug[0]["variable"] == "stage"
        assert old_slug[0]["value"] == pytest.approx(-0.12)


@pytest.mark.asyncio
async def test_migration_is_idempotent(old_schema_db):
    async with DuckDBStore(old_schema_db):
        pass
    # Second writable open must be a no-op, not a second rebuild/error.
    async with DuckDBStore(old_schema_db) as store:
        count = store.conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0]
        assert count == len(_OLD_ROWS)


@pytest.mark.asyncio
async def test_append_after_migration_dedups_against_migrated_rows(old_schema_db):
    async with DuckDBStore(old_schema_db) as store:
        chunk = TimeSeriesChunk(
            station_id="usgs:01646500", provider="usgs",
            observations=[
                # Same timestamp as a migrated row, but migrated rows carry
                # resolution='unknown' — this daily_mean row is a NEW key.
                Observation(station_id="usgs:01646500",
                            timestamp=datetime(2024, 6, 1, tzinfo=UTC),
                            resolution="daily_mean", value=150.5),
                # Exact key of a migrated row -> deduplicated.
                Observation(station_id="usgs:01646500",
                            timestamp=datetime(2024, 6, 1, tzinfo=UTC),
                            resolution="unknown", value=150.5),
            ],
            fetched_at=datetime(2024, 6, 4, tzinfo=UTC),
        )
        n = await store.append_observations(chunk)
        assert n == 1


@pytest.mark.asyncio
async def test_read_only_open_of_old_schema_fails_fast(old_schema_db):
    with pytest.raises(RuntimeError, match="pre-multi-variable schema"):
        async with DuckDBStore(old_schema_db, read_only=True):
            pass


@pytest.mark.asyncio
async def test_fetched_at_index_recreated(old_schema_db):
    async with DuckDBStore(old_schema_db) as store:
        indexes = {
            row[0]
            for row in store.conn.execute(
                "SELECT index_name FROM duckdb_indexes() WHERE table_name = 'observations'"
            ).fetchall()
        }
        assert "idx_observations_fetched" in indexes


@pytest.mark.asyncio
async def test_current_schema_without_constraints_is_repaired(tmp_path):
    """A materialized snapshot regains the keys required by ON CONFLICT."""
    db_path = tmp_path / "constraintless.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute("""
        CREATE TABLE stations AS SELECT
            'usgs:1'::VARCHAR AS id, 'usgs'::VARCHAR AS provider,
            '1'::VARCHAR AS native_id, 'One'::VARCHAR AS name,
            1.0::DOUBLE AS latitude, 2.0::DOUBLE AS longitude,
            'US'::VARCHAR AS country_code, NULL::VARCHAR AS river,
            NULL::DOUBLE AS catchment_area_km2, NULL::DOUBLE AS elevation_m,
            true::BOOLEAN AS is_active, current_timestamp::TIMESTAMP AS updated_at
    """)
    conn.execute("""
        CREATE TABLE observations AS SELECT
            'usgs:1'::VARCHAR AS station_id, current_timestamp::TIMESTAMPTZ AS timestamp,
            'discharge'::VARCHAR AS variable, 'unknown'::VARCHAR AS resolution,
            1.0::DOUBLE AS value, 'raw'::VARCHAR AS quality,
            current_timestamp::TIMESTAMP AS fetched_at
    """)
    conn.execute("""
        CREATE TABLE acquisition_log AS SELECT
            'usgs'::VARCHAR AS provider, current_timestamp::TIMESTAMPTZ AS started_at,
            1.0::DOUBLE AS duration_s, 'ok'::VARCHAR AS status,
            1::INTEGER AS stations, 1::INTEGER AS observations,
            1::INTEGER AS fetched, 0::INTEGER AS failed, 0::INTEGER AS retried,
            0::INTEGER AS recovered, NULL::VARCHAR AS error_message
    """)
    conn.close()

    async with DuckDBStore(db_path) as store:
        keys = {
            table: store._primary_key_columns(table)
            for table in ("stations", "observations", "acquisition_log")
        }
        assert keys == {
            "stations": ("id",),
            "observations": ("station_id", "variable", "resolution", "timestamp"),
            "acquisition_log": ("provider", "started_at"),
        }
        store.conn.execute("""
            INSERT INTO observations SELECT * FROM observations
            ON CONFLICT DO NOTHING
        """)
        assert store.conn.execute("SELECT COUNT(*) FROM observations").fetchone()[0] == 1

    # Repair is idempotent on subsequent writable opens.
    async with DuckDBStore(db_path):
        pass
