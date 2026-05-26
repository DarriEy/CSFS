# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""DuckDB-backed observation store — portable, zero-config, fast for analytics."""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import duckdb

from csfs.core.models import Station, TimeSeriesChunk
from csfs.store.base import BaseStore

_INIT_SQL = """
CREATE TABLE IF NOT EXISTS stations (
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

CREATE TABLE IF NOT EXISTS observations (
    station_id      VARCHAR NOT NULL,
    timestamp       TIMESTAMPTZ NOT NULL,
    discharge_m3s   DOUBLE,
    quality         VARCHAR NOT NULL DEFAULT 'raw',
    fetched_at      TIMESTAMP DEFAULT current_timestamp,
    PRIMARY KEY (station_id, timestamp)
);

CREATE INDEX IF NOT EXISTS idx_stations_provider ON stations (provider);
CREATE INDEX IF NOT EXISTS idx_observations_fetched ON observations (fetched_at);

CREATE TABLE IF NOT EXISTS acquisition_log (
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

CREATE INDEX IF NOT EXISTS idx_acqlog_provider ON acquisition_log (provider, started_at);
"""


class DuckDBStore(BaseStore):
    def __init__(self, db_path: str | Path = "csfs.duckdb") -> None:
        self._db_path = str(db_path)
        self._conn: duckdb.DuckDBPyConnection | None = None

    async def __aenter__(self) -> DuckDBStore:
        self._conn = duckdb.connect(self._db_path)
        self._conn.execute(_INIT_SQL)
        return self

    async def __aexit__(self, *exc) -> None:
        if self._conn:
            self._conn.close()
            self._conn = None

    @property
    def conn(self) -> duckdb.DuckDBPyConnection:
        if self._conn is None:
            raise RuntimeError("Store used outside async context manager")
        return self._conn

    async def upsert_stations(self, stations: list[Station]) -> int:
        if not stations:
            return 0
        self.conn.executemany("""
            INSERT OR REPLACE INTO stations
                (id, provider, native_id, name, latitude, longitude,
                 country_code, river, catchment_area_km2, elevation_m, is_active)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, [
            (s.id, s.provider, s.native_id, s.name, s.latitude, s.longitude,
             s.country_code, s.river, s.catchment_area_km2, s.elevation_m, s.is_active)
            for s in stations
        ])
        return len(stations)

    async def append_observations(self, chunk: TimeSeriesChunk) -> int:
        if not chunk.observations:
            return 0
        rows = [
            (obs.station_id, obs.timestamp, obs.discharge_m3s,
             obs.quality.value, chunk.fetched_at)
            for obs in chunk.observations
        ]
        self.conn.execute(
            "CREATE TEMPORARY TABLE IF NOT EXISTS _obs_staging "
            "(station_id VARCHAR, timestamp TIMESTAMPTZ, "
            "discharge_m3s DOUBLE, quality VARCHAR, fetched_at TIMESTAMP)"
        )
        self.conn.execute("DELETE FROM _obs_staging")
        self.conn.executemany(
            "INSERT INTO _obs_staging VALUES (?, ?, ?, ?, ?)", rows,
        )
        result = self.conn.execute("""
            INSERT INTO observations
                (station_id, timestamp, discharge_m3s, quality, fetched_at)
            SELECT s.station_id, s.timestamp, s.discharge_m3s,
                   s.quality, s.fetched_at
            FROM _obs_staging s
            WHERE NOT EXISTS (
                SELECT 1 FROM observations o
                WHERE o.station_id = s.station_id
                  AND o.timestamp = s.timestamp
            )
        """)
        return result.fetchone()[0] if result.description else len(rows)

    async def get_stations(
        self,
        provider: str | None = None,
        country_code: str | None = None,
        bbox: tuple[float, float, float, float] | None = None,
    ) -> list[Station]:
        query = "SELECT * FROM stations WHERE 1=1"
        params: list = []
        if provider:
            query += " AND provider = ?"
            params.append(provider)
        if country_code:
            query += " AND country_code = ?"
            params.append(country_code)
        if bbox:
            min_lon, min_lat, max_lon, max_lat = bbox
            query += " AND longitude BETWEEN ? AND ? AND latitude BETWEEN ? AND ?"
            params.extend([min_lon, max_lon, min_lat, max_lat])

        rows = self.conn.execute(query, params).fetchall()
        columns = [desc[0] for desc in self.conn.description]
        return [
            Station(**{c: v for c, v in zip(columns, row) if c in Station.model_fields})
            for row in rows
        ]

    async def get_observations(
        self,
        station_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
    ) -> list[dict]:
        query = "SELECT station_id, timestamp, discharge_m3s, quality FROM observations WHERE station_id = ?"
        params: list = [station_id]
        if start:
            query += " AND timestamp >= ?"
            params.append(start)
        if end:
            query += " AND timestamp <= ?"
            params.append(end)
        query += " ORDER BY timestamp"

        rows = self.conn.execute(query, params).fetchall()
        columns = [desc[0] for desc in self.conn.description]
        return [dict(zip(columns, row)) for row in rows]

    async def get_latest_timestamp(self, station_id: str) -> datetime | None:
        result = self.conn.execute(
            "SELECT MAX(timestamp) FROM observations WHERE station_id = ?",
            [station_id],
        ).fetchone()
        return result[0] if result and result[0] else None

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
        self.conn.execute(
            """INSERT INTO acquisition_log
                (provider, started_at, duration_s, status, stations, observations,
                 fetched, failed, retried, recovered, error_message)
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            [provider, started_at, duration_s, status, stations, observations,
             fetched, failed, retried, recovered, error_message],
        )

    async def get_acquisition_history(
        self,
        provider: str | None = None,
        limit: int = 20,
    ) -> list[dict]:
        query = "SELECT * FROM acquisition_log"
        params: list = []
        if provider:
            query += " WHERE provider = ?"
            params.append(provider)
        query += " ORDER BY started_at DESC LIMIT ?"
        params.append(limit)
        rows = self.conn.execute(query, params).fetchall()
        columns = [desc[0] for desc in self.conn.description]
        return [dict(zip(columns, row)) for row in rows]
