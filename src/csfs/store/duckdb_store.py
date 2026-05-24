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
        count = 0
        for s in stations:
            self.conn.execute("""
                INSERT OR REPLACE INTO stations
                    (id, provider, native_id, name, latitude, longitude,
                     country_code, river, catchment_area_km2, elevation_m, is_active)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """, [
                s.id, s.provider, s.native_id, s.name, s.latitude, s.longitude,
                s.country_code, s.river, s.catchment_area_km2, s.elevation_m, s.is_active,
            ])
            count += 1
        return count

    async def append_observations(self, chunk: TimeSeriesChunk) -> int:
        if not chunk.observations:
            return 0
        count = 0
        for obs in chunk.observations:
            self.conn.execute("""
                INSERT OR IGNORE INTO observations
                    (station_id, timestamp, discharge_m3s, quality, fetched_at)
                VALUES (?, ?, ?, ?, ?)
            """, [
                obs.station_id, obs.timestamp, obs.discharge_m3s,
                obs.quality.value, chunk.fetched_at,
            ])
            count += 1
        return count

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
