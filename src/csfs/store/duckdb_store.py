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
    def __init__(self, db_path: str | Path = "csfs.duckdb", read_only: bool = False) -> None:
        self._db_path = str(db_path)
        self._read_only = read_only
        self._conn: duckdb.DuckDBPyConnection | None = None

    async def __aenter__(self) -> DuckDBStore:
        self._conn = duckdb.connect(self._db_path, read_only=self._read_only)
        # Schema creation is DDL, which a read-only connection cannot run; a
        # read-only store (e.g. the API) serves an already-initialised database.
        if not self._read_only:
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
        row = result.fetchone()
        return row[0] if row is not None else len(rows)

    async def get_stations(
        self,
        provider: str | None = None,
        country_code: str | None = None,
        bbox: tuple[float, float, float, float] | None = None,
        limit: int | None = None,
        offset: int = 0,
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
        query += " ORDER BY id"
        if limit is not None:
            query += " LIMIT ? OFFSET ?"
            params.extend([limit, offset])

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
        limit: int | None = None,
        offset: int = 0,
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
        if limit is not None:
            query += " LIMIT ? OFFSET ?"
            params.extend([limit, offset])

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

    async def get_connector_health(
        self,
        stale_after_hours: float = 168.0,
    ) -> list[dict]:
        """Per-provider health derived from stored data and the acquisition log.

        Consolidates the data-coverage view (station/observation counts and
        freshness) with the acquisition-log view (last run, status, error,
        success rate) into one row per provider. Providers that appear in
        either the stations table or the acquisition log are included.

        ``data_health`` classifies the *stored data*:
        - ``none``  — no stations on record
        - ``empty`` — stations exist but no observations
        - ``stale`` — newest observation older than ``stale_after_hours``
        - ``ok``    — fresh observations present
        """
        now_row = self.conn.execute("SELECT CURRENT_TIMESTAMP").fetchone()
        now = now_row[0] if now_row else None

        coverage = self.conn.execute("""
            SELECT s.provider,
                   COUNT(DISTINCT s.id)        AS stations,
                   COUNT(o.station_id)         AS observations,
                   MAX(o.timestamp)            AS latest_observation,
                   MAX(o.fetched_at)           AS last_fetch_at
            FROM stations s
            LEFT JOIN observations o ON o.station_id = s.id
            GROUP BY s.provider
        """).fetchall()

        # Latest run per provider, plus all-time run/ok counts and last-ok time.
        acq = self.conn.execute("""
            WITH ranked AS (
                SELECT provider, status, started_at, error_message,
                       ROW_NUMBER() OVER (
                           PARTITION BY provider ORDER BY started_at DESC
                       ) AS rn
                FROM acquisition_log
            ),
            agg AS (
                SELECT provider,
                       COUNT(*)                                  AS total_runs,
                       COUNT(*) FILTER (WHERE status = 'ok')     AS ok_runs,
                       MAX(started_at) FILTER (WHERE status = 'ok') AS last_ok_at
                FROM acquisition_log
                GROUP BY provider
            )
            SELECT r.provider, r.status, r.started_at, r.error_message,
                   a.total_runs, a.ok_runs, a.last_ok_at
            FROM ranked r
            JOIN agg a USING (provider)
            WHERE r.rn = 1
        """).fetchall()

        merged: dict[str, dict] = {}
        for provider, stations, observations, latest_obs, last_fetch in coverage:
            staleness_hours: float | None = None
            if latest_obs is not None and now is not None:
                staleness_hours = (now - latest_obs).total_seconds() / 3600.0

            if stations == 0:
                data_health = "none"
            elif observations == 0:
                data_health = "empty"
            elif staleness_hours is not None and staleness_hours > stale_after_hours:
                data_health = "stale"
            else:
                data_health = "ok"

            merged[provider] = {
                "provider": provider,
                "stations": stations,
                "observations": observations,
                "latest_observation": latest_obs,
                "last_fetch_at": last_fetch,
                "staleness_hours": staleness_hours,
                "data_health": data_health,
                "last_run": None,
                "last_status": None,
                "last_error": None,
                "last_ok_at": None,
                "total_runs": 0,
                "ok_runs": 0,
                "success_rate": None,
            }

        for provider, status, started_at, error, total_runs, ok_runs, last_ok in acq:
            row = merged.setdefault(provider, {
                "provider": provider,
                "stations": 0,
                "observations": 0,
                "latest_observation": None,
                "last_fetch_at": None,
                "staleness_hours": None,
                "data_health": "none",
            })
            row.update({
                "last_run": started_at,
                "last_status": status,
                "last_error": error,
                "last_ok_at": last_ok,
                "total_runs": total_runs,
                "ok_runs": ok_runs,
                "success_rate": (ok_runs / total_runs) if total_runs else None,
            })

        return [merged[p] for p in sorted(merged)]
