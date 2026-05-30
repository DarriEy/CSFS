# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""FastAPI application for serving CSFS data."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Query, Request

from csfs.store.duckdb_store import DuckDBStore

DEFAULT_PAGE_SIZE = 1000
MAX_PAGE_SIZE = 10000


def create_app(db_path: Path | str = "csfs.duckdb") -> FastAPI:
    # The API only reads; opening read-only avoids locking out a concurrently
    # running acquisition daemon writing to the same database.
    store = DuckDBStore(db_path, read_only=True)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        await store.__aenter__()
        app.state.store = store
        yield
        await store.__aexit__(None, None, None)
        app.state.store = None

    app = FastAPI(
        title="CSFS — Community Streamflow Service",
        version="0.1.0",
        lifespan=lifespan,
    )

    @app.get("/api/v1/stations")
    async def list_stations(
        request: Request,
        provider: str | None = None,
        country: str | None = Query(None, min_length=2, max_length=2),
        min_lon: float | None = None,
        min_lat: float | None = None,
        max_lon: float | None = None,
        max_lat: float | None = None,
        limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
        offset: int = Query(0, ge=0),
    ):
        store: DuckDBStore = request.app.state.store
        bbox = None
        if min_lon is not None and min_lat is not None and max_lon is not None and max_lat is not None:
            bbox = (min_lon, min_lat, max_lon, max_lat)
        stations = await store.get_stations(
            provider=provider, country_code=country, bbox=bbox,
            limit=limit, offset=offset,
        )
        return {
            "count": len(stations),
            "limit": limit,
            "offset": offset,
            "stations": [s.model_dump() for s in stations],
        }

    @app.get("/api/v1/observations/{station_id}")
    async def get_observations(
        request: Request,
        station_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
        limit: int = Query(DEFAULT_PAGE_SIZE, ge=1, le=MAX_PAGE_SIZE),
        offset: int = Query(0, ge=0),
    ):
        store: DuckDBStore = request.app.state.store
        obs = await store.get_observations(
            station_id, start=start, end=end, limit=limit, offset=offset,
        )
        return {
            "station_id": station_id,
            "count": len(obs),
            "limit": limit,
            "offset": offset,
            "observations": obs,
        }

    @app.get("/api/v1/providers")
    async def list_providers():
        from csfs.core.registry import discover
        from csfs.core.registry import list_providers as _lp
        discover()
        return {"providers": _lp()}

    @app.get("/health")
    async def health():
        return {"status": "ok"}

    return app
