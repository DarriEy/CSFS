# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""FastAPI application for serving CSFS data."""

from __future__ import annotations

from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path

from fastapi import FastAPI, Query

from csfs.store.duckdb_store import DuckDBStore

_store: DuckDBStore | None = None


def create_app(db_path: Path | str = "csfs.duckdb") -> FastAPI:
    store = DuckDBStore(db_path)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        global _store
        await store.__aenter__()
        _store = store
        yield
        await store.__aexit__(None, None, None)
        _store = None

    app = FastAPI(
        title="CSFS — Community Streamflow Service",
        version="0.1.0",
        lifespan=lifespan,
    )

    @app.get("/api/v1/stations")
    async def list_stations(
        provider: str | None = None,
        country: str | None = Query(None, min_length=2, max_length=2),
        min_lon: float | None = None,
        min_lat: float | None = None,
        max_lon: float | None = None,
        max_lat: float | None = None,
    ):
        assert _store is not None
        bbox = None
        if min_lon is not None and min_lat is not None and max_lon is not None and max_lat is not None:
            bbox = (min_lon, min_lat, max_lon, max_lat)
        stations = await _store.get_stations(
            provider=provider, country_code=country, bbox=bbox,
        )
        return {"count": len(stations), "stations": [s.model_dump() for s in stations]}

    @app.get("/api/v1/observations/{station_id}")
    async def get_observations(
        station_id: str,
        start: datetime | None = None,
        end: datetime | None = None,
    ):
        assert _store is not None
        obs = await _store.get_observations(station_id, start=start, end=end)
        return {"station_id": station_id, "count": len(obs), "observations": obs}

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
