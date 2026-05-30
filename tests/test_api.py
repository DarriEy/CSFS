"""Tests for the CSFS FastAPI application (src/csfs/api/app.py)."""

from __future__ import annotations

from datetime import datetime

import pytest

fastapi = pytest.importorskip("fastapi")

from httpx import ASGITransport, AsyncClient  # noqa: E402

from csfs.api.app import create_app  # noqa: E402
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk  # noqa: E402
from csfs.store.duckdb_store import DuckDBStore  # noqa: E402


@pytest.fixture
async def populated_db(tmp_path):
    """Create a DuckDB with sample stations and observations, return the path."""
    db_path = tmp_path / "api_test.duckdb"
    async with DuckDBStore(db_path) as store:
        # Insert stations
        stations = [
            Station(
                id="usgs:01646500", provider="usgs", native_id="01646500",
                name="Potomac River near DC", latitude=38.95, longitude=-77.13,
                country_code="US", river="Potomac",
            ),
            Station(
                id="uk_ea:TH001", provider="uk_ea", native_id="TH001",
                name="Thames at Kingston", latitude=51.41, longitude=-0.31,
                country_code="GB", river="Thames",
            ),
        ]
        await store.upsert_stations(stations)

        # Insert observations
        chunk = TimeSeriesChunk(
            station_id="usgs:01646500",
            provider="usgs",
            observations=[
                Observation(
                    station_id="usgs:01646500",
                    timestamp=datetime(2024, 6, 1, 0, 0),
                    discharge_m3s=150.5,
                    quality=QualityFlag.GOOD,
                ),
                Observation(
                    station_id="usgs:01646500",
                    timestamp=datetime(2024, 6, 2, 0, 0),
                    discharge_m3s=145.2,
                    quality=QualityFlag.GOOD,
                ),
            ],
            fetched_at=datetime(2024, 6, 2, 12, 0),
        )
        await store.append_observations(chunk)

    return db_path


@pytest.fixture
async def client(populated_db):
    """Create an AsyncClient bound to the FastAPI app with a populated DB."""
    app = create_app(populated_db)

    # ASGITransport doesn't run lifespan events, so bind the store to app.state
    # manually (read-only, matching production).
    store = DuckDBStore(populated_db, read_only=True)
    await store.__aenter__()
    app.state.store = store

    transport = ASGITransport(app=app)
    async with AsyncClient(transport=transport, base_url="http://testserver") as ac:
        yield ac

    await store.__aexit__(None, None, None)
    app.state.store = None


# ---- /health endpoint ----

async def test_health(client):
    resp = await client.get("/health")
    assert resp.status_code == 200
    data = resp.json()
    assert data["status"] == "ok"


# ---- /api/v1/stations endpoint ----

async def test_list_stations(client):
    resp = await client.get("/api/v1/stations")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 2
    assert len(data["stations"]) == 2


async def test_list_stations_filter_provider(client):
    resp = await client.get("/api/v1/stations", params={"provider": "usgs"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 1
    assert data["stations"][0]["provider"] == "usgs"


async def test_list_stations_filter_country(client):
    resp = await client.get("/api/v1/stations", params={"country": "GB"})
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 1
    assert data["stations"][0]["country_code"] == "GB"


async def test_list_stations_filter_bbox(client):
    """Bounding box that includes only the US station."""
    resp = await client.get("/api/v1/stations", params={
        "min_lon": -80.0, "min_lat": 35.0,
        "max_lon": -70.0, "max_lat": 42.0,
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 1
    assert data["stations"][0]["id"] == "usgs:01646500"


async def test_list_stations_filter_bbox_no_match(client):
    """Bounding box that matches no stations."""
    resp = await client.get("/api/v1/stations", params={
        "min_lon": 100.0, "min_lat": 50.0,
        "max_lon": 110.0, "max_lat": 60.0,
    })
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 0


async def test_list_stations_country_validation(client):
    """Country code must be exactly 2 characters."""
    resp = await client.get("/api/v1/stations", params={"country": "USA"})
    assert resp.status_code == 422  # validation error


# ---- /api/v1/observations endpoint ----

async def test_get_observations(client):
    resp = await client.get("/api/v1/observations/usgs:01646500")
    assert resp.status_code == 200
    data = resp.json()
    assert data["station_id"] == "usgs:01646500"
    assert data["count"] == 2
    assert len(data["observations"]) == 2


async def test_get_observations_empty_station(client):
    """A station with no observations should return empty list."""
    resp = await client.get("/api/v1/observations/uk_ea:TH001")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 0


async def test_get_observations_nonexistent_station(client):
    """A non-existent station should return empty (not 404)."""
    resp = await client.get("/api/v1/observations/fake:999")
    assert resp.status_code == 200
    data = resp.json()
    assert data["count"] == 0


async def test_get_observations_time_filter(client):
    """The start/end query params should filter observations."""
    resp = await client.get("/api/v1/observations/usgs:01646500", params={
        "start": "2024-06-01T12:00:00",
        "end": "2024-06-02T12:00:00",
    })
    assert resp.status_code == 200
    data = resp.json()
    # Only the June 2 observation should match (after noon June 1)
    assert data["count"] == 1


# ---- /api/v1/providers endpoint ----

async def test_list_providers(client):
    resp = await client.get("/api/v1/providers")
    assert resp.status_code == 200
    data = resp.json()
    assert "providers" in data
    assert isinstance(data["providers"], list)
    assert len(data["providers"]) > 0
