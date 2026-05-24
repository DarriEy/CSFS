"""Tests for the UK NRFA connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.uk_nrfa import UKNRFAConnector

MOCK_STATIONS_RESPONSE = {
    "data": [
        {
            "id": 39001,
            "name": "Thames at Kingston",
            "lat": 51.4167,
            "lng": -0.3117,
            "river": "Thames",
            "catchment-area": 9948.0,
        },
        {
            "id": 54001,
            "name": "Severn at Bewdley",
            "lat": 52.3767,
            "lng": -2.3250,
            "river": "Severn",
            "catchment-area": 4325.0,
        },
    ]
}

MOCK_TIMESERIES_RESPONSE = {
    "data-stream": [
        {"date": "2024-06-01", "gdf-mean-flow": 65.2},
        {"date": "2024-06-02", "gdf-mean-flow": 70.1},
        {"date": "2024-06-03", "gdf-mean-flow": None},
    ]
}


@respx.mock
async def test_fetch_stations():
    """Stations are fetched and parsed from NRFA JSON."""
    respx.get("https://nrfaapps.ceh.ac.uk/nrfa/ws/station-info").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with UKNRFAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    s = next(s for s in stations if s.native_id == "39001")
    assert s.id == "uk_nrfa:39001"
    assert s.name == "Thames at Kingston"
    assert s.river == "Thames"
    assert s.catchment_area_km2 == pytest.approx(9948.0)
    assert s.country_code == "GB"


@respx.mock
async def test_fetch_stations_failure():
    """Returns empty list on HTTP failure."""
    respx.get("https://nrfaapps.ceh.ac.uk/nrfa/ws/station-info").mock(
        return_value=httpx.Response(500)
    )

    async with UKNRFAConnector() as conn:
        stations = await conn.fetch_stations()

    assert stations == []


@respx.mock
async def test_fetch_observations():
    """GDF time-series are parsed correctly."""
    respx.get("https://nrfaapps.ceh.ac.uk/nrfa/ws/time-series").mock(
        return_value=httpx.Response(200, json=MOCK_TIMESERIES_RESPONSE)
    )

    async with UKNRFAConnector() as conn:
        chunk = await conn.fetch_observations(
            "uk_nrfa:39001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 4, tzinfo=UTC),
        )

    assert chunk.provider == "uk_nrfa"
    assert chunk.station_id == "uk_nrfa:39001"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(65.2)
    assert chunk.observations[0].quality.value == "good"
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@respx.mock
async def test_fetch_observations_failure():
    """Returns empty chunk on failure."""
    respx.get("https://nrfaapps.ceh.ac.uk/nrfa/ws/time-series").mock(
        return_value=httpx.Response(500)
    )

    async with UKNRFAConnector() as conn:
        chunk = await conn.fetch_observations(
            "uk_nrfa:39001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 4, tzinfo=UTC),
        )

    assert chunk.observations == []


@respx.mock
async def test_fetch_observations_empty_stream():
    """Empty data-stream returns zero observations."""
    respx.get("https://nrfaapps.ceh.ac.uk/nrfa/ws/time-series").mock(
        return_value=httpx.Response(200, json={"data-stream": []})
    )

    async with UKNRFAConnector() as conn:
        chunk = await conn.fetch_observations(
            "uk_nrfa:39001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 4, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@respx.mock
async def test_fetch_stations_skips_missing_coords():
    """Stations without lat/lng are skipped."""
    data = {
        "data": [
            {"id": 99999, "name": "No Coords", "river": "Test"},
            {"id": 11111, "name": "Good", "lat": 52.0, "lng": -1.5},
        ]
    }
    respx.get("https://nrfaapps.ceh.ac.uk/nrfa/ws/station-info").mock(
        return_value=httpx.Response(200, json=data)
    )

    async with UKNRFAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "11111"


@respx.mock
async def test_connector_registration():
    """Connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("uk_nrfa")
    assert cls is UKNRFAConnector
