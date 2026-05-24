"""Tests for the Bangladesh FFWC / BWDB connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.bangladesh_ffwc import BangladeshFFWCConnector

BASE = "https://ffwc.bwdb.gov.bd"

MOCK_STATIONS_RESPONSE = [
    {
        "station_id": "SW001",
        "station_name": "Bahadurabad",
        "latitude": 25.18,
        "longitude": 89.67,
        "river_name": "Brahmaputra",
        "division": "Mymensingh",
    },
    {
        "station_id": "SW002",
        "station_name": "Hardinge Bridge",
        "latitude": 24.08,
        "longitude": 88.56,
        "river_name": "Ganges",
        "division": "Rajshahi",
    },
    {
        "station_id": "SW003",
        "station_name": "Chandpur",
        "latitude": 23.22,
        "longitude": 90.65,
        "river_name": "Meghna",
        "division": "Chittagong",
    },
]

MOCK_OBSERVATIONS_RESPONSE = {
    "data": [
        {"datetime": "2024-07-01", "water_level": 12.5, "discharge": 35000.0, "quality": "good"},
        {"datetime": "2024-07-02", "water_level": 13.1, "discharge": 37500.5, "quality": "good"},
        {"datetime": "2024-07-03", "water_level": 14.0, "discharge": None, "quality": "missing"},
    ]
}

MOCK_FALLBACK_STATIONS_RESPONSE = [
    {
        "stationId": "SW010",
        "stationName": "Chilmari",
        "lat": 25.56,
        "lng": 89.68,
        "riverName": "Brahmaputra",
    },
]

MOCK_FALLBACK_OBSERVATIONS_RESPONSE = {
    "data": [
        {"datetime": "01-07-2024", "discharge": 28000.0, "quality": "estimated"},
        {"datetime": "02-07-2024", "water_level": 11.5, "quality": ""},
    ]
}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_primary_endpoint():
    """Stations are fetched and parsed from the primary data_load endpoint."""
    respx.get(f"{BASE}/data_load").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE),
    )

    async with BangladeshFFWCConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 3

    station_a = next(s for s in stations if s.native_id == "SW001")
    assert station_a.id == "bangladesh_ffwc:SW001"
    assert station_a.provider == "bangladesh_ffwc"
    assert station_a.name == "Bahadurabad"
    assert station_a.latitude == 25.18
    assert station_a.longitude == 89.67
    assert station_a.country_code == "BD"
    assert station_a.river == "Brahmaputra"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fallback_endpoint():
    """Falls back to /api/stations when primary data_load endpoint fails."""
    respx.get(f"{BASE}/data_load").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE}/api/stations").mock(
        return_value=httpx.Response(200, json=MOCK_FALLBACK_STATIONS_RESPONSE),
    )

    async with BangladeshFFWCConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "SW010"
    assert stations[0].name == "Chilmari"
    assert stations[0].river == "Brahmaputra"
    assert stations[0].latitude == 25.56
    assert stations[0].longitude == 89.68


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_all_endpoints_fail():
    """Returns empty list when all station endpoints fail."""
    respx.get(f"{BASE}/data_load").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE}/api/stations").mock(
        return_value=httpx.Response(503),
    )

    async with BangladeshFFWCConnector() as conn:
        stations = await conn.fetch_stations()

    assert stations == []


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_primary_endpoint():
    """Observations are parsed from the primary data_load endpoint."""
    respx.get(f"{BASE}/data_load").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE),
    )

    async with BangladeshFFWCConnector() as conn:
        chunk = await conn.fetch_observations(
            "bangladesh_ffwc:SW001",
            start=datetime(2024, 7, 1, tzinfo=UTC),
            end=datetime(2024, 7, 4, tzinfo=UTC),
        )

    assert chunk.provider == "bangladesh_ffwc"
    assert chunk.station_id == "bangladesh_ffwc:SW001"
    assert len(chunk.observations) == 3

    assert chunk.observations[0].discharge_m3s == pytest.approx(35000.0)
    assert chunk.observations[0].quality.value == "good"

    # Third entry has discharge=None, water_level=14.0 -> falls back to water_level
    assert chunk.observations[2].discharge_m3s == pytest.approx(14.0)
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_fallback_endpoint():
    """Falls back to /api/data when primary data_load endpoint fails."""
    respx.get(f"{BASE}/data_load").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE}/api/data").mock(
        return_value=httpx.Response(200, json=MOCK_FALLBACK_OBSERVATIONS_RESPONSE),
    )

    async with BangladeshFFWCConnector() as conn:
        chunk = await conn.fetch_observations(
            "bangladesh_ffwc:SW001",
            start=datetime(2024, 7, 1, tzinfo=UTC),
            end=datetime(2024, 7, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(28000.0)
    assert chunk.observations[0].quality.value == "estimated"
    # Second entry: no discharge, has water_level -> falls back to water_level
    assert chunk.observations[1].discharge_m3s == pytest.approx(11.5)
    assert chunk.observations[1].quality.value == "raw"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_all_endpoints_fail():
    """Returns empty chunk when all observation endpoints fail."""
    respx.get(f"{BASE}/data_load").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE}/api/data").mock(
        return_value=httpx.Response(500),
    )

    async with BangladeshFFWCConnector() as conn:
        chunk = await conn.fetch_observations(
            "bangladesh_ffwc:SW001",
            start=datetime(2024, 7, 1, tzinfo=UTC),
            end=datetime(2024, 7, 3, tzinfo=UTC),
        )

    assert chunk.observations == []
    assert chunk.station_id == "bangladesh_ffwc:SW001"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_entries_without_id():
    """Entries missing station_id are skipped gracefully."""
    data = [
        {"station_name": "No ID Station", "latitude": 23.0, "longitude": 90.0},
        {
            "station_id": "SW099",
            "station_name": "Valid Station",
            "latitude": 24.0,
            "longitude": 91.0,
        },
    ]
    respx.get(f"{BASE}/data_load").mock(
        return_value=httpx.Response(200, json=data),
    )

    async with BangladeshFFWCConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "SW099"


@pytest.mark.asyncio
@respx.mock
async def test_connector_registration():
    """Connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("bangladesh_ffwc")
    assert cls is BangladeshFFWCConnector
