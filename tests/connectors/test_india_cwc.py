"""Tests for the India CWC / WRIS connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.india_cwc import IndiaCWCConnector

MOCK_STATIONS_RESPONSE = [
    {
        "station_id": "CWC001",
        "station_name": "Farakka",
        "latitude": 24.81,
        "longitude": 87.92,
        "river_name": "Ganga",
        "basin_name": "Ganga Basin",
        "state": "West Bengal",
    },
    {
        "station_id": "CWC002",
        "station_name": "Varanasi",
        "latitude": 25.32,
        "longitude": 83.01,
        "river_name": "Ganga",
        "basin_name": "Ganga Basin",
        "state": "Uttar Pradesh",
    },
    {
        "station_id": "CWC003",
        "station_name": "Polavaram",
        "latitude": 17.25,
        "longitude": 81.65,
        "river_name": "Godavari",
        "basin_name": "Godavari Basin",
        "state": "Andhra Pradesh",
    },
]

MOCK_OBSERVATIONS_RESPONSE = {
    "observations": [
        {"date": "2024-06-01", "value": 1500.5, "quality": "good"},
        {"date": "2024-06-02", "value": 1620.3, "quality": "good"},
        {"date": "2024-06-03", "value": None, "quality": "missing"},
    ]
}

MOCK_FALLBACK_STATIONS_RESPONSE = [
    {
        "stationId": "CWC010",
        "stationName": "Allahabad",
        "lat": 25.43,
        "lng": 81.85,
        "riverName": "Yamuna",
    },
]

MOCK_FALLBACK_OBSERVATIONS_RESPONSE = {
    "data": [
        {"date": "01-06-2024", "value": 800.0, "quality": "estimated"},
        {"date": "02-06-2024", "value": 850.5, "quality": ""},
    ]
}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_primary_endpoint():
    """Stations are fetched and parsed from the primary endpoint."""
    respx.get("https://indiawris.gov.in/api/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with IndiaCWCConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 3

    station_a = next(s for s in stations if s.native_id == "CWC001")
    assert station_a.id == "india_cwc:CWC001"
    assert station_a.provider == "india_cwc"
    assert station_a.name == "Farakka"
    assert station_a.latitude == 24.81
    assert station_a.longitude == 87.92
    assert station_a.country_code == "IN"
    assert station_a.river == "Ganga"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fallback_endpoint():
    """Falls back to alternative endpoint when primary fails."""
    respx.get("https://indiawris.gov.in/api/stations").mock(
        return_value=httpx.Response(500)
    )
    respx.get("https://indiawris.gov.in/api/SubInfo/getGaugeStation").mock(
        return_value=httpx.Response(200, json=MOCK_FALLBACK_STATIONS_RESPONSE)
    )

    async with IndiaCWCConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "CWC010"
    assert stations[0].name == "Allahabad"
    assert stations[0].river == "Yamuna"
    assert stations[0].latitude == 25.43
    assert stations[0].longitude == 81.85


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_all_endpoints_fail():
    """Returns empty list when all endpoints fail."""
    respx.get("https://indiawris.gov.in/api/stations").mock(
        return_value=httpx.Response(500)
    )
    respx.get("https://indiawris.gov.in/api/SubInfo/getGaugeStation").mock(
        return_value=httpx.Response(503)
    )

    async with IndiaCWCConnector() as conn:
        stations = await conn.fetch_stations()

    assert stations == []


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_primary_endpoint():
    """Observations are parsed from the primary endpoint."""
    respx.get("https://indiawris.gov.in/api/observations").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE)
    )

    async with IndiaCWCConnector() as conn:
        chunk = await conn.fetch_observations(
            "india_cwc:CWC001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 4, tzinfo=UTC),
        )

    assert chunk.provider == "india_cwc"
    assert chunk.station_id == "india_cwc:CWC001"
    assert len(chunk.observations) == 3

    assert chunk.observations[0].discharge_m3s == pytest.approx(1500.5)
    assert chunk.observations[0].quality.value == "good"

    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_fallback_endpoint():
    """Falls back to alternative endpoint when primary fails."""
    respx.get("https://indiawris.gov.in/api/observations").mock(
        return_value=httpx.Response(500)
    )
    respx.get("https://indiawris.gov.in/api/SubInfo/getDischargeData").mock(
        return_value=httpx.Response(200, json=MOCK_FALLBACK_OBSERVATIONS_RESPONSE)
    )

    async with IndiaCWCConnector() as conn:
        chunk = await conn.fetch_observations(
            "india_cwc:CWC001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(800.0)
    assert chunk.observations[0].quality.value == "estimated"
    # Empty quality string with non-None value -> RAW
    assert chunk.observations[1].quality.value == "raw"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_all_endpoints_fail():
    """Returns empty chunk when all endpoints fail."""
    respx.get("https://indiawris.gov.in/api/observations").mock(
        return_value=httpx.Response(500)
    )
    respx.get("https://indiawris.gov.in/api/SubInfo/getDischargeData").mock(
        return_value=httpx.Response(500)
    )

    async with IndiaCWCConnector() as conn:
        chunk = await conn.fetch_observations(
            "india_cwc:CWC001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 3, tzinfo=UTC),
        )

    assert chunk.observations == []
    assert chunk.station_id == "india_cwc:CWC001"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_dd_mm_yyyy_dates():
    """Handles DD-MM-YYYY date format from the fallback endpoint."""
    response_data = {
        "observations": [
            {"date": "15-07-2024", "value": 2200.0, "quality": "good"},
        ]
    }
    respx.get("https://indiawris.gov.in/api/observations").mock(
        return_value=httpx.Response(200, json=response_data)
    )

    async with IndiaCWCConnector() as conn:
        chunk = await conn.fetch_observations(
            "india_cwc:CWC001",
            start=datetime(2024, 7, 1, tzinfo=UTC),
            end=datetime(2024, 7, 31, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].timestamp == datetime(2024, 7, 15)
    assert chunk.observations[0].discharge_m3s == pytest.approx(2200.0)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty_list():
    """An empty station array returns no stations."""
    respx.get("https://indiawris.gov.in/api/stations").mock(
        return_value=httpx.Response(200, json=[])
    )

    async with IndiaCWCConnector() as conn:
        stations = await conn.fetch_stations()

    assert stations == []


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_entries_without_id():
    """Entries missing station_id are skipped gracefully."""
    data = [
        {"station_name": "No ID Station", "latitude": 10.0, "longitude": 75.0},
        {"station_id": "CWC099", "station_name": "Good Station", "latitude": 20.0, "longitude": 80.0},
    ]
    respx.get("https://indiawris.gov.in/api/stations").mock(
        return_value=httpx.Response(200, json=data)
    )

    async with IndiaCWCConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "CWC099"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty_observations_array():
    """Empty observations array returns zero observations."""
    respx.get("https://indiawris.gov.in/api/observations").mock(
        return_value=httpx.Response(200, json={"observations": []})
    )

    async with IndiaCWCConnector() as conn:
        chunk = await conn.fetch_observations(
            "india_cwc:CWC001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_connector_registration():
    """Connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("india_cwc")
    assert cls is IndiaCWCConnector
