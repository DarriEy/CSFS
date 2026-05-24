"""Tests for the PEGELONLINE (Germany) connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.germany_pegelonline import GermanyPegelonlineConnector

MOCK_STATIONS_RESPONSE = [
    {
        "uuid": "aaaa-bbbb-cccc-1111",
        "number": "10010000",
        "shortname": "STATION A",
        "longname": "STATION A LONG",
        "latitude": 52.52,
        "longitude": 13.405,
        "water": {"longname": "RHEIN"},
        "timeseries": [
            {"shortname": "W"},
            {"shortname": "Q"},
        ],
    },
    {
        "uuid": "aaaa-bbbb-cccc-2222",
        "number": "10020000",
        "shortname": "STATION B",
        "longname": "STATION B LONG",
        "latitude": 48.13,
        "longitude": 11.58,
        "water": {"longname": "DONAU"},
        "timeseries": [
            {"shortname": "W"},
        ],
    },
    {
        "uuid": "aaaa-bbbb-cccc-3333",
        "number": "10030000",
        "shortname": "STATION C",
        "longname": "STATION C LONG",
        "latitude": 50.94,
        "longitude": 6.96,
        "water": {"longname": "ELBE"},
        "timeseries": [
            {"shortname": "Q"},
        ],
    },
]

MOCK_MEASUREMENTS_RESPONSE = [
    {
        "timestamp": "2024-06-01T12:00:00+02:00",
        "value": 123.4,
    },
    {
        "timestamp": "2024-06-01T12:15:00+02:00",
        "value": 125.0,
    },
    {
        "timestamp": "2024-06-01T12:30:00+02:00",
        "value": None,
    },
]


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_filters_discharge():
    """Only stations with a Q timeseries are returned."""
    respx.get(
        "https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json"
    ).mock(return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE))

    async with GermanyPegelonlineConnector() as conn:
        stations = await conn.fetch_stations()

    # Station B has only W (water level), so it should be excluded
    assert len(stations) == 2
    native_ids = {s.native_id for s in stations}
    assert native_ids == {"10010000", "10030000"}

    # Check fields on first station
    station_a = next(s for s in stations if s.native_id == "10010000")
    assert station_a.id == "germany_pegelonline:10010000"
    assert station_a.provider == "germany_pegelonline"
    assert station_a.country_code == "DE"
    assert station_a.river == "RHEIN"
    assert station_a.latitude == 52.52
    assert station_a.longitude == 13.405


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_caches_uuid_mapping():
    """fetch_stations populates the number -> uuid cache."""
    respx.get(
        "https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json"
    ).mock(return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE))

    async with GermanyPegelonlineConnector() as conn:
        await conn.fetch_stations()

    assert conn._number_to_uuid["10010000"] == "aaaa-bbbb-cccc-1111"
    assert conn._number_to_uuid["10030000"] == "aaaa-bbbb-cccc-3333"
    # Station B was filtered out but should NOT be cached (no Q timeseries)
    assert "10020000" not in conn._number_to_uuid


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_json():
    """Measurements are correctly parsed into observations."""
    # Pre-populate the uuid cache to avoid a station listing call
    conn = GermanyPegelonlineConnector()
    conn._number_to_uuid["10010000"] = "aaaa-bbbb-cccc-1111"

    respx.get(
        "https://www.pegelonline.wsv.de/webservices/rest-api/v2"
        "/stations/aaaa-bbbb-cccc-1111/Q/measurements.json"
    ).mock(return_value=httpx.Response(200, json=MOCK_MEASUREMENTS_RESPONSE))

    async with conn:
        chunk = await conn.fetch_observations(
            "germany_pegelonline:10010000",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "germany_pegelonline"
    assert chunk.station_id == "germany_pegelonline:10010000"
    assert len(chunk.observations) == 3

    # First observation — value in m3/s (no conversion)
    assert chunk.observations[0].discharge_m3s == pytest.approx(123.4)
    assert chunk.observations[0].quality.value == "raw"

    # Third observation — None value should yield MISSING
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty():
    """An empty measurements array returns zero observations."""
    conn = GermanyPegelonlineConnector()
    conn._number_to_uuid["10010000"] = "aaaa-bbbb-cccc-1111"

    respx.get(
        "https://www.pegelonline.wsv.de/webservices/rest-api/v2"
        "/stations/aaaa-bbbb-cccc-1111/Q/measurements.json"
    ).mock(return_value=httpx.Response(200, json=[]))

    async with conn:
        chunk = await conn.fetch_observations(
            "germany_pegelonline:10010000",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_resolves_uuid_when_not_cached():
    """When uuid is not cached, fetch_observations fetches the station list first."""
    # Mock both endpoints: the station listing and the measurements
    respx.get(
        "https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json"
    ).mock(return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE))

    respx.get(
        "https://www.pegelonline.wsv.de/webservices/rest-api/v2"
        "/stations/aaaa-bbbb-cccc-1111/Q/measurements.json"
    ).mock(return_value=httpx.Response(200, json=MOCK_MEASUREMENTS_RESPONSE))

    async with GermanyPegelonlineConnector() as conn:
        chunk = await conn.fetch_observations(
            "germany_pegelonline:10010000",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 3
    # Confirm the cache was populated via the station listing call
    assert conn._number_to_uuid["10010000"] == "aaaa-bbbb-cccc-1111"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty station list returns no stations."""
    respx.get(
        "https://www.pegelonline.wsv.de/webservices/rest-api/v2/stations.json"
    ).mock(return_value=httpx.Response(200, json=[]))

    async with GermanyPegelonlineConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0
