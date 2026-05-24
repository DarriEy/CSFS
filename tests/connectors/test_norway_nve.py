"""Tests for the NVE HydAPI (Norway) connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.norway_nve import NorwayNVEConnector

MOCK_STATIONS_RESPONSE = [
    {
        "stationId": "2.32.0",
        "stationName": "Gryta",
        "latitude": 60.12,
        "longitude": 11.45,
        "riverName": "Glomma",
        "drainageBasinArea_km2": 354.2,
    },
    {
        "stationId": "12.209.0",
        "stationName": "Sjodalsvatn",
        "latitude": 61.45,
        "longitude": 9.32,
        "riverName": "Sjoa",
        "drainageBasinArea_km2": 487.0,
    },
]

MOCK_OBSERVATIONS_RESPONSE = {
    "data": [
        {
            "stationId": "2.32.0",
            "parameter": "1001",
            "observations": [
                {
                    "time": "2024-06-01T00:00:00Z",
                    "value": 12.3,
                    "correction": 1,
                },
                {
                    "time": "2024-06-02T00:00:00Z",
                    "value": 14.7,
                    "correction": 0,
                },
                {
                    "time": "2024-06-03T00:00:00Z",
                    "value": 11.0,
                    "correction": 2,
                },
            ],
        }
    ]
}

MOCK_OBSERVATIONS_WITH_NULL = {
    "data": [
        {
            "stationId": "2.32.0",
            "parameter": "1001",
            "observations": [
                {
                    "time": "2024-06-01T00:00:00Z",
                    "value": 12.3,
                    "correction": 1,
                },
                {
                    "time": "2024-06-02T00:00:00Z",
                    "value": None,
                    "correction": 0,
                },
            ],
        }
    ]
}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_correctly():
    """Station list is parsed into Station models."""
    respx.get("https://hydapi.nve.no/api/v1/Stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with NorwayNVEConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    native_ids = {s.native_id for s in stations}
    assert native_ids == {"2.32.0", "12.209.0"}

    gryta = next(s for s in stations if s.native_id == "2.32.0")
    assert gryta.id == "norway_nve:2.32.0"
    assert gryta.provider == "norway_nve"
    assert gryta.name == "Gryta"
    assert gryta.country_code == "NO"
    assert gryta.river == "Glomma"
    assert gryta.catchment_area_km2 == pytest.approx(354.2)
    assert gryta.latitude == pytest.approx(60.12)
    assert gryta.longitude == pytest.approx(11.45)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty station list returns no stations."""
    respx.get("https://hydapi.nve.no/api/v1/Stations").mock(
        return_value=httpx.Response(200, json=[])
    )

    async with NorwayNVEConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_json():
    """Observations are correctly parsed with quality flags."""
    respx.get("https://hydapi.nve.no/api/v1/Observations").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE)
    )

    async with NorwayNVEConnector() as conn:
        chunk = await conn.fetch_observations(
            "norway_nve:2.32.0",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 7),
        )

    assert chunk.provider == "norway_nve"
    assert chunk.station_id == "norway_nve:2.32.0"
    assert len(chunk.observations) == 3

    # correction=1 -> GOOD
    assert chunk.observations[0].discharge_m3s == pytest.approx(12.3)
    assert chunk.observations[0].quality.value == "good"

    # correction=0 -> RAW
    assert chunk.observations[1].discharge_m3s == pytest.approx(14.7)
    assert chunk.observations[1].quality.value == "raw"

    # correction=2 -> ESTIMATED
    assert chunk.observations[2].discharge_m3s == pytest.approx(11.0)
    assert chunk.observations[2].quality.value == "estimated"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_null_value_is_missing():
    """A null value in observations yields MISSING quality."""
    respx.get("https://hydapi.nve.no/api/v1/Observations").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_WITH_NULL)
    )

    async with NorwayNVEConnector() as conn:
        chunk = await conn.fetch_observations(
            "norway_nve:2.32.0",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 3),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[1].discharge_m3s is None
    assert chunk.observations[1].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty_data():
    """Empty data array returns zero observations."""
    respx.get("https://hydapi.nve.no/api/v1/Observations").mock(
        return_value=httpx.Response(200, json={"data": []})
    )

    async with NorwayNVEConnector() as conn:
        chunk = await conn.fetch_observations(
            "norway_nve:2.32.0",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_api_key_header_set_when_configured():
    """When an api_key is provided in config, X-API-Key header is set."""
    respx.get("https://hydapi.nve.no/api/v1/Stations").mock(
        return_value=httpx.Response(200, json=[])
    )

    async with NorwayNVEConnector(config={"api_key": "test-key-123"}) as conn:
        assert conn.client.headers["X-API-Key"] == "test-key-123"
        await conn.fetch_stations()


@pytest.mark.asyncio
@respx.mock
async def test_no_api_key_header_when_not_configured():
    """When no api_key is provided, X-API-Key header is absent."""
    respx.get("https://hydapi.nve.no/api/v1/Stations").mock(
        return_value=httpx.Response(200, json=[])
    )

    async with NorwayNVEConnector() as conn:
        assert "X-API-Key" not in conn.client.headers
        await conn.fetch_stations()


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_reference_time_format():
    """ReferenceTime parameter uses the expected date-range format."""
    route = respx.get("https://hydapi.nve.no/api/v1/Observations").mock(
        return_value=httpx.Response(200, json={"data": []})
    )

    async with NorwayNVEConnector() as conn:
        await conn.fetch_observations(
            "norway_nve:2.32.0",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 7),
        )

    assert route.called
    request = route.calls[0].request
    assert "ReferenceTime=2024-06-01%2F2024-06-07" in str(request.url)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_entries_without_station_id():
    """Station entries missing stationId are silently skipped."""
    data = [
        {
            "stationName": "Ghost Station",
            "latitude": 60.0,
            "longitude": 10.0,
        },
        {
            "stationId": "5.1.0",
            "stationName": "Real Station",
            "latitude": 61.0,
            "longitude": 11.0,
            "riverName": "Namsen",
        },
    ]
    respx.get("https://hydapi.nve.no/api/v1/Stations").mock(
        return_value=httpx.Response(200, json=data)
    )

    async with NorwayNVEConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "5.1.0"
