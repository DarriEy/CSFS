"""Tests for the Iran IWRMC connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.iran_iwrmc import _SEED_STATIONS, IranIWRMCConnector

MOCK_STATIONS_RESPONSE = [
    {
        "station_code": "21-001",
        "station_name": "Ahvaz",
        "latitude": 31.32,
        "longitude": 48.67,
        "river_name": "Karun",
        "basin_name": "Persian Gulf",
    },
    {
        "station_code": "22-001",
        "station_name": "Hamidieh",
        "latitude": 31.48,
        "longitude": 48.43,
        "river_name": "Karkheh",
        "basin_name": "Persian Gulf",
    },
]

MOCK_OBSERVATIONS_RESPONSE = [
    {"date": "2024-06-01", "value": 120.5},
    {"date": "2024-06-02", "value": 115.3},
    {"date": "2024-06-03", "value": None},
]

BASE = "https://stu.wrm.ir"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_primary_endpoint():
    """Stations are fetched and parsed from the primary endpoint."""
    respx.get(f"{BASE}/amar/istgah_list.asp").mock(
        return_value=httpx.Response(
            200, json=MOCK_STATIONS_RESPONSE,
        ),
    )

    async with IranIWRMCConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    ahvaz = next(
        s for s in stations if s.native_id == "21-001"
    )
    assert ahvaz.id == "iran_iwrmc:21-001"
    assert ahvaz.provider == "iran_iwrmc"
    assert ahvaz.name == "Ahvaz"
    assert ahvaz.country_code == "IR"
    assert ahvaz.river == "Karun"
    assert ahvaz.latitude == pytest.approx(31.32)
    assert ahvaz.longitude == pytest.approx(48.67)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fallback_to_seed():
    """Falls back to seed list when all endpoints fail."""
    respx.get(f"{BASE}/amar/istgah_list.asp").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE}/api/stations").mock(
        return_value=httpx.Response(500),
    )

    async with IranIWRMCConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    # Check a known seed station
    karun = next(
        s for s in stations if s.native_id == "21-001"
    )
    assert karun.river == "Karun"
    assert karun.country_code == "IR"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fallback_endpoint():
    """Falls back to second endpoint when primary fails."""
    respx.get(f"{BASE}/amar/istgah_list.asp").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE}/api/stations").mock(
        return_value=httpx.Response(
            200, json=MOCK_STATIONS_RESPONSE,
        ),
    )

    async with IranIWRMCConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_primary_endpoint():
    """Observations are parsed from the primary endpoint."""
    respx.get(f"{BASE}/amar/data.asp").mock(
        return_value=httpx.Response(
            200, json=MOCK_OBSERVATIONS_RESPONSE,
        ),
    )

    async with IranIWRMCConnector() as conn:
        chunk = await conn.fetch_observations(
            "iran_iwrmc:21-001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 4, tzinfo=UTC),
        )

    assert chunk.provider == "iran_iwrmc"
    assert chunk.station_id == "iran_iwrmc:21-001"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(
        120.5,
    )
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_all_fail_returns_empty():
    """Returns empty chunk when all endpoints fail."""
    respx.get(f"{BASE}/amar/data.asp").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE}/api/observations").mock(
        return_value=httpx.Response(500),
    )

    async with IranIWRMCConnector() as conn:
        chunk = await conn.fetch_observations(
            "iran_iwrmc:21-001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 3, tzinfo=UTC),
        )

    assert chunk.observations == []
    assert chunk.station_id == "iran_iwrmc:21-001"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_entries_without_id():
    """Entries missing station_code are skipped."""
    data = [
        {"station_name": "No Code", "latitude": 30.0},
        {"station_code": "", "station_name": "Empty Code"},
        MOCK_STATIONS_RESPONSE[0],
    ]
    respx.get(f"{BASE}/amar/istgah_list.asp").mock(
        return_value=httpx.Response(200, json=data),
    )

    async with IranIWRMCConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "21-001"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_wrapped_in_dict():
    """Observations wrapped in a dict envelope are parsed."""
    wrapped = {"data": MOCK_OBSERVATIONS_RESPONSE}
    respx.get(f"{BASE}/amar/data.asp").mock(
        return_value=httpx.Response(200, json=wrapped),
    )

    async with IranIWRMCConnector() as conn:
        chunk = await conn.fetch_observations(
            "iran_iwrmc:22-001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 4, tzinfo=UTC),
        )

    assert len(chunk.observations) == 3


@pytest.mark.asyncio
@respx.mock
async def test_connector_registration():
    """Connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("iran_iwrmc")
    assert cls is IranIWRMCConnector
