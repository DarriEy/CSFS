"""Tests for the DanubeHIS connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.danube_his import (
    _SEED_STATIONS,
    DanubeHisConnector,
)

BASE_URL = "https://www.danubehis.org"

MOCK_API_STATIONS = [
    {
        "id": "AT-001",
        "name": "Wien - Nussdorf",
        "latitude": 48.25,
        "longitude": 16.36,
        "country_code": "AT",
        "river": "Donau",
        "catchment_area": 101700.0,
    },
    {
        "id": "HU-001",
        "name": "Budapest",
        "latitude": 47.50,
        "longitude": 19.04,
        "country_code": "HU",
        "river": "Duna",
    },
    {
        "id": "",
        "name": "Missing ID",
        "latitude": 45.0,
        "longitude": 20.0,
    },
    {
        "id": "XX-999",
        "name": "No Coords",
    },
]

MOCK_CSV_DATA = """date,discharge
2024-06-01,150.3
2024-06-02,148.7
2024-06-03,
2024-06-04,155.0
"""

MOCK_JSON_OBSERVATIONS = [
    {"timestamp": "2024-06-01T00:00:00", "discharge": 150.3},
    {"timestamp": "2024-06-02T00:00:00", "discharge": 148.7},
    {"timestamp": "2024-06-03T00:00:00", "discharge": None},
    {"timestamp": "2024-06-04T00:00:00", "discharge": 155.0},
]


@pytest.mark.asyncio
async def test_fetch_stations_returns_seed_list():
    """Seed stations are returned when API is unreachable."""
    async with DanubeHisConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    countries = {s.country_code for s in stations}
    # Verify multi-country coverage
    assert "DE" in countries
    assert "SK" in countries
    assert "HU" in countries
    assert "RS" in countries
    assert "RO" in countries


@pytest.mark.asyncio
async def test_fetch_stations_seed_fields():
    """Seed stations have correct metadata."""
    async with DanubeHisConnector() as conn:
        stations = await conn.fetch_stations()

    nagymaros = next(s for s in stations if s.native_id == "6442500")
    assert nagymaros.id == "danube_his:6442500"
    assert nagymaros.provider == "danube_his"
    assert nagymaros.name == "Nagymaros"
    assert nagymaros.country_code == "HU"
    assert nagymaros.river == "Danube"
    assert nagymaros.latitude == pytest.approx(47.78)
    assert nagymaros.catchment_area_km2 is None


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_from_api():
    """Station list is parsed from API when available."""
    respx.get(f"{BASE_URL}/api/stations").mock(
        return_value=httpx.Response(200, json=MOCK_API_STATIONS),
    )

    async with DanubeHisConnector() as conn:
        stations = await conn.fetch_stations()

    # Only 2 valid entries (empty ID and no-coords are skipped)
    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"AT-001", "HU-001"}

    wien = next(s for s in stations if s.native_id == "AT-001")
    assert wien.catchment_area_km2 == pytest.approx(101700.0)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_csv():
    """CSV observations are parsed with date filtering."""
    respx.get(f"{BASE_URL}/api/data/HU-001").mock(
        return_value=httpx.Response(
            200,
            text=MOCK_CSV_DATA,
            headers={"content-type": "text/csv"},
        ),
    )

    async with DanubeHisConnector() as conn:
        chunk = await conn.fetch_observations(
            "danube_his:HU-001",
            start=datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 2, 23, 59, 59, tzinfo=UTC),
        )

    assert chunk.provider == "danube_his"
    assert chunk.station_id == "danube_his:HU-001"
    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(150.3)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_json():
    """JSON observations are parsed correctly."""
    respx.get(f"{BASE_URL}/api/data/AT-001").mock(
        return_value=httpx.Response(
            200,
            json=MOCK_JSON_OBSERVATIONS,
            headers={"content-type": "application/json"},
        ),
    )

    async with DanubeHisConnector() as conn:
        chunk = await conn.fetch_observations(
            "danube_his:AT-001",
            start=datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 4, 23, 59, 59, tzinfo=UTC),
        )

    assert len(chunk.observations) == 4
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"
    assert chunk.observations[3].discharge_m3s == pytest.approx(155.0)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_returns_empty_on_failure():
    """Server errors return an empty chunk."""
    respx.get(f"{BASE_URL}/api/data/RS-001").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE_URL}/data/RS-001").mock(
        return_value=httpx.Response(500),
    )

    async with DanubeHisConnector() as conn:
        chunk = await conn.fetch_observations(
            "danube_his:RS-001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0
    assert chunk.provider == "danube_his"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_with_auth_token():
    """Auth token is included in params when configured."""
    respx.get(f"{BASE_URL}/api/data/HU-001").mock(
        return_value=httpx.Response(
            200,
            text=MOCK_CSV_DATA,
            headers={"content-type": "text/csv"},
        ),
    )

    config = {"api_token": "test-token-123"}
    async with DanubeHisConnector(config=config) as conn:
        chunk = await conn.fetch_observations(
            "danube_his:HU-001",
            start=datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 4, 23, 59, 59, tzinfo=UTC),
        )

    assert len(chunk.observations) == 4


def test_connector_is_registered():
    """The connector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("danube_his")
    assert cls is DanubeHisConnector
