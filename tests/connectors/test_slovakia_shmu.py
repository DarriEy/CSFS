"""Tests for the Slovakia SHMU connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.slovakia_shmu import SlovakiaSHMUConnector

BASE_URL = "https://www.shmu.sk"

MOCK_STATIONS = [
    {
        "id": "SK001",
        "nazov": "Bratislava - Devinska Nova Ves",
        "zs": 48.20,
        "zd": 16.98,
        "tok": "Dunaj",
    },
    {
        "id": "SK002",
        "nazov": "Komarno",
        "zs": 47.76,
        "zd": 18.12,
        "tok": "Dunaj",
    },
    {
        "id": "SK003",
        "nazov": "Banska Bystrica",
        "zs": 48.74,
        "zd": 19.15,
        "tok": "Hron",
    },
]

MOCK_OBSERVATIONS = [
    {"datum": "2024-06-01T06:00:00", "hodnota": 1850.0},
    {"datum": "2024-06-01T12:00:00", "hodnota": 1920.3},
    {"datum": "2024-06-01T18:00:00", "hodnota": None},
]


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations():
    """Stations are fetched and parsed correctly."""
    respx.get(f"{BASE_URL}/api/hydro/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS),
    )

    async with SlovakiaSHMUConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 3
    ids = {s.native_id for s in stations}
    assert ids == {"SK001", "SK002", "SK003"}

    ba = next(s for s in stations if s.native_id == "SK001")
    assert ba.id == "slovakia_shmu:SK001"
    assert ba.provider == "slovakia_shmu"
    assert ba.country_code == "SK"
    assert ba.river == "Dunaj"
    assert ba.latitude == pytest.approx(48.20)
    assert ba.longitude == pytest.approx(16.98)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_empty():
    """An empty station list returns no stations."""
    respx.get(f"{BASE_URL}/api/hydro/stations").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with SlovakiaSHMUConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_server_error():
    """Server errors return an empty list instead of raising."""
    respx.get(f"{BASE_URL}/api/hydro/stations").mock(
        return_value=httpx.Response(500, text="Server Error"),
    )

    async with SlovakiaSHMUConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations():
    """Observations are parsed correctly."""
    respx.get(f"{BASE_URL}/api/hydro/data").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS),
    )

    async with SlovakiaSHMUConnector() as conn:
        chunk = await conn.fetch_observations(
            "slovakia_shmu:SK001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert chunk.provider == "slovakia_shmu"
    assert chunk.station_id == "slovakia_shmu:SK001"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(1850.0)
    assert chunk.observations[0].quality.value == "raw"
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty():
    """An empty observations list returns zero observations."""
    respx.get(f"{BASE_URL}/api/hydro/data").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with SlovakiaSHMUConnector() as conn:
        chunk = await conn.fetch_observations(
            "slovakia_shmu:SK001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_server_error():
    """Server errors return an empty chunk instead of raising."""
    respx.get(f"{BASE_URL}/api/hydro/data").mock(
        return_value=httpx.Response(500, text="Server Error"),
    )

    async with SlovakiaSHMUConnector() as conn:
        chunk = await conn.fetch_observations(
            "slovakia_shmu:SK001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0
    assert chunk.provider == "slovakia_shmu"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_wrapped_response():
    """Stations wrapped in a 'data' key are parsed correctly."""
    wrapped = {"data": MOCK_STATIONS}
    respx.get(f"{BASE_URL}/api/hydro/stations").mock(
        return_value=httpx.Response(200, json=wrapped),
    )

    async with SlovakiaSHMUConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 3


def test_connector_is_registered():
    """The connector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("slovakia_shmu")
    assert cls is SlovakiaSHMUConnector
