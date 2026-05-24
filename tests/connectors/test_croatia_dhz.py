"""Tests for the Croatia DHZ connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.croatia_dhz import CroatiaDhzConnector

BASE_URL = "https://hidro.dhz.hr"

MOCK_STATIONS = [
    {
        "sifra": "1001",
        "naziv": "Zagreb - Podsused",
        "lat": 45.8000,
        "lon": 15.8700,
        "rijeka": "Sava",
        "sliv": 12200.0,
    },
    {
        "sifra": "1002",
        "naziv": "Karlovac",
        "lat": 45.4900,
        "lon": 15.5500,
        "rijeka": "Kupa",
        "sliv": 8800.0,
    },
    {
        "sifra": "",
        "naziv": "Missing Code",
        "lat": 45.0,
        "lon": 16.0,
    },
    {
        "sifra": "1003",
        "naziv": "No Coords",
        "rijeka": "Drava",
    },
]

MOCK_OBSERVATIONS = [
    {
        "datum": "2024-06-01T12:00:00",
        "vrijednost": 120.5,
    },
    {
        "datum": "2024-06-01T12:15:00",
        "vrijednost": 118.3,
    },
    {
        "datum": "2024-06-01T12:30:00",
        "vrijednost": None,
    },
]


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_list():
    """Station list is parsed and invalid entries are skipped."""
    respx.get(f"{BASE_URL}/api/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS),
    )

    async with CroatiaDhzConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"1001", "1002"}

    zagreb = next(s for s in stations if s.native_id == "1001")
    assert zagreb.id == "croatia_dhz:1001"
    assert zagreb.provider == "croatia_dhz"
    assert zagreb.country_code == "HR"
    assert zagreb.river == "Sava"
    assert zagreb.latitude == pytest.approx(45.8000)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty station list returns no stations."""
    respx.get(f"{BASE_URL}/api/stations").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with CroatiaDhzConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_wrapped_response():
    """Stations wrapped in a 'stations' key are parsed correctly."""
    wrapped = {"stations": MOCK_STATIONS[:2]}
    respx.get(f"{BASE_URL}/api/stations").mock(
        return_value=httpx.Response(200, json=wrapped),
    )

    async with CroatiaDhzConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_json():
    """Observations are correctly parsed into a TimeSeriesChunk."""
    respx.get(f"{BASE_URL}/api/data").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS),
    )

    async with CroatiaDhzConnector() as conn:
        chunk = await conn.fetch_observations(
            "croatia_dhz:1001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "croatia_dhz"
    assert chunk.station_id == "croatia_dhz:1001"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(120.5)
    assert chunk.observations[0].quality.value == "raw"
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty():
    """An empty observation list returns zero observations."""
    respx.get(f"{BASE_URL}/api/data").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with CroatiaDhzConnector() as conn:
        chunk = await conn.fetch_observations(
            "croatia_dhz:1001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_wrapped_response():
    """Observations wrapped in a 'podaci' key are parsed correctly."""
    wrapped = {"podaci": MOCK_OBSERVATIONS[:2]}
    respx.get(f"{BASE_URL}/api/data").mock(
        return_value=httpx.Response(200, json=wrapped),
    )

    async with CroatiaDhzConnector() as conn:
        chunk = await conn.fetch_observations(
            "croatia_dhz:1001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_strips_prefix():
    """The native_id is extracted correctly from the full station_id."""
    respx.get(f"{BASE_URL}/api/data").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with CroatiaDhzConnector() as conn:
        chunk = await conn.fetch_observations(
            "croatia_dhz:1001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    request = respx.calls.last.request
    assert "station=1001" in str(request.url)
    assert chunk.station_id == "croatia_dhz:1001"


def test_connector_is_registered():
    """The connector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("croatia_dhz")
    assert cls is CroatiaDhzConnector
