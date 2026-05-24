"""Tests for the Portugal SNIRH connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.portugal_snirh import PortugalSnirhConnector

BASE_URL = "https://snirh.apambiente.pt"

MOCK_STATIONS = [
    {
        "codigo": "12G/01",
        "nome": "Almourol",
        "latitude": 39.4617,
        "longitude": -8.3833,
        "bacia": 67500.0,
        "curso_agua": "Tejo",
    },
    {
        "codigo": "15F/02",
        "nome": "Ponte de Sor",
        "latitude": 39.2500,
        "longitude": -8.0000,
        "bacia": 3300.0,
        "curso_agua": "Sor",
    },
    {
        "codigo": "",
        "nome": "Missing ID",
        "latitude": 40.0,
        "longitude": -8.0,
    },
    {
        "codigo": "99Z/01",
        "nome": "No Coords",
        "curso_agua": "Unknown",
    },
]

MOCK_OBSERVATIONS = [
    {
        "data": "2024-06-01T12:00:00",
        "valor": 34.5,
    },
    {
        "data": "2024-06-01T12:15:00",
        "valor": 36.1,
    },
    {
        "data": "2024-06-01T12:30:00",
        "valor": None,
    },
]


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_list():
    """Station list is parsed and invalid entries are skipped."""
    respx.get(f"{BASE_URL}/snirh/download/cen498/stations.json").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS),
    )

    async with PortugalSnirhConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"12G/01", "15F/02"}

    almourol = next(s for s in stations if s.native_id == "12G/01")
    assert almourol.id == "portugal_snirh:12G/01"
    assert almourol.provider == "portugal_snirh"
    assert almourol.country_code == "PT"
    assert almourol.river == "Tejo"
    assert almourol.latitude == pytest.approx(39.4617)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty station list returns no stations."""
    respx.get(f"{BASE_URL}/snirh/download/cen498/stations.json").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with PortugalSnirhConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_wrapped_response():
    """Stations wrapped in a 'stations' key are parsed correctly."""
    wrapped = {"stations": MOCK_STATIONS[:2]}
    respx.get(f"{BASE_URL}/snirh/download/cen498/stations.json").mock(
        return_value=httpx.Response(200, json=wrapped),
    )

    async with PortugalSnirhConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_json():
    """Observations are correctly parsed into a TimeSeriesChunk."""
    respx.get(
        f"{BASE_URL}/snirh/download/cen498/data/12G%2F01",
    ).mock(return_value=httpx.Response(200, json=MOCK_OBSERVATIONS))

    async with PortugalSnirhConnector() as conn:
        chunk = await conn.fetch_observations(
            "portugal_snirh:12G/01",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "portugal_snirh"
    assert chunk.station_id == "portugal_snirh:12G/01"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(34.5)
    assert chunk.observations[0].quality.value == "raw"
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty():
    """An empty observation list returns zero observations."""
    respx.get(
        f"{BASE_URL}/snirh/download/cen498/data/12G%2F01",
    ).mock(return_value=httpx.Response(200, json=[]))

    async with PortugalSnirhConnector() as conn:
        chunk = await conn.fetch_observations(
            "portugal_snirh:12G/01",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_wrapped_response():
    """Observations wrapped in a 'dados' key are parsed correctly."""
    wrapped = {"dados": MOCK_OBSERVATIONS[:2]}
    respx.get(
        f"{BASE_URL}/snirh/download/cen498/data/12G%2F01",
    ).mock(return_value=httpx.Response(200, json=wrapped))

    async with PortugalSnirhConnector() as conn:
        chunk = await conn.fetch_observations(
            "portugal_snirh:12G/01",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_http_error():
    """HTTP errors are wrapped in ConnectorError."""
    respx.get(f"{BASE_URL}/snirh/download/cen498/stations.json").mock(
        return_value=httpx.Response(500, text="Server Error"),
    )

    async with PortugalSnirhConnector() as conn:
        with pytest.raises(Exception, match="Failed to fetch"):
            await conn.fetch_stations()


def test_connector_is_registered():
    """The connector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("portugal_snirh")
    assert cls is PortugalSnirhConnector
