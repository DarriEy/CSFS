"""Tests for the Greece OpenHI connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.greece_openhi import GreeceOpenhiConnector

BASE_URL = "https://openhi.net"

MOCK_STATIONS = [
    {
        "id": "STA001",
        "name": "Aliakmonas - Ilarion",
        "latitude": 40.1900,
        "longitude": 21.7400,
        "river": "Aliakmonas",
    },
    {
        "id": "STA002",
        "name": "Acheloos - Kremasta",
        "latitude": 38.8800,
        "longitude": 21.5100,
        "river": "Acheloos",
    },
    {
        "id": "",
        "name": "Missing ID",
        "latitude": 39.0,
        "longitude": 22.0,
    },
    {
        "id": "STA003",
        "name": "No Coords",
        "river": "Pinios",
    },
]

MOCK_OBSERVATIONS = [
    {
        "timestamp": "2024-06-01T12:00:00",
        "value": 45.3,
        "flag": "VALIDATED",
    },
    {
        "timestamp": "2024-06-01T12:15:00",
        "value": 44.1,
        "flag": "RAW",
    },
    {
        "timestamp": "2024-06-01T12:30:00",
        "value": None,
        "flag": "MISSING",
    },
]


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_list():
    """Station list is parsed and invalid entries are skipped."""
    respx.get(f"{BASE_URL}/api/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS),
    )

    async with GreeceOpenhiConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"STA001", "STA002"}

    ilarion = next(s for s in stations if s.native_id == "STA001")
    assert ilarion.id == "greece_openhi:STA001"
    assert ilarion.provider == "greece_openhi"
    assert ilarion.country_code == "GR"
    assert ilarion.river == "Aliakmonas"
    assert ilarion.latitude == pytest.approx(40.19)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty station list returns no stations."""
    respx.get(f"{BASE_URL}/api/stations").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with GreeceOpenhiConnector() as conn:
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

    async with GreeceOpenhiConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_json():
    """Observations are correctly parsed with quality flags."""
    respx.get(f"{BASE_URL}/api/timeseries/STA001").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS),
    )

    async with GreeceOpenhiConnector() as conn:
        chunk = await conn.fetch_observations(
            "greece_openhi:STA001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "greece_openhi"
    assert chunk.station_id == "greece_openhi:STA001"
    assert len(chunk.observations) == 3

    assert chunk.observations[0].discharge_m3s == pytest.approx(45.3)
    assert chunk.observations[0].quality.value == "good"

    assert chunk.observations[1].discharge_m3s == pytest.approx(44.1)
    assert chunk.observations[1].quality.value == "raw"

    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty():
    """An empty observation list returns zero observations."""
    respx.get(f"{BASE_URL}/api/timeseries/STA001").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with GreeceOpenhiConnector() as conn:
        chunk = await conn.fetch_observations(
            "greece_openhi:STA001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_wrapped_response():
    """Observations wrapped in a 'data' key are parsed correctly."""
    wrapped = {"data": MOCK_OBSERVATIONS[:2]}
    respx.get(f"{BASE_URL}/api/timeseries/STA001").mock(
        return_value=httpx.Response(200, json=wrapped),
    )

    async with GreeceOpenhiConnector() as conn:
        chunk = await conn.fetch_observations(
            "greece_openhi:STA001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_suspect_flag():
    """SUSPECT flag is mapped correctly."""
    data = [
        {"timestamp": "2024-06-01T12:00:00", "value": 30.0, "flag": "SUSPECT"},
    ]
    respx.get(f"{BASE_URL}/api/timeseries/STA001").mock(
        return_value=httpx.Response(200, json=data),
    )

    async with GreeceOpenhiConnector() as conn:
        chunk = await conn.fetch_observations(
            "greece_openhi:STA001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.observations[0].quality.value == "suspect"


def test_connector_is_registered():
    """The connector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("greece_openhi")
    assert cls is GreeceOpenhiConnector
