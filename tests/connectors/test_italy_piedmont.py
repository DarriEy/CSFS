"""Tests for the Italy ARPA Piemonte connector with mocked HTTP."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.italy_piedmont import ItalyPiedmontConnector
from csfs.core.exceptions import ConnectorError

MOCK_STATIONS = [
    {
        "code": "PM001",
        "name": "Torino Po",
        "lat": 45.0703,
        "lon": 7.6869,
        "river": "Po",
    },
    {
        "code": "PM002",
        "name": "Carignano",
        "lat": 44.9042,
        "lon": 7.6737,
        "river": "Po",
    },
    {
        "code": "",
        "name": "No Code",
        "lat": 45.0,
        "lon": 7.5,
    },
    {
        "code": "PM003",
        "name": "No Coords",
        "river": "Dora Riparia",
    },
]

MOCK_OBSERVATIONS = [
    {"timestamp": "2024-06-01T12:00:00", "value": 210.5},
    {"timestamp": "2024-06-01T12:15:00", "value": 208.0},
    {"timestamp": "2024-06-01T12:30:00", "value": None},
]

BASE = (
    "https://www.arpa.piemonte.it"
    "/rischinaturali/tematismi/dati-idrologici"
)


@respx.mock
async def test_fetch_stations_parses_list():
    """Station list is parsed; invalid entries are skipped."""
    respx.get(f"{BASE}/stations.json").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS),
    )

    async with ItalyPiedmontConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"PM001", "PM002"}

    s = next(s for s in stations if s.native_id == "PM001")
    assert s.id == "italy_piedmont:PM001"
    assert s.country_code == "IT"
    assert s.river == "Po"
    assert s.latitude == pytest.approx(45.0703)


@respx.mock
async def test_fetch_stations_handles_wrapped():
    """Stations wrapped in a 'stations' key are parsed."""
    wrapped = {"stations": MOCK_STATIONS[:2]}
    respx.get(f"{BASE}/stations.json").mock(
        return_value=httpx.Response(200, json=wrapped),
    )

    async with ItalyPiedmontConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@respx.mock
async def test_fetch_stations_empty():
    """An empty list returns no stations."""
    respx.get(f"{BASE}/stations.json").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with ItalyPiedmontConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@respx.mock
async def test_fetch_observations_parses_json():
    """Observations are parsed into a TimeSeriesChunk."""
    respx.get(f"{BASE}/data/PM001/portata").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS),
    )

    async with ItalyPiedmontConnector() as conn:
        chunk = await conn.fetch_observations(
            "italy_piedmont:PM001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "italy_piedmont"
    assert chunk.station_id == "italy_piedmont:PM001"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(210.5)
    assert chunk.observations[0].quality.value == "raw"
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@respx.mock
async def test_fetch_observations_wrapped():
    """Observations wrapped in a 'values' key are handled."""
    wrapped = {"values": MOCK_OBSERVATIONS[:2]}
    respx.get(f"{BASE}/data/PM001/portata").mock(
        return_value=httpx.Response(200, json=wrapped),
    )

    async with ItalyPiedmontConnector() as conn:
        chunk = await conn.fetch_observations(
            "italy_piedmont:PM001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 2


@respx.mock
async def test_fetch_observations_empty():
    """An empty observation list returns zero observations."""
    respx.get(f"{BASE}/data/PM001/portata").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with ItalyPiedmontConnector() as conn:
        chunk = await conn.fetch_observations(
            "italy_piedmont:PM001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@respx.mock
async def test_fetch_observations_strips_prefix():
    """The station code is in the URL path, not query params."""
    respx.get(f"{BASE}/data/PM001/portata").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with ItalyPiedmontConnector() as conn:
        chunk = await conn.fetch_observations(
            "italy_piedmont:PM001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    request = respx.calls.last.request
    assert "/data/PM001/portata" in str(request.url)
    assert chunk.station_id == "italy_piedmont:PM001"


@respx.mock
async def test_fetch_stations_http_error():
    """HTTP errors are wrapped in ConnectorError."""
    respx.get(f"{BASE}/stations.json").mock(
        return_value=httpx.Response(500),
    )

    async with ItalyPiedmontConnector() as conn:
        with pytest.raises(ConnectorError, match="italy_piedmont"):
            await conn.fetch_stations()
