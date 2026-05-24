"""Tests for the Italy ISPRA SINTAI connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.italy_ispra import ItalyISPRAConnector
from csfs.core.exceptions import ConnectorError

MOCK_STATIONS = [
    {
        "StationCode": "IT001",
        "StationName": "Roma Ripetta",
        "Latitude": 41.9028,
        "Longitude": 12.4964,
        "RiverName": "Tevere",
    },
    {
        "StationCode": "IT002",
        "StationName": "Firenze Uffizi",
        "Latitude": 43.7696,
        "Longitude": 11.2558,
        "RiverName": "Arno",
    },
    {
        "StationCode": "",
        "StationName": "Missing ID",
        "Latitude": 45.0,
        "Longitude": 10.0,
    },
    {
        "StationCode": "IT003",
        "StationName": "No Coords",
        "RiverName": "Po",
    },
]

MOCK_OBSERVATIONS = [
    {"DateTime": "2024-06-01T12:00:00", "Value": 120.5},
    {"DateTime": "2024-06-01T12:15:00", "Value": 118.3},
    {"DateTime": "2024-06-01T12:30:00", "Value": None},
]

BASE = "http://www.hiscentral.isprambiente.gov.it"


@respx.mock
async def test_fetch_stations_parses_list():
    """Station list is parsed; invalid entries are skipped."""
    respx.get(f"{BASE}/hiscentral/hydromap/getStations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS),
    )

    async with ItalyISPRAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"IT001", "IT002"}

    s = next(s for s in stations if s.native_id == "IT001")
    assert s.id == "italy_ispra:IT001"
    assert s.country_code == "IT"
    assert s.river == "Tevere"
    assert s.latitude == pytest.approx(41.9028)


@respx.mock
async def test_fetch_stations_handles_wrapped():
    """Stations wrapped in a 'stations' key are parsed."""
    wrapped = {"stations": MOCK_STATIONS[:2]}
    respx.get(f"{BASE}/hiscentral/hydromap/getStations").mock(
        return_value=httpx.Response(200, json=wrapped),
    )

    async with ItalyISPRAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@respx.mock
async def test_fetch_stations_empty():
    """An empty list returns no stations."""
    respx.get(f"{BASE}/hiscentral/hydromap/getStations").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with ItalyISPRAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@respx.mock
async def test_fetch_observations_parses_json():
    """Observations are parsed into a TimeSeriesChunk."""
    respx.get(f"{BASE}/hiscentral/hydromap/getValues").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS),
    )

    async with ItalyISPRAConnector() as conn:
        chunk = await conn.fetch_observations(
            "italy_ispra:IT001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "italy_ispra"
    assert chunk.station_id == "italy_ispra:IT001"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(120.5)
    assert chunk.observations[0].quality.value == "raw"
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@respx.mock
async def test_fetch_observations_wrapped_response():
    """Observations wrapped in a 'values' key are handled."""
    wrapped = {"values": MOCK_OBSERVATIONS[:2]}
    respx.get(f"{BASE}/hiscentral/hydromap/getValues").mock(
        return_value=httpx.Response(200, json=wrapped),
    )

    async with ItalyISPRAConnector() as conn:
        chunk = await conn.fetch_observations(
            "italy_ispra:IT001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 2


@respx.mock
async def test_fetch_observations_empty():
    """An empty observation list returns zero observations."""
    respx.get(f"{BASE}/hiscentral/hydromap/getValues").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with ItalyISPRAConnector() as conn:
        chunk = await conn.fetch_observations(
            "italy_ispra:IT001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@respx.mock
async def test_fetch_observations_strips_prefix():
    """The native_id is extracted from the full station_id."""
    respx.get(f"{BASE}/hiscentral/hydromap/getValues").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with ItalyISPRAConnector() as conn:
        chunk = await conn.fetch_observations(
            "italy_ispra:IT001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    request = respx.calls.last.request
    assert "stationCode=IT001" in str(request.url)
    assert chunk.station_id == "italy_ispra:IT001"


@respx.mock
async def test_fetch_stations_http_error():
    """HTTP errors are wrapped in ConnectorError."""
    respx.get(f"{BASE}/hiscentral/hydromap/getStations").mock(
        return_value=httpx.Response(500),
    )

    async with ItalyISPRAConnector() as conn:
        with pytest.raises(ConnectorError, match="italy_ispra"):
            await conn.fetch_stations()
