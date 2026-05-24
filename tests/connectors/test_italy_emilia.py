"""Tests for the Italy ARPAE Emilia-Romagna connector with mocked HTTP."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.italy_emilia import ItalyEmiliaConnector
from csfs.core.exceptions import ConnectorError

MOCK_STATIONS = [
    {
        "codice": "ER001",
        "denominazione": "Pontelagoscuro",
        "latitudine": 44.8853,
        "longitudine": 11.6048,
        "bacino": "Po",
    },
    {
        "codice": "ER002",
        "denominazione": "Boretto",
        "latitudine": 44.9010,
        "longitudine": 10.5551,
        "bacino": "Po",
    },
    {
        "codice": "",
        "denominazione": "No Code",
        "latitudine": 44.0,
        "longitudine": 11.0,
    },
    {
        "codice": "ER003",
        "denominazione": "No Coords",
        "bacino": "Reno",
    },
]

MOCK_OBSERVATIONS = [
    {"data": "2024-06-01T12:00:00", "valore": 850.2},
    {"data": "2024-06-01T12:15:00", "valore": 845.0},
    {"data": "2024-06-01T12:30:00", "valore": None},
]

BASE = "https://simc.arpae.it/dext3r"


@respx.mock
async def test_fetch_stations_parses_list():
    """Station list is parsed; invalid entries are skipped."""
    respx.get(f"{BASE}/api/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS),
    )

    async with ItalyEmiliaConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"ER001", "ER002"}

    s = next(s for s in stations if s.native_id == "ER001")
    assert s.id == "italy_emilia:ER001"
    assert s.country_code == "IT"
    assert s.river == "Po"
    assert s.latitude == pytest.approx(44.8853)


@respx.mock
async def test_fetch_stations_handles_wrapped():
    """Stations wrapped in a 'stazioni' key are parsed."""
    wrapped = {"stazioni": MOCK_STATIONS[:2]}
    respx.get(f"{BASE}/api/stations").mock(
        return_value=httpx.Response(200, json=wrapped),
    )

    async with ItalyEmiliaConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@respx.mock
async def test_fetch_stations_empty():
    """An empty list returns no stations."""
    respx.get(f"{BASE}/api/stations").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with ItalyEmiliaConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@respx.mock
async def test_fetch_observations_parses_json():
    """Observations are parsed into a TimeSeriesChunk."""
    respx.get(f"{BASE}/api/data").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS),
    )

    async with ItalyEmiliaConnector() as conn:
        chunk = await conn.fetch_observations(
            "italy_emilia:ER001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "italy_emilia"
    assert chunk.station_id == "italy_emilia:ER001"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(850.2)
    assert chunk.observations[0].quality.value == "raw"
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@respx.mock
async def test_fetch_observations_wrapped():
    """Observations wrapped in a 'values' key are handled."""
    wrapped = {"values": MOCK_OBSERVATIONS[:2]}
    respx.get(f"{BASE}/api/data").mock(
        return_value=httpx.Response(200, json=wrapped),
    )

    async with ItalyEmiliaConnector() as conn:
        chunk = await conn.fetch_observations(
            "italy_emilia:ER001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 2


@respx.mock
async def test_fetch_observations_empty():
    """An empty observation list returns zero observations."""
    respx.get(f"{BASE}/api/data").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with ItalyEmiliaConnector() as conn:
        chunk = await conn.fetch_observations(
            "italy_emilia:ER001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@respx.mock
async def test_fetch_observations_strips_prefix():
    """The native_id is extracted from the full station_id."""
    respx.get(f"{BASE}/api/data").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with ItalyEmiliaConnector() as conn:
        chunk = await conn.fetch_observations(
            "italy_emilia:ER001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    request = respx.calls.last.request
    assert "station=ER001" in str(request.url)
    assert chunk.station_id == "italy_emilia:ER001"


@respx.mock
async def test_fetch_stations_http_error():
    """HTTP errors are wrapped in ConnectorError."""
    respx.get(f"{BASE}/api/stations").mock(
        return_value=httpx.Response(500),
    )

    async with ItalyEmiliaConnector() as conn:
        with pytest.raises(ConnectorError, match="italy_emilia"):
            await conn.fetch_stations()
