"""Tests for the Italy SIR Toscana connector with mocked HTTP."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.italy_tuscany import ItalyTuscanyConnector
from csfs.core.exceptions import ConnectorError

MOCK_STATIONS = [
    {
        "codice": "TOS001",
        "nome": "Firenze Uffizi",
        "latitudine": 43.7696,
        "longitudine": 11.2558,
        "corso_acqua": "Arno",
    },
    {
        "codice": "TOS002",
        "nome": "Pisa San Rossore",
        "latitudine": 43.7228,
        "longitudine": 10.3944,
        "corso_acqua": "Arno",
    },
    {
        "codice": "",
        "nome": "No Code",
        "latitudine": 43.0,
        "longitudine": 11.0,
    },
    {
        "codice": "TOS003",
        "nome": "No Coords",
        "corso_acqua": "Serchio",
    },
]

MOCK_OBSERVATIONS = [
    {"data": "2024-06-01T12:00:00", "valore": 95.3},
    {"data": "2024-06-01T12:15:00", "valore": 93.8},
    {"data": "2024-06-01T12:30:00", "valore": None},
]

BASE = "http://www.sir.toscana.it"


@respx.mock
async def test_fetch_stations_parses_list():
    """Station list is parsed; invalid entries are skipped."""
    respx.get(f"{BASE}/api/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS),
    )

    async with ItalyTuscanyConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"TOS001", "TOS002"}

    s = next(s for s in stations if s.native_id == "TOS001")
    assert s.id == "italy_tuscany:TOS001"
    assert s.country_code == "IT"
    assert s.river == "Arno"
    assert s.latitude == pytest.approx(43.7696)


@respx.mock
async def test_fetch_stations_handles_wrapped():
    """Stations wrapped in a 'stazioni' key are parsed."""
    wrapped = {"stazioni": MOCK_STATIONS[:2]}
    respx.get(f"{BASE}/api/stations").mock(
        return_value=httpx.Response(200, json=wrapped),
    )

    async with ItalyTuscanyConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@respx.mock
async def test_fetch_stations_empty():
    """An empty list returns no stations."""
    respx.get(f"{BASE}/api/stations").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with ItalyTuscanyConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@respx.mock
async def test_fetch_observations_parses_json():
    """Observations are parsed into a TimeSeriesChunk."""
    respx.get(f"{BASE}/api/data").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS),
    )

    async with ItalyTuscanyConnector() as conn:
        chunk = await conn.fetch_observations(
            "italy_tuscany:TOS001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "italy_tuscany"
    assert chunk.station_id == "italy_tuscany:TOS001"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(95.3)
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

    async with ItalyTuscanyConnector() as conn:
        chunk = await conn.fetch_observations(
            "italy_tuscany:TOS001",
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

    async with ItalyTuscanyConnector() as conn:
        chunk = await conn.fetch_observations(
            "italy_tuscany:TOS001",
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

    async with ItalyTuscanyConnector() as conn:
        chunk = await conn.fetch_observations(
            "italy_tuscany:TOS001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    request = respx.calls.last.request
    assert "station=TOS001" in str(request.url)
    assert chunk.station_id == "italy_tuscany:TOS001"


@respx.mock
async def test_fetch_stations_http_error():
    """HTTP errors are wrapped in ConnectorError."""
    respx.get(f"{BASE}/api/stations").mock(
        return_value=httpx.Response(500),
    )

    async with ItalyTuscanyConnector() as conn:
        with pytest.raises(ConnectorError, match="italy_tuscany"):
            await conn.fetch_stations()
