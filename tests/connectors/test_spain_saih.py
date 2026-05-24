"""Tests for the Spain SAIH/CEDEX connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.spain_saih import SpainSAIHConnector

MOCK_STATIONS_RESPONSE = [
    {
        "codigo": "A001",
        "nombre": "Estación Zaragoza",
        "coordX": -0.8773,
        "coordY": 41.6561,
        "rio": "Ebro",
        "cuenca": 40434.0,
    },
    {
        "codigo": "A002",
        "nombre": "Estación Tortosa",
        "coordX": 0.5218,
        "coordY": 40.8125,
        "rio": "Ebro",
        "cuenca": 84230.0,
    },
    {
        "codigo": "",
        "nombre": "Missing ID",
        "coordX": 1.0,
        "coordY": 42.0,
        "rio": "Unknown",
    },
    {
        "codigo": "A003",
        "nombre": "No Coords",
        "rio": "Segre",
    },
]

MOCK_OBSERVATIONS_RESPONSE = {
    "valores": [
        {
            "fecha": "2024-06-01T12:00:00",
            "valor": 34.5,
            "validado": True,
        },
        {
            "fecha": "2024-06-01T12:15:00",
            "valor": 36.1,
            "validado": False,
        },
        {
            "fecha": "2024-06-01T12:30:00",
            "valor": None,
            "validado": None,
        },
    ],
}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_list():
    """Station list is parsed and invalid entries are skipped."""
    respx.get(
        "https://www.saihebro.com/saihebro/api/datos/estaciones"
    ).mock(return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE))

    async with SpainSAIHConnector() as conn:
        stations = await conn.fetch_stations()

    # Empty codigo and missing coords should be skipped
    assert len(stations) == 2
    native_ids = {s.native_id for s in stations}
    assert native_ids == {"A001", "A002"}

    station_a = next(s for s in stations if s.native_id == "A001")
    assert station_a.id == "spain_saih:A001"
    assert station_a.provider == "spain_saih"
    assert station_a.country_code == "ES"
    assert station_a.river == "Ebro"
    assert station_a.latitude == 41.6561
    assert station_a.longitude == -0.8773
    assert station_a.catchment_area_km2 == 40434.0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty station list returns no stations."""
    respx.get(
        "https://www.saihebro.com/saihebro/api/datos/estaciones"
    ).mock(return_value=httpx.Response(200, json=[]))

    async with SpainSAIHConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_wrapped_response():
    """Stations wrapped in an 'estaciones' key are parsed correctly."""
    wrapped = {"estaciones": MOCK_STATIONS_RESPONSE[:2]}
    respx.get(
        "https://www.saihebro.com/saihebro/api/datos/estaciones"
    ).mock(return_value=httpx.Response(200, json=wrapped))

    async with SpainSAIHConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_json():
    """Observations are correctly parsed into a TimeSeriesChunk."""
    respx.get(
        "https://www.saihebro.com/saihebro/api/datos/valores"
    ).mock(return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE))

    async with SpainSAIHConnector() as conn:
        chunk = await conn.fetch_observations(
            "spain_saih:A001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "spain_saih"
    assert chunk.station_id == "spain_saih:A001"
    assert len(chunk.observations) == 3

    # First observation — validated
    assert chunk.observations[0].discharge_m3s == pytest.approx(34.5)
    assert chunk.observations[0].quality.value == "good"

    # Second observation — not validated
    assert chunk.observations[1].discharge_m3s == pytest.approx(36.1)
    assert chunk.observations[1].quality.value == "raw"

    # Third observation — None value should yield MISSING
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty():
    """An empty valores array returns zero observations."""
    respx.get(
        "https://www.saihebro.com/saihebro/api/datos/valores"
    ).mock(return_value=httpx.Response(200, json={"valores": []}))

    async with SpainSAIHConnector() as conn:
        chunk = await conn.fetch_observations(
            "spain_saih:A001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_bare_list_response():
    """Observations returned as a bare list (no 'valores' wrapper) are handled."""
    bare_list = MOCK_OBSERVATIONS_RESPONSE["valores"][:2]
    respx.get(
        "https://www.saihebro.com/saihebro/api/datos/valores"
    ).mock(return_value=httpx.Response(200, json=bare_list))

    async with SpainSAIHConnector() as conn:
        chunk = await conn.fetch_observations(
            "spain_saih:A001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_integer_validado():
    """Integer validado values (1/0) are mapped correctly."""
    data = {
        "valores": [
            {"fecha": "2024-06-01T10:00:00", "valor": 50.0, "validado": 1},
            {"fecha": "2024-06-01T10:15:00", "valor": 48.0, "validado": 0},
        ],
    }
    respx.get(
        "https://www.saihebro.com/saihebro/api/datos/valores"
    ).mock(return_value=httpx.Response(200, json=data))

    async with SpainSAIHConnector() as conn:
        chunk = await conn.fetch_observations(
            "spain_saih:A001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.observations[0].quality.value == "good"
    assert chunk.observations[1].quality.value == "raw"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_strips_prefix():
    """The native_id is extracted correctly from the full station_id."""
    respx.get(
        "https://www.saihebro.com/saihebro/api/datos/valores"
    ).mock(return_value=httpx.Response(200, json={"valores": []}))

    async with SpainSAIHConnector() as conn:
        chunk = await conn.fetch_observations(
            "spain_saih:A001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    # Verify that the request used the correct native_id param
    request = respx.calls.last.request
    assert "estacion=A001" in str(request.url)
    assert chunk.station_id == "spain_saih:A001"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_uses_latitud_longitud_fallback():
    """Stations using 'latitud'/'longitud' keys instead of coordX/coordY."""
    alt_stations = [
        {
            "codigo": "B001",
            "nombre": "Alt Station",
            "latitud": 42.0,
            "longitud": -1.5,
            "rio": "Aragón",
        },
    ]
    respx.get(
        "https://www.saihebro.com/saihebro/api/datos/estaciones"
    ).mock(return_value=httpx.Response(200, json=alt_stations))

    async with SpainSAIHConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].latitude == 42.0
    assert stations[0].longitude == -1.5
