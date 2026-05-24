"""Tests for Mexico CONAGUA BANDAS/SINA connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.mexico_conagua import MexicoCONAGUAConnector

MOCK_STATIONS_JSON = [
    {
        "clave": "10001",
        "nombre": "EL PALMITO",
        "latitud": 24.05,
        "longitud": -105.47,
        "corriente": "RIO NAZAS",
        "region_hidrologica": "RH36",
        "area_cuenca": 18321.0,
    },
    {
        "clave": "10002",
        "nombre": "LAZARO CARDENAS",
        "latitud": 25.82,
        "longitud": -103.58,
        "corriente": "RIO AGUANAVAL",
        "region_hidrologica": "RH36",
        "area_cuenca": 22540.0,
    },
]

MOCK_STATIONS_WRAPPED_JSON = {
    "estaciones": MOCK_STATIONS_JSON,
}

MOCK_STATIONS_MISSING_COORDS = [
    {
        "clave": "10003",
        "nombre": "INCOMPLETE",
        "latitud": None,
        "longitud": None,
        "corriente": "RIO X",
    },
]

MOCK_OBSERVATIONS_JSON = {
    "datos": [
        {
            "fecha": "2024-06-01 12:00:00",
            "valor": 45.3,
            "bandera": "B",
        },
        {
            "fecha": "2024-06-02 12:00:00",
            "valor": 52.1,
            "bandera": None,
        },
        {
            "fecha": "2024-06-03 12:00:00",
            "valor": None,
            "bandera": "M",
        },
    ],
}

MOCK_OBSERVATIONS_LIST_JSON = [
    {"fecha": "2024-07-01", "valor": 100.0, "bandera": "E"},
]

MOCK_EMPTY_OBSERVATIONS_JSON = {"datos": []}

BASE_URL = "https://sina.conagua.gob.mx/sina"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_json():
    respx.get(f"{BASE_URL}/api/estaciones").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_JSON),
    )

    async with MexicoCONAGUAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2

    assert stations[0].native_id == "10001"
    assert stations[0].id == "mexico_conagua:10001"
    assert stations[0].name == "EL PALMITO"
    assert stations[0].latitude == 24.05
    assert stations[0].longitude == -105.47
    assert stations[0].river == "RIO NAZAS"
    assert stations[0].catchment_area_km2 == 18321.0
    assert stations[0].country_code == "MX"
    assert stations[0].is_active is True

    assert stations[1].native_id == "10002"
    assert stations[1].name == "LAZARO CARDENAS"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_wrapped_json():
    """Stations nested under an 'estaciones' key should be unwrapped."""
    respx.get(f"{BASE_URL}/api/estaciones").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_WRAPPED_JSON),
    )

    async with MexicoCONAGUAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    assert stations[0].native_id == "10001"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_missing_coords():
    # Both endpoints return stations with missing coords => all skipped
    respx.get(f"{BASE_URL}/api/estaciones").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_MISSING_COORDS),
    )
    respx.get(f"{BASE_URL}/Estaciones.aspx").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_MISSING_COORDS),
    )

    async with MexicoCONAGUAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fallback_endpoint():
    """When the first endpoint fails, the connector falls back to the second."""
    respx.get(f"{BASE_URL}/api/estaciones").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE_URL}/Estaciones.aspx").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_JSON),
    )

    async with MexicoCONAGUAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_all_endpoints_fail():
    """When all endpoints fail, return empty list without crashing."""
    respx.get(f"{BASE_URL}/api/estaciones").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE_URL}/Estaciones.aspx").mock(
        return_value=httpx.Response(500),
    )

    async with MexicoCONAGUAConnector() as conn:
        stations = await conn.fetch_stations()

    assert stations == []


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_json():
    respx.get(f"{BASE_URL}/api/datos").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_JSON),
    )

    async with MexicoCONAGUAConnector() as conn:
        chunk = await conn.fetch_observations(
            "mexico_conagua:10001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 3),
        )

    assert chunk.station_id == "mexico_conagua:10001"
    assert chunk.provider == "mexico_conagua"
    assert len(chunk.observations) == 3

    assert chunk.observations[0].discharge_m3s == pytest.approx(45.3)
    assert chunk.observations[0].quality.value == "good"

    assert chunk.observations[1].discharge_m3s == pytest.approx(52.1)
    assert chunk.observations[1].quality.value == "raw"

    # Third has None valor -> MISSING quality
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_list_format():
    """Observations returned as a bare JSON list (not wrapped in a dict)."""
    respx.get(f"{BASE_URL}/api/datos").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_LIST_JSON),
    )

    async with MexicoCONAGUAConnector() as conn:
        chunk = await conn.fetch_observations(
            "mexico_conagua:10001",
            start=datetime(2024, 7, 1),
            end=datetime(2024, 7, 1),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(100.0)
    assert chunk.observations[0].quality.value == "estimated"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty_response():
    respx.get(f"{BASE_URL}/api/datos").mock(
        return_value=httpx.Response(200, json=MOCK_EMPTY_OBSERVATIONS_JSON),
    )

    async with MexicoCONAGUAConnector() as conn:
        chunk = await conn.fetch_observations(
            "mexico_conagua:10001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 3),
        )

    assert len(chunk.observations) == 0
    assert chunk.station_id == "mexico_conagua:10001"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_blank_body():
    respx.get(f"{BASE_URL}/api/datos").mock(
        return_value=httpx.Response(200, text=""),
    )

    async with MexicoCONAGUAConnector() as conn:
        chunk = await conn.fetch_observations(
            "mexico_conagua:10001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 3),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_fallback_endpoint():
    """When the first obs endpoint fails, fall back to the second."""
    respx.get(f"{BASE_URL}/api/datos").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE_URL}/DatosHidrometricos.aspx").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_JSON),
    )

    async with MexicoCONAGUAConnector() as conn:
        chunk = await conn.fetch_observations(
            "mexico_conagua:10001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 3),
        )

    assert len(chunk.observations) == 3


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_all_endpoints_fail():
    """When all endpoints fail, return empty chunk without crashing."""
    respx.get(f"{BASE_URL}/api/datos").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE_URL}/DatosHidrometricos.aspx").mock(
        return_value=httpx.Response(500),
    )

    async with MexicoCONAGUAConnector() as conn:
        chunk = await conn.fetch_observations(
            "mexico_conagua:10001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 3),
        )

    assert len(chunk.observations) == 0
    assert chunk.station_id == "mexico_conagua:10001"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_request_params():
    """Verify the query parameters sent for the primary endpoint."""
    route = respx.get(f"{BASE_URL}/api/datos").mock(
        return_value=httpx.Response(200, json=MOCK_EMPTY_OBSERVATIONS_JSON),
    )

    async with MexicoCONAGUAConnector() as conn:
        await conn.fetch_observations(
            "mexico_conagua:10001",
            start=datetime(2024, 1, 15),
            end=datetime(2024, 12, 25),
        )

    assert route.called
    url = str(route.calls[0].request.url)
    assert "estacion=10001" in url
    assert "variable=Q" in url
    assert "inicio=2024-01-15" in url
    assert "fin=2024-12-25" in url


@pytest.mark.asyncio
@respx.mock
async def test_connector_sets_json_accept_header():
    """Verify that the connector sets Accept: application/json header."""
    route = respx.get(f"{BASE_URL}/api/estaciones").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_JSON),
    )

    async with MexicoCONAGUAConnector() as conn:
        await conn.fetch_stations()

    request = route.calls[0].request
    assert request.headers["accept"] == "application/json"


@pytest.mark.asyncio
@respx.mock
async def test_registration():
    """Verify the connector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("mexico_conagua")
    assert cls is MexicoCONAGUAConnector


@pytest.mark.asyncio
@respx.mock
async def test_station_id_prefix_stripped():
    """Native ID extraction should strip the provider prefix."""
    respx.get(f"{BASE_URL}/api/datos").mock(
        return_value=httpx.Response(200, json=MOCK_EMPTY_OBSERVATIONS_JSON),
    )

    async with MexicoCONAGUAConnector() as conn:
        chunk = await conn.fetch_observations(
            "mexico_conagua:10001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 3),
        )

    assert chunk.station_id == "mexico_conagua:10001"
