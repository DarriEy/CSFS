"""Tests for the Argentina SNIH connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.argentina_snih import ArgentinaSNIHConnector

MOCK_STATIONS_JSON = [
    {
        "codigo": "AR001",
        "nombre": "Paso de los Libres",
        "latitud": -29.7167,
        "longitud": -57.0833,
        "rio": "Uruguay",
        "cuenca": 243000.0,
    },
    {
        "codigo": "AR002",
        "nombre": "Corrientes",
        "latitud": -27.4833,
        "longitud": -58.8167,
        "rio": "Parana",
        "cuenca": 1510000.0,
    },
]

MOCK_OBSERVATIONS_JSON = {
    "datos": [
        {"fecha": "2024-06-01", "valor": 12500.0},
        {"fecha": "2024-06-02", "valor": 12800.5},
        {"fecha": "2024-06-03", "valor": None},
    ]
}


@respx.mock
async def test_fetch_stations():
    """Stations are fetched and parsed from SNIH API."""
    respx.get(
        "https://snih.hidricosargentina.gob.ar/api/estaciones"
    ).mock(return_value=httpx.Response(200, json=MOCK_STATIONS_JSON))

    async with ArgentinaSNIHConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    s = next(s for s in stations if s.native_id == "AR001")
    assert s.id == "argentina_snih:AR001"
    assert s.name == "Paso de los Libres"
    assert s.river == "Uruguay"
    assert s.country_code == "AR"


@respx.mock
async def test_fetch_stations_failure():
    """Returns empty list on HTTP failure."""
    respx.get(
        "https://snih.hidricosargentina.gob.ar/api/estaciones"
    ).mock(return_value=httpx.Response(500))

    async with ArgentinaSNIHConnector() as conn:
        stations = await conn.fetch_stations()

    assert stations == []


@respx.mock
async def test_fetch_observations():
    """Observations are parsed correctly."""
    respx.get(
        "https://snih.hidricosargentina.gob.ar/api/datos"
    ).mock(return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_JSON))

    async with ArgentinaSNIHConnector() as conn:
        chunk = await conn.fetch_observations(
            "argentina_snih:AR001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 4, tzinfo=UTC),
        )

    assert chunk.provider == "argentina_snih"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(12500.0)
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@respx.mock
async def test_fetch_observations_failure():
    """Returns empty chunk on failure."""
    respx.get(
        "https://snih.hidricosargentina.gob.ar/api/datos"
    ).mock(return_value=httpx.Response(500))

    async with ArgentinaSNIHConnector() as conn:
        chunk = await conn.fetch_observations(
            "argentina_snih:AR001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 4, tzinfo=UTC),
        )

    assert chunk.observations == []


@respx.mock
async def test_fetch_stations_dict_wrapper():
    """Handles response wrapped in a dict."""
    respx.get(
        "https://snih.hidricosargentina.gob.ar/api/estaciones"
    ).mock(
        return_value=httpx.Response(
            200, json={"estaciones": MOCK_STATIONS_JSON}
        )
    )

    async with ArgentinaSNIHConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@respx.mock
async def test_fetch_observations_empty():
    """Empty data returns zero observations."""
    respx.get(
        "https://snih.hidricosargentina.gob.ar/api/datos"
    ).mock(return_value=httpx.Response(200, json={"datos": []}))

    async with ArgentinaSNIHConnector() as conn:
        chunk = await conn.fetch_observations(
            "argentina_snih:AR001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@respx.mock
async def test_connector_registration():
    """Connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("argentina_snih")
    assert cls is ArgentinaSNIHConnector
