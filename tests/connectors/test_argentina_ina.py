"""Tests for the Argentina INA connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.argentina_ina import ArgentinaINAConnector

MOCK_STATIONS_RESPONSE = [
    {
        "id": 101,
        "nombre": "San Martin",
        "geom": {
            "type": "Point",
            "coordinates": [-58.5, -34.6],
        },
        "rio": "Parana",
        "tipo": "H",
    },
    {
        "id": 202,
        "nombre": "Corrientes",
        "geom": {
            "type": "Point",
            "coordinates": [-58.8, -27.5],
        },
        "rio": "Uruguay",
        "tipo": "H",
    },
    {
        "id": 303,
        "nombre": "Bad Geom",
        "geom": {"type": "Point", "coordinates": []},
        "rio": None,
        "tipo": "H",
    },
]

MOCK_SERIES_GEOJSON = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "properties": {
                "id": 31,
                "estacion_id": 101,
                "var_id": 4,
                "var_nombre": "Caudal",
            },
        },
        {
            "type": "Feature",
            "properties": {
                "id": 55,
                "estacion_id": 101,
                "var_id": 2,
                "var_nombre": "Altura hidrometrica",
            },
        },
        {
            "type": "Feature",
            "properties": {
                "id": 42,
                "estacion_id": 202,
                "var_id": 4,
                "var_nombre": "Caudal medio diario",
            },
        },
    ],
}

MOCK_OBSERVATIONS_RESPONSE = [
    {
        "series_id": 31,
        "timestart": "2024-01-01T03:00:00.000Z",
        "valor": 5.21,
    },
    {
        "series_id": 31,
        "timestart": "2024-01-01T06:00:00.000Z",
        "valor": 5.35,
    },
    {
        "series_id": 31,
        "timestart": "2024-01-01T09:00:00.000Z",
        "valor": None,
    },
]


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_list():
    """Stations with valid geometry are parsed correctly."""
    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/estaciones"
    ).mock(return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE))

    async with ArgentinaINAConnector() as conn:
        stations = await conn.fetch_stations()

    # Station 303 has empty coordinates, so only 2 are returned
    assert len(stations) == 2
    native_ids = {s.native_id for s in stations}
    assert native_ids == {"101", "202"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_field_values():
    """Station fields are mapped correctly."""
    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/estaciones"
    ).mock(return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE))

    async with ArgentinaINAConnector() as conn:
        stations = await conn.fetch_stations()

    station_a = next(s for s in stations if s.native_id == "101")
    assert station_a.id == "argentina_ina:101"
    assert station_a.provider == "argentina_ina"
    assert station_a.name == "San Martin"
    assert station_a.country_code == "AR"
    assert station_a.river == "Parana"
    assert station_a.latitude == pytest.approx(-34.6)
    assert station_a.longitude == pytest.approx(-58.5)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty station list returns no stations."""
    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/estaciones"
    ).mock(return_value=httpx.Response(200, json=[]))

    async with ArgentinaINAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_values():
    """Observations are correctly parsed into a TimeSeriesChunk."""
    conn = ArgentinaINAConnector()
    conn._station_to_series["101"] = 31

    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/series/31/observaciones"
    ).mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE)
    )

    async with conn:
        chunk = await conn.fetch_observations(
            "argentina_ina:101",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 2),
        )

    assert chunk.provider == "argentina_ina"
    assert chunk.station_id == "argentina_ina:101"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(5.21)
    assert chunk.observations[0].quality.value == "raw"
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty():
    """An empty observations array returns zero observations."""
    conn = ArgentinaINAConnector()
    conn._station_to_series["101"] = 31

    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/series/31/observaciones"
    ).mock(return_value=httpx.Response(200, json=[]))

    async with conn:
        chunk = await conn.fetch_observations(
            "argentina_ina:101",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_resolve_series_id_from_cache():
    """When the series cache is pre-populated, no metadata call is made."""
    conn = ArgentinaINAConnector()
    conn._station_to_series["101"] = 31

    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/series/31/observaciones"
    ).mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE)
    )

    async with conn:
        chunk = await conn.fetch_observations(
            "argentina_ina:101",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 2),
        )

    assert len(chunk.observations) == 3


@pytest.mark.asyncio
@respx.mock
async def test_resolve_series_id_fetches_metadata():
    """When cache is empty, series metadata is fetched first."""
    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/series"
    ).mock(return_value=httpx.Response(200, json=MOCK_SERIES_GEOJSON))

    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/series/31/observaciones"
    ).mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE)
    )

    async with ArgentinaINAConnector() as conn:
        chunk = await conn.fetch_observations(
            "argentina_ina:101",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 2),
        )

    assert len(chunk.observations) == 3
    assert conn._station_to_series["101"] == 31
    assert conn._station_to_series["202"] == 42


@pytest.mark.asyncio
@respx.mock
async def test_resolve_series_id_raises_on_unknown_station():
    """If no discharge series exists for the station, an error is raised."""
    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/series"
    ).mock(return_value=httpx.Response(200, json=MOCK_SERIES_GEOJSON))

    from csfs.core.exceptions import DataFormatError

    async with ArgentinaINAConnector() as conn:
        with pytest.raises(DataFormatError, match="No discharge series"):
            await conn.fetch_observations(
                "argentina_ina:999",
                start=datetime(2024, 1, 1),
                end=datetime(2024, 1, 2),
            )


@pytest.mark.asyncio
@respx.mock
async def test_build_series_cache_filters_discharge_only():
    """Only series whose var_nombre contains 'caudal' are cached."""
    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/series"
    ).mock(return_value=httpx.Response(200, json=MOCK_SERIES_GEOJSON))

    conn = ArgentinaINAConnector()
    async with conn:
        await conn._build_series_cache()

    # Station 101 has Caudal (id=31) and Altura (id=55); only Caudal cached
    assert conn._station_to_series["101"] == 31
    # Station 202 has "Caudal medio diario" -> also matched
    assert conn._station_to_series["202"] == 42
    # Only 2 entries total (Altura was excluded)
    assert len(conn._station_to_series) == 2


@pytest.mark.asyncio
@respx.mock
async def test_stations_skip_missing_id():
    """Stations without an 'id' field are skipped."""
    data = [
        {
            "nombre": "No ID Station",
            "geom": {"type": "Point", "coordinates": [-58.5, -34.6]},
        },
    ]
    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/estaciones"
    ).mock(return_value=httpx.Response(200, json=data))

    async with ArgentinaINAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0
