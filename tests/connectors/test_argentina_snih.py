"""Tests for the Argentina INA connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.argentina_snih import ArgentinaSnihConnector
from csfs.core.models import Resolution, Variable

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
                "count": 120,
                "timeend": "2026-07-12T00:00:00",
            },
        },
        {
            "type": "Feature",
            "properties": {
                "id": 55,
                "estacion_id": 101,
                "var_id": 2,
                "var_nombre": "Altura hidrometrica",
                "count": 88,
                "timeend": "2026-07-12T00:00:00",
            },
        },
        {
            "type": "Feature",
            "properties": {
                "id": 42,
                "estacion_id": 202,
                "var_id": 4,
                "var_nombre": "Caudal medio diario",
                "count": 300,
                "timeend": "2026-07-11T00:00:00",
            },
        },
    ],
}


def _mock_series_route(payload: dict | None = None) -> None:
    """Mock the paginated series-catalogue endpoint."""
    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/series"
    ).mock(
        return_value=httpx.Response(
            200, json=payload if payload is not None else MOCK_SERIES_GEOJSON
        )
    )

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

# Stage ("Altura hidrometrica", var id 2) series 55 -- values in metres.
MOCK_STAGE_OBSERVATIONS_RESPONSE = [
    {
        "series_id": 55,
        "timestart": "2024-01-01T03:00:00.000Z",
        "valor": 2.31,
    },
]


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_list():
    """Stations with valid geometry and a data series are returned."""
    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/estaciones"
    ).mock(return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE))
    _mock_series_route()

    async with ArgentinaSnihConnector() as conn:
        stations = await conn.fetch_stations()

    # Station 303 has empty coordinates, so only 2 are returned
    assert len(stations) == 2
    native_ids = {s.native_id for s in stations}
    assert native_ids == {"101", "202"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_filters_to_series_holders():
    """Stations without any discharge/stage series are dropped."""
    data = MOCK_STATIONS_RESPONSE + [
        {
            "id": 404,
            "nombre": "Precip Only",
            "geom": {"type": "Point", "coordinates": [-60.0, -30.0]},
            "rio": None,
            "tipo": "M",
        },
    ]
    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/estaciones"
    ).mock(return_value=httpx.Response(200, json=data))
    _mock_series_route()

    async with ArgentinaSnihConnector() as conn:
        stations = await conn.fetch_stations()

    # 404 has no series in the catalogue -> filtered out.
    assert {s.native_id for s in stations} == {"101", "202"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_unfiltered_when_series_unavailable():
    """If the series catalogue cannot be fetched, the raw roster is kept."""
    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/estaciones"
    ).mock(return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE))
    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/series"
    ).mock(return_value=httpx.Response(500))

    async with ArgentinaSnihConnector() as conn:
        stations = await conn.fetch_stations()

    assert {s.native_id for s in stations} == {"101", "202"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_field_values():
    """Station fields are mapped correctly."""
    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/estaciones"
    ).mock(return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE))
    _mock_series_route()

    async with ArgentinaSnihConnector() as conn:
        stations = await conn.fetch_stations()

    station_a = next(s for s in stations if s.native_id == "101")
    assert station_a.id == "argentina_snih:101"
    assert station_a.provider == "argentina_snih"
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
    _mock_series_route()

    async with ArgentinaSnihConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_values():
    """Observations are correctly parsed into a TimeSeriesChunk."""
    conn = ArgentinaSnihConnector()
    conn._station_to_series["101"] = 31

    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/series/31/observaciones"
    ).mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE)
    )

    async with conn:
        chunk = await conn.fetch_observations(
            "argentina_snih:101",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 2),
        )

    assert chunk.provider == "argentina_snih"
    assert chunk.station_id == "argentina_snih:101"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].variable is Variable.DISCHARGE
    # Plain "Caudal" declares no aggregation.
    assert chunk.observations[0].resolution is Resolution.UNKNOWN
    assert chunk.observations[0].value == pytest.approx(5.21)
    assert chunk.observations[0].quality.value == "raw"
    assert chunk.observations[2].value is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty():
    """An empty observations array returns zero observations."""
    conn = ArgentinaSnihConnector()
    conn._station_to_series["101"] = 31

    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/series/31/observaciones"
    ).mock(return_value=httpx.Response(200, json=[]))

    async with conn:
        chunk = await conn.fetch_observations(
            "argentina_snih:101",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_resolve_series_id_from_cache():
    """When the series cache is pre-populated, no metadata call is made."""
    conn = ArgentinaSnihConnector()
    conn._station_to_series["101"] = 31

    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/series/31/observaciones"
    ).mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE)
    )

    async with conn:
        chunk = await conn.fetch_observations(
            "argentina_snih:101",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 2),
        )

    assert len(chunk.observations) == 3


@pytest.mark.asyncio
@respx.mock
async def test_resolve_series_id_fetches_metadata():
    """When cache is empty, series metadata is fetched; both variables emitted."""
    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/series"
    ).mock(return_value=httpx.Response(200, json=MOCK_SERIES_GEOJSON))

    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/series/31/observaciones"
    ).mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE)
    )
    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/series/55/observaciones"
    ).mock(
        return_value=httpx.Response(200, json=MOCK_STAGE_OBSERVATIONS_RESPONSE)
    )

    async with ArgentinaSnihConnector() as conn:
        chunk = await conn.fetch_observations(
            "argentina_snih:101",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 2),
        )

    # 3 discharge (series 31) + 1 stage (series 55) observations.
    assert len(chunk.observations) == 4
    assert conn._station_to_series["101"] == 31
    assert conn._station_to_series["202"] == 42
    assert conn._station_to_stage_series["101"] == 55

    stage = [
        o for o in chunk.observations if o.variable is Variable.STAGE
    ]
    assert len(stage) == 1
    # "Altura hidrometrica" is served in metres -- no conversion.
    assert stage[0].value == pytest.approx(2.31)
    assert stage[0].resolution is Resolution.UNKNOWN


@pytest.mark.asyncio
@respx.mock
async def test_resolve_series_id_raises_on_unknown_station():
    """If no discharge or stage series exists for the station, an error is raised."""
    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/series"
    ).mock(return_value=httpx.Response(200, json=MOCK_SERIES_GEOJSON))

    from csfs.core.exceptions import DataFormatError

    async with ArgentinaSnihConnector() as conn:
        with pytest.raises(DataFormatError, match="No discharge or stage series"):
            await conn.fetch_observations(
                "argentina_ina:999",
                start=datetime(2024, 1, 1),
                end=datetime(2024, 1, 2),
            )


@pytest.mark.asyncio
@respx.mock
async def test_build_series_cache_classifies_variables():
    """Caudal series go to the discharge cache, altura series to the stage cache."""
    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/series"
    ).mock(return_value=httpx.Response(200, json=MOCK_SERIES_GEOJSON))

    conn = ArgentinaSnihConnector()
    async with conn:
        await conn._build_series_cache()

    # Station 101 has Caudal (id=31) and Altura (id=55).
    assert conn._station_to_series == {"101": 31, "202": 42}
    assert conn._station_to_stage_series == {"101": 55}

    # Resolutions: "Caudal medio diario" is a daily mean; the plain
    # "Caudal"/"Altura hidrometrica" series declare no aggregation.
    assert conn._series_resolution[42] is Resolution.DAILY_MEAN
    assert conn._series_resolution[31] is Resolution.UNKNOWN
    assert conn._series_resolution[55] is Resolution.UNKNOWN

    assert ArgentinaSnihConnector.supported_variables == ("discharge", "stage")


@pytest.mark.asyncio
@respx.mock
async def test_series_cache_skips_dataless_and_prefers_fresh():
    """Placeholder series are skipped; ties break on newest timeend."""
    _mock_series_route({
        "features": [
            # Dataless placeholder (no count, no timeend) -- skipped.
            {
                "properties": {
                    "id": 90, "estacion_id": 700, "var_nombre": "Caudal",
                },
            },
            # Same variant priority, stale timeend.
            {
                "properties": {
                    "id": 91, "estacion_id": 700, "var_nombre": "Caudal",
                    "count": 5, "timeend": "2020-01-01T00:00:00",
                },
            },
            # Same variant priority, fresh timeend -- wins.
            {
                "properties": {
                    "id": 92, "estacion_id": 700, "var_nombre": "Caudal",
                    "count": 5, "timeend": "2026-07-12T00:00:00",
                },
            },
            # Monthly-mean variant carries its own resolution.
            {
                "properties": {
                    "id": 93, "estacion_id": 701,
                    "var_nombre": "caudal medio mensual",
                    "count": 5, "timeend": "2026-06-01T00:00:00",
                },
            },
        ],
    })

    conn = ArgentinaSnihConnector()
    async with conn:
        await conn._build_series_cache()

    assert conn._station_to_series == {"700": 92, "701": 93}
    assert 90 not in conn._series_resolution
    assert conn._series_resolution[93] is Resolution.MONTHLY_MEAN


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
    _mock_series_route()

    async with ArgentinaSnihConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


# ------------------------------------------------------------------
# Coverage gap tests — invalid coordinate values
# ------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_stations_invalid_coords_skipped():
    """Stations with non-numeric coordinate values are skipped."""
    data = [
        {
            "id": 501,
            "nombre": "Bad Coords Station",
            "geom": {
                "type": "Point",
                "coordinates": ["not_a_number", "also_bad"],
            },
            "rio": None,
        },
        {
            "id": 502,
            "nombre": "Good Station",
            "geom": {
                "type": "Point",
                "coordinates": [-58.5, -34.6],
            },
            "rio": "Parana",
        },
    ]
    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/estaciones"
    ).mock(return_value=httpx.Response(200, json=data))
    _mock_series_route({
        "features": [
            {
                "properties": {
                    "id": 71,
                    "estacion_id": 501,
                    "var_nombre": "Caudal",
                    "count": 10,
                    "timeend": "2026-07-12T00:00:00",
                },
            },
            {
                "properties": {
                    "id": 72,
                    "estacion_id": 502,
                    "var_nombre": "Caudal",
                    "count": 10,
                    "timeend": "2026-07-12T00:00:00",
                },
            },
        ],
    })

    async with ArgentinaSnihConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "502"


# ------------------------------------------------------------------
# Coverage gap tests — station append ValueError/KeyError
# ------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_stations_append_failure_skipped():
    """Stations that fail during Station model creation are skipped."""
    # A station with valid coordinates but missing nombre (still works
    # since nombre defaults to native_id). To trigger ValueError/KeyError
    # in the Station constructor, we need unusual data. Since the Station
    # model is quite permissive, we'll test that malformed entries don't crash.
    data = [
        {
            "id": 601,
            "nombre": None,
            "geom": {
                "type": "Point",
                "coordinates": [-58.5, -34.6],
            },
            "rio": None,
        },
    ]
    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/estaciones"
    ).mock(return_value=httpx.Response(200, json=data))
    _mock_series_route({
        "features": [
            {
                "properties": {
                    "id": 81,
                    "estacion_id": 601,
                    "var_nombre": "Caudal",
                    "count": 10,
                    "timeend": "2026-07-12T00:00:00",
                },
            },
        ],
    })

    async with ArgentinaSnihConnector() as conn:
        stations = await conn.fetch_stations()

    # Station with None nombre uses native_id as name
    assert len(stations) == 1
    assert stations[0].name == "601"


# ------------------------------------------------------------------
# Coverage gap tests — observation with invalid timestamp
# ------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_observations_invalid_timestamp_raises():
    """Invalid timestamp in observation raises DataFormatError."""
    from csfs.core.exceptions import DataFormatError

    conn = ArgentinaSnihConnector()
    conn._station_to_series["101"] = 31

    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/series/31/observaciones"
    ).mock(
        return_value=httpx.Response(200, json=[
            {"timestart": "not-a-timestamp", "valor": 5.0},
        ])
    )

    async with conn:
        with pytest.raises(DataFormatError, match="Invalid timestamp"):
            await conn.fetch_observations(
                "argentina_snih:101",
                start=datetime(2024, 1, 1),
                end=datetime(2024, 1, 2),
            )


@pytest.mark.asyncio
@respx.mock
async def test_observations_missing_timestamp_key_raises():
    """Missing 'timestart' key in observation raises DataFormatError."""
    from csfs.core.exceptions import DataFormatError

    conn = ArgentinaSnihConnector()
    conn._station_to_series["101"] = 31

    respx.get(
        "https://alerta.ina.gob.ar/a5/obs/puntual/series/31/observaciones"
    ).mock(
        return_value=httpx.Response(200, json=[
            {"valor": 5.0},  # no timestart key
        ])
    )

    async with conn:
        with pytest.raises(DataFormatError, match="Invalid timestamp"):
            await conn.fetch_observations(
                "argentina_snih:101",
                start=datetime(2024, 1, 1),
                end=datetime(2024, 1, 2),
            )

