"""Tests for the Spain CEDEX Anuario de Aforos connector."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.spain_cedex import SpainCEDEXConnector
from csfs.core.exceptions import ConnectorError

MOCK_STATIONS_RESPONSE = [
    {
        "codigo": "1001",
        "nombre": "Ebro en Miranda",
        "latitud": 42.6833,
        "longitud": -2.9472,
        "rio": "Ebro",
        "superficie_km2": 1530.0,
    },
    {
        "codigo": "1002",
        "nombre": "Ebro en Tortosa",
        "latitud": 40.8125,
        "longitud": 0.5218,
        "rio": "Ebro",
        "superficie_km2": 84230.0,
    },
    {
        "codigo": "",
        "nombre": "Empty Code Station",
        "latitud": 41.0,
        "longitud": -1.0,
        "rio": "Unknown",
    },
    {
        "codigo": "1003",
        "nombre": "No Coords Station",
        "rio": "Segre",
    },
]

MOCK_CSV_RESPONSE = """\
fecha;caudal
1950-01-01;120.5
1950-01-02;115.3
1950-01-03;-
1950-01-04;108.7
1950-01-05;102.0
"""

MOCK_CSV_COMMA_DELIMITED = """\
fecha,caudal
1950-01-01,120.5
1950-01-02,115.3
"""


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_list():
    """Station list is parsed and invalid entries are skipped."""
    respx.get(
        "https://ceh.cedex.es/anuarioaforos/inventario.asp"
    ).mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE),
    )

    async with SpainCEDEXConnector() as conn:
        stations = await conn.fetch_stations()

    # Empty codigo and missing coords should be skipped
    assert len(stations) == 2
    native_ids = {s.native_id for s in stations}
    assert native_ids == {"1001", "1002"}

    st = next(s for s in stations if s.native_id == "1001")
    assert st.id == "spain_cedex:1001"
    assert st.provider == "spain_cedex"
    assert st.country_code == "ES"
    assert st.river == "Ebro"
    assert st.latitude == pytest.approx(42.6833)
    assert st.longitude == pytest.approx(-2.9472)
    assert st.catchment_area_km2 == pytest.approx(1530.0)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_wrapped_response():
    """Stations wrapped in an 'estaciones' key are parsed."""
    wrapped = {"estaciones": MOCK_STATIONS_RESPONSE[:2]}
    respx.get(
        "https://ceh.cedex.es/anuarioaforos/inventario.asp"
    ).mock(return_value=httpx.Response(200, json=wrapped))

    async with SpainCEDEXConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty station list returns no stations."""
    respx.get(
        "https://ceh.cedex.es/anuarioaforos/inventario.asp"
    ).mock(return_value=httpx.Response(200, json=[]))

    async with SpainCEDEXConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_http_error():
    """ConnectorError is raised on HTTP failure."""
    respx.get(
        "https://ceh.cedex.es/anuarioaforos/inventario.asp"
    ).mock(return_value=httpx.Response(500))

    async with SpainCEDEXConnector() as conn:
        with pytest.raises(ConnectorError, match="spain_cedex"):
            await conn.fetch_stations()


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_csv():
    """Observations CSV is correctly parsed into a TimeSeriesChunk."""
    respx.get(
        "https://ceh.cedex.es/anuarioaforos/datos.asp"
    ).mock(return_value=httpx.Response(200, text=MOCK_CSV_RESPONSE))

    async with SpainCEDEXConnector() as conn:
        chunk = await conn.fetch_observations(
            "spain_cedex:1001",
            start=datetime(1950, 1, 1, tzinfo=UTC),
            end=datetime(1950, 1, 5, tzinfo=UTC),
        )

    assert chunk.provider == "spain_cedex"
    assert chunk.station_id == "spain_cedex:1001"
    assert len(chunk.observations) == 5

    # First obs: valid discharge
    assert chunk.observations[0].discharge_m3s == pytest.approx(120.5)
    assert chunk.observations[0].quality.value == "raw"

    # Third obs: "-" value should yield MISSING
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_date_filtering():
    """Only observations within [start, end] are returned."""
    respx.get(
        "https://ceh.cedex.es/anuarioaforos/datos.asp"
    ).mock(return_value=httpx.Response(200, text=MOCK_CSV_RESPONSE))

    async with SpainCEDEXConnector() as conn:
        chunk = await conn.fetch_observations(
            "spain_cedex:1001",
            start=datetime(1950, 1, 2, tzinfo=UTC),
            end=datetime(1950, 1, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    dates = [obs.timestamp.day for obs in chunk.observations]
    assert dates == [2, 3]


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_comma_delimited():
    """CSV with comma delimiter is handled correctly."""
    respx.get(
        "https://ceh.cedex.es/anuarioaforos/datos.asp"
    ).mock(
        return_value=httpx.Response(
            200, text=MOCK_CSV_COMMA_DELIMITED,
        ),
    )

    async with SpainCEDEXConnector() as conn:
        chunk = await conn.fetch_observations(
            "spain_cedex:1001",
            start=datetime(1950, 1, 1, tzinfo=UTC),
            end=datetime(1950, 1, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(120.5)
    assert chunk.observations[1].discharge_m3s == pytest.approx(115.3)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_strips_prefix():
    """The native_id is extracted correctly from the full station_id."""
    respx.get(
        "https://ceh.cedex.es/anuarioaforos/datos.asp"
    ).mock(
        return_value=httpx.Response(200, text="fecha;caudal\n"),
    )

    async with SpainCEDEXConnector() as conn:
        chunk = await conn.fetch_observations(
            "spain_cedex:1001",
            start=datetime(1950, 1, 1, tzinfo=UTC),
            end=datetime(1950, 1, 5, tzinfo=UTC),
        )

    request = respx.calls.last.request
    assert "estacion=1001" in str(request.url)
    assert chunk.station_id == "spain_cedex:1001"
