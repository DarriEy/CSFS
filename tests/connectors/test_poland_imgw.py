"""Tests for the IMGW (Poland) connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.poland_imgw import PolandImgwConnector

MOCK_HYDRO_RESPONSE = [
    {
        "id_stacji": "150190330",
        "stacja": "WARSZAWA",
        "rzeka": "WISŁA",
        "województwo": "mazowieckie",
        "stan_wody": "210",
        "stan_wody_data_pomiaru": "2024-06-01T10:00:00",
        "przepływ": "450.5",
        "data_pomiaru": "2024-06-01T10:00:00",
        "temperatura_wody": "18.5",
    },
    {
        "id_stacji": "150190340",
        "stacja": "KRAKÓW",
        "rzeka": "WISŁA",
        "województwo": "małopolskie",
        "stan_wody": "180",
        "stan_wody_data_pomiaru": "2024-06-01T10:00:00",
        "przepływ": None,
        "data_pomiaru": "2024-06-01T10:00:00",
        "temperatura_wody": "17.0",
    },
    {
        "id_stacji": "150190350",
        "stacja": "GDAŃSK",
        "rzeka": "MOTŁAWA",
        "województwo": "pomorskie",
        "stan_wody": "95",
        "stan_wody_data_pomiaru": "2024-06-01T10:00:00",
        "przepływ": "12.3",
        "data_pomiaru": "2024-06-01T10:00:00",
        "temperatura_wody": "16.0",
    },
]


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_returns_all():
    """All stations from the real-time endpoint are returned."""
    respx.get("https://danepubliczne.imgw.pl/api/data/hydro/").mock(
        return_value=httpx.Response(200, json=MOCK_HYDRO_RESPONSE),
    )

    async with PolandImgwConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 3
    native_ids = {s.native_id for s in stations}
    assert native_ids == {"150190330", "150190340", "150190350"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fields():
    """Station metadata fields are correctly parsed."""
    respx.get("https://danepubliczne.imgw.pl/api/data/hydro/").mock(
        return_value=httpx.Response(200, json=MOCK_HYDRO_RESPONSE),
    )

    async with PolandImgwConnector() as conn:
        stations = await conn.fetch_stations()

    warsaw = next(s for s in stations if s.native_id == "150190330")
    assert warsaw.id == "poland_imgw:150190330"
    assert warsaw.provider == "poland_imgw"
    assert warsaw.name == "WARSZAWA"
    assert warsaw.country_code == "PL"
    assert warsaw.river == "WISŁA"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_deduplicates():
    """Duplicate station IDs in the response are collapsed."""
    duplicate_data = [MOCK_HYDRO_RESPONSE[0], MOCK_HYDRO_RESPONSE[0]]
    respx.get("https://danepubliczne.imgw.pl/api/data/hydro/").mock(
        return_value=httpx.Response(200, json=duplicate_data),
    )

    async with PolandImgwConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_empty():
    """An empty response returns no stations."""
    respx.get("https://danepubliczne.imgw.pl/api/data/hydro/").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with PolandImgwConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_discharge():
    """Observations with a valid discharge value are parsed correctly."""
    respx.get("https://danepubliczne.imgw.pl/api/data/hydro/").mock(
        return_value=httpx.Response(200, json=MOCK_HYDRO_RESPONSE),
    )

    async with PolandImgwConnector() as conn:
        chunk = await conn.fetch_observations(
            "poland_imgw:150190330",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "poland_imgw"
    assert chunk.station_id == "poland_imgw:150190330"
    assert len(chunk.observations) == 1

    obs = chunk.observations[0]
    assert obs.discharge_m3s == pytest.approx(450.5)
    assert obs.quality.value == "raw"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_null_discharge():
    """Stations with null discharge get MISSING quality flag."""
    respx.get("https://danepubliczne.imgw.pl/api/data/hydro/").mock(
        return_value=httpx.Response(200, json=MOCK_HYDRO_RESPONSE),
    )

    async with PolandImgwConnector() as conn:
        chunk = await conn.fetch_observations(
            "poland_imgw:150190340",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 1
    obs = chunk.observations[0]
    assert obs.discharge_m3s is None
    assert obs.quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_unknown_station():
    """Requesting a station not in the response returns zero observations."""
    respx.get("https://danepubliczne.imgw.pl/api/data/hydro/").mock(
        return_value=httpx.Response(200, json=MOCK_HYDRO_RESPONSE),
    )

    async with PolandImgwConnector() as conn:
        chunk = await conn.fetch_observations(
            "poland_imgw:999999999",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest():
    """fetch_latest returns the current snapshot for a station."""
    respx.get("https://danepubliczne.imgw.pl/api/data/hydro/").mock(
        return_value=httpx.Response(200, json=MOCK_HYDRO_RESPONSE),
    )

    async with PolandImgwConnector() as conn:
        chunk = await conn.fetch_latest("poland_imgw:150190350")

    assert chunk.station_id == "poland_imgw:150190350"
    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(12.3)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty_discharge_string():
    """An empty-string discharge is treated as MISSING."""
    data = [
        {
            "id_stacji": "150190330",
            "stacja": "WARSZAWA",
            "rzeka": "WISŁA",
            "stan_wody": "210",
            "stan_wody_data_pomiaru": "2024-06-01T10:00:00",
            "przepływ": "",
            "data_pomiaru": "2024-06-01T10:00:00",
        },
    ]
    respx.get("https://danepubliczne.imgw.pl/api/data/hydro/").mock(
        return_value=httpx.Response(200, json=data),
    )

    async with PolandImgwConnector() as conn:
        chunk = await conn.fetch_observations(
            "poland_imgw:150190330",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[0].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_no_river():
    """Stations with no river field get river=None."""
    data = [
        {
            "id_stacji": "150190360",
            "stacja": "SOME STATION",
            "rzeka": "",
            "stan_wody": "100",
            "stan_wody_data_pomiaru": "2024-06-01T10:00:00",
            "przepływ": "5.0",
            "data_pomiaru": "2024-06-01T10:00:00",
        },
    ]
    respx.get("https://danepubliczne.imgw.pl/api/data/hydro/").mock(
        return_value=httpx.Response(200, json=data),
    )

    async with PolandImgwConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].river is None


@pytest.mark.asyncio
@respx.mock
async def test_registry_slug():
    """Connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("poland_imgw")
    assert cls is PolandImgwConnector
