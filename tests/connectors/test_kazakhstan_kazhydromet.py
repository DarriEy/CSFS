"""Tests for the Kazakhstan Kazhydromet connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.kazakhstan_kazhydromet import (
    KazakhstanKazhydrometConnector,
)

MOCK_STATIONS_RESPONSE = [
    {
        "station_id": "KZ-100",
        "name": "Irtysh at Semey",
        "latitude": 50.4167,
        "longitude": 80.25,
        "river": "Irtysh",
        "catchment_area_km2": 175000.0,
        "elevation_m": 312.0,
    },
    {
        "station_id": "KZ-101",
        "name": "Ili at Kapchagay",
        "latitude": 43.8833,
        "longitude": 77.0667,
        "river": "Ili",
    },
    {
        "station_id": "",
        "name": "Missing ID",
        "latitude": 50.0,
        "longitude": 70.0,
    },
    {
        "station_id": "KZ-102",
        "name": "No Coords Station",
    },
]

MOCK_OBSERVATIONS_RESPONSE = {
    "data": [
        {
            "timestamp": "2024-06-01T12:00:00",
            "discharge": 1250.5,
            "quality": "good",
        },
        {
            "timestamp": "2024-06-01T12:15:00",
            "discharge": 1230.0,
        },
        {
            "timestamp": "2024-06-01T12:30:00",
            "discharge": None,
            "quality": None,
        },
    ],
}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_list():
    """Station list is parsed and invalid entries are skipped."""
    respx.get(
        "https://meteo.kazhydromet.kz/database_hydro/stations"
    ).mock(
        return_value=httpx.Response(
            200, json=MOCK_STATIONS_RESPONSE,
        ),
    )

    async with KazakhstanKazhydrometConnector() as conn:
        stations = await conn.fetch_stations()

    # Empty id and missing coords should be skipped
    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"KZ-100", "KZ-101"}

    st = next(s for s in stations if s.native_id == "KZ-100")
    assert st.id == "kazakhstan_kazhydromet:KZ-100"
    assert st.provider == "kazakhstan_kazhydromet"
    assert st.country_code == "KZ"
    assert st.river == "Irtysh"
    assert st.latitude == 50.4167
    assert st.longitude == 80.25
    assert st.catchment_area_km2 == 175000.0
    assert st.elevation_m == 312.0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_seed_fallback():
    """Seed list is used when all live endpoints fail."""
    respx.route().mock(return_value=httpx.Response(500))

    async with KazakhstanKazhydrometConnector() as conn:
        stations = await conn.fetch_stations()

    # Should return the 25 seed stations
    assert len(stations) == 25
    rivers = {s.river for s in stations}
    assert "Irtysh" in rivers
    assert "Ili" in rivers
    assert "Syr Darya" in rivers
    assert "Ural" in rivers


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_wrapped_response():
    """Stations wrapped in a 'stations' key are parsed."""
    wrapped = {"stations": MOCK_STATIONS_RESPONSE[:2]}
    respx.get(
        "https://meteo.kazhydromet.kz/database_hydro/stations"
    ).mock(
        return_value=httpx.Response(200, json=wrapped),
    )

    async with KazakhstanKazhydrometConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_second_api_path():
    """Falls back to the second API path when the first fails."""
    respx.get(
        "https://meteo.kazhydromet.kz/database_hydro/stations"
    ).mock(return_value=httpx.Response(404))
    respx.get(
        "https://meteo.kazhydromet.kz/api/hydro/stations"
    ).mock(
        return_value=httpx.Response(
            200, json=MOCK_STATIONS_RESPONSE[:2],
        ),
    )

    async with KazakhstanKazhydrometConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_json():
    """Observations are correctly parsed into a TimeSeriesChunk."""
    respx.get(
        "https://meteo.kazhydromet.kz/database_hydro/data"
    ).mock(
        return_value=httpx.Response(
            200, json=MOCK_OBSERVATIONS_RESPONSE,
        ),
    )

    async with KazakhstanKazhydrometConnector() as conn:
        chunk = await conn.fetch_observations(
            "kazakhstan_kazhydromet:KZ-100",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "kazakhstan_kazhydromet"
    assert chunk.station_id == "kazakhstan_kazhydromet:KZ-100"
    assert len(chunk.observations) == 3

    # First — quality=good
    obs0 = chunk.observations[0]
    assert obs0.discharge_m3s == pytest.approx(1250.5)
    assert obs0.quality.value == "good"

    # Second — no quality = RAW
    obs1 = chunk.observations[1]
    assert obs1.discharge_m3s == pytest.approx(1230.0)
    assert obs1.quality.value == "raw"

    # Third — None discharge = MISSING
    obs2 = chunk.observations[2]
    assert obs2.discharge_m3s is None
    assert obs2.quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty_fallback():
    """Returns empty chunk when all data endpoints are unreachable."""
    respx.route().mock(return_value=httpx.Response(500))

    async with KazakhstanKazhydrometConnector() as conn:
        chunk = await conn.fetch_observations(
            "kazakhstan_kazhydromet:KZ-001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "kazakhstan_kazhydromet"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_bare_list():
    """Observations returned as a bare list are handled."""
    bare_list = MOCK_OBSERVATIONS_RESPONSE["data"][:2]
    respx.get(
        "https://meteo.kazhydromet.kz/database_hydro/data"
    ).mock(return_value=httpx.Response(200, json=bare_list))

    async with KazakhstanKazhydrometConnector() as conn:
        chunk = await conn.fetch_observations(
            "kazakhstan_kazhydromet:KZ-100",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_strips_prefix():
    """The native_id is extracted correctly from the station_id."""
    respx.get(
        "https://meteo.kazhydromet.kz/database_hydro/data"
    ).mock(
        return_value=httpx.Response(200, json={"data": []}),
    )

    async with KazakhstanKazhydrometConnector() as conn:
        chunk = await conn.fetch_observations(
            "kazakhstan_kazhydromet:KZ-100",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    request = respx.calls.last.request
    assert "station=KZ-100" in str(request.url)
    assert chunk.station_id == "kazakhstan_kazhydromet:KZ-100"
