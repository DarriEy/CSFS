"""Tests for the Jamaica WRA connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.jamaica_wra import JamaicaWRAConnector

MOCK_STATIONS_RESPONSE = [
    {
        "station_id": "JM-100",
        "name": "Rio Grande at Fellowship",
        "latitude": 18.1167,
        "longitude": -76.3333,
        "river": "Rio Grande",
        "catchment_area_km2": 350.0,
    },
    {
        "station_id": "JM-101",
        "name": "Black River at Maggotty",
        "latitude": 18.1667,
        "longitude": -77.75,
        "river": "Black River",
    },
    {
        "station_id": "",
        "name": "Missing ID",
        "latitude": 18.0,
        "longitude": -77.0,
    },
    {
        "station_id": "JM-102",
        "name": "No Coords Station",
    },
]

MOCK_OBSERVATIONS_RESPONSE = {
    "data": [
        {
            "timestamp": "2024-06-01T12:00:00",
            "discharge": 34.5,
            "quality": "good",
        },
        {
            "timestamp": "2024-06-01T12:15:00",
            "discharge": 36.1,
            "quality": "estimated",
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
    respx.get("https://www.wra.gov.jm/data/stations").mock(
        return_value=httpx.Response(
            200, json=MOCK_STATIONS_RESPONSE,
        ),
    )

    async with JamaicaWRAConnector() as conn:
        stations = await conn.fetch_stations()

    # Empty id and missing coords should be skipped
    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"JM-100", "JM-101"}

    st = next(s for s in stations if s.native_id == "JM-100")
    assert st.id == "jamaica_wra:JM-100"
    assert st.provider == "jamaica_wra"
    assert st.country_code == "JM"
    assert st.river == "Rio Grande"
    assert st.latitude == 18.1167
    assert st.longitude == -76.3333
    assert st.catchment_area_km2 == 350.0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_seed_fallback():
    """Seed list is used when the live endpoint fails."""
    respx.get("https://www.wra.gov.jm/data/stations").mock(
        return_value=httpx.Response(500),
    )

    async with JamaicaWRAConnector() as conn:
        stations = await conn.fetch_stations()

    # Should return the 20 seed stations
    assert len(stations) == 5
    rivers = {s.river for s in stations}
    assert "Rio Grande" in rivers
    assert "Black River" in rivers


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty station list returns no stations (live)."""
    respx.get("https://www.wra.gov.jm/data/stations").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with JamaicaWRAConnector() as conn:
        stations = await conn.fetch_stations()

    # Empty live response falls through to seed stations
    assert len(stations) == 5


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_wrapped_response():
    """Stations wrapped in a 'stations' key are parsed."""
    wrapped = {"stations": MOCK_STATIONS_RESPONSE[:2]}
    respx.get("https://www.wra.gov.jm/data/stations").mock(
        return_value=httpx.Response(200, json=wrapped),
    )

    async with JamaicaWRAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_json():
    """Observations are correctly parsed into a TimeSeriesChunk."""
    respx.get("https://www.wra.gov.jm/data/discharge").mock(
        return_value=httpx.Response(
            200, json=MOCK_OBSERVATIONS_RESPONSE,
        ),
    )

    async with JamaicaWRAConnector() as conn:
        chunk = await conn.fetch_observations(
            "jamaica_wra:JM-100",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "jamaica_wra"
    assert chunk.station_id == "jamaica_wra:JM-100"
    assert len(chunk.observations) == 3

    # First — quality=good
    assert chunk.observations[0].discharge_m3s == pytest.approx(34.5)
    assert chunk.observations[0].quality.value == "good"

    # Second — quality=estimated
    assert chunk.observations[1].discharge_m3s == pytest.approx(36.1)
    assert chunk.observations[1].quality.value == "estimated"

    # Third — None value = MISSING
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty_fallback():
    """Returns empty chunk when the API is unreachable."""
    respx.get("https://www.wra.gov.jm/data/discharge").mock(
        return_value=httpx.Response(500),
    )

    async with JamaicaWRAConnector() as conn:
        chunk = await conn.fetch_observations(
            "jamaica_wra:JM-001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "jamaica_wra"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_bare_list():
    """Observations returned as a bare list are handled."""
    bare_list = MOCK_OBSERVATIONS_RESPONSE["data"][:2]
    respx.get("https://www.wra.gov.jm/data/discharge").mock(
        return_value=httpx.Response(200, json=bare_list),
    )

    async with JamaicaWRAConnector() as conn:
        chunk = await conn.fetch_observations(
            "jamaica_wra:JM-100",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_strips_prefix():
    """The native_id is extracted correctly from the full station_id."""
    respx.get("https://www.wra.gov.jm/data/discharge").mock(
        return_value=httpx.Response(200, json={"data": []}),
    )

    async with JamaicaWRAConnector() as conn:
        chunk = await conn.fetch_observations(
            "jamaica_wra:JM-100",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    request = respx.calls.last.request
    assert "station=JM-100" in str(request.url)
    assert chunk.station_id == "jamaica_wra:JM-100"
