"""Tests for the Lithuania meteo.lt connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.lithuania_meteo import LithuaniaMeteoConnector

BASE_URL = "https://api.meteo.lt"

# -- Station fixtures ------------------------------------------------------

MOCK_STATIONS = [
    {
        "code": "nemunas-kaunas",
        "name": "Nemunas ties Kaunu",
        "coordinates": {"latitude": 54.8985, "longitude": 23.8858},
        "waterBody": "Nemunas",
    },
    {
        "code": "neris-vilnius",
        "name": "Neris ties Vilniumi",
        "coordinates": {"latitude": 54.6872, "longitude": 25.2798},
        "waterBody": "Neris",
    },
]

MOCK_STATIONS_WITH_BAD_ENTRIES = [
    {
        "code": "nemunas-kaunas",
        "name": "Nemunas ties Kaunu",
        "coordinates": {"latitude": 54.8985, "longitude": 23.8858},
        "waterBody": "Nemunas",
    },
    {
        "code": "",
        "name": "Empty code",
        "coordinates": {"latitude": 55.0, "longitude": 24.0},
    },
    {
        "code": "no-coords",
        "name": "No Coords",
        "coordinates": {},
    },
    {
        "code": "neris-vilnius",
        "name": "Neris ties Vilniumi",
        "coordinates": {"latitude": 54.6872, "longitude": 25.2798},
        "waterBody": "Neris",
    },
]

# -- Observation fixtures --------------------------------------------------

MOCK_OBS_DAY1 = {
    "observations": [
        {
            "observationTimeUtc": "2024-06-01T06:00:00",
            "waterLevel": 152.3,
            "waterTemperature": 15.2,
        },
        {
            "observationTimeUtc": "2024-06-01T07:00:00",
            "waterLevel": 153.1,
            "waterTemperature": 15.4,
        },
    ],
}

MOCK_OBS_DAY2 = {
    "observations": [
        {
            "observationTimeUtc": "2024-06-02T06:00:00",
            "waterLevel": 150.0,
            "waterTemperature": 14.8,
        },
    ],
}

MOCK_OBS_WITH_NULL = {
    "observations": [
        {
            "observationTimeUtc": "2024-06-01T06:00:00",
            "waterLevel": None,
            "waterTemperature": None,
        },
    ],
}


# ======================================================================
# Station tests
# ======================================================================


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_json_array():
    """Station list is parsed from JSON array."""
    respx.get(f"{BASE_URL}/v1/hydro-stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS),
    )

    async with LithuaniaMeteoConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    codes = {s.native_id for s in stations}
    assert codes == {"nemunas-kaunas", "neris-vilnius"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fields_correct():
    """Station metadata is correctly extracted."""
    respx.get(f"{BASE_URL}/v1/hydro-stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS),
    )

    async with LithuaniaMeteoConnector() as conn:
        stations = await conn.fetch_stations()

    kaunas = next(
        s for s in stations if s.native_id == "nemunas-kaunas"
    )
    assert kaunas.id == "lithuania_meteo:nemunas-kaunas"
    assert kaunas.provider == "lithuania_meteo"
    assert kaunas.name == "Nemunas ties Kaunu"
    assert kaunas.country_code == "LT"
    assert kaunas.river == "Nemunas"
    assert kaunas.latitude == pytest.approx(54.8985)
    assert kaunas.longitude == pytest.approx(23.8858)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_invalid_entries():
    """Entries with empty code or missing coords are skipped."""
    respx.get(f"{BASE_URL}/v1/hydro-stations").mock(
        return_value=httpx.Response(
            200, json=MOCK_STATIONS_WITH_BAD_ENTRIES,
        ),
    )

    async with LithuaniaMeteoConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    codes = {s.native_id for s in stations}
    assert codes == {"nemunas-kaunas", "neris-vilnius"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty station list returns no stations."""
    respx.get(f"{BASE_URL}/v1/hydro-stations").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with LithuaniaMeteoConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


# ======================================================================
# Observation tests
# ======================================================================


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_single_day():
    """Observations for a single day are parsed correctly."""
    respx.get(
        f"{BASE_URL}/v1/hydro-stations/nemunas-kaunas"
        f"/observations/measured/2024-06-01",
    ).mock(
        return_value=httpx.Response(200, json=MOCK_OBS_DAY1),
    )

    async with LithuaniaMeteoConnector() as conn:
        chunk = await conn.fetch_observations(
            "lithuania_meteo:nemunas-kaunas",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 1),
        )

    assert chunk.provider == "lithuania_meteo"
    assert chunk.station_id == "lithuania_meteo:nemunas-kaunas"
    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(152.3)
    assert chunk.observations[0].quality.value == "raw"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_multi_day():
    """Observations spanning two days are fetched and combined."""
    respx.get(
        f"{BASE_URL}/v1/hydro-stations/nemunas-kaunas"
        f"/observations/measured/2024-06-01",
    ).mock(
        return_value=httpx.Response(200, json=MOCK_OBS_DAY1),
    )
    respx.get(
        f"{BASE_URL}/v1/hydro-stations/nemunas-kaunas"
        f"/observations/measured/2024-06-02",
    ).mock(
        return_value=httpx.Response(200, json=MOCK_OBS_DAY2),
    )

    async with LithuaniaMeteoConnector() as conn:
        chunk = await conn.fetch_observations(
            "lithuania_meteo:nemunas-kaunas",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 3


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_null_water_level():
    """A null water level results in MISSING quality flag."""
    respx.get(
        f"{BASE_URL}/v1/hydro-stations/nemunas-kaunas"
        f"/observations/measured/2024-06-01",
    ).mock(
        return_value=httpx.Response(200, json=MOCK_OBS_WITH_NULL),
    )

    async with LithuaniaMeteoConnector() as conn:
        chunk = await conn.fetch_observations(
            "lithuania_meteo:nemunas-kaunas",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 1),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[0].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_404_returns_empty():
    """A 404 for a specific day is treated as no data (not an error)."""
    respx.get(
        f"{BASE_URL}/v1/hydro-stations/nemunas-kaunas"
        f"/observations/measured/2024-06-01",
    ).mock(
        return_value=httpx.Response(404),
    )

    async with LithuaniaMeteoConnector() as conn:
        chunk = await conn.fetch_observations(
            "lithuania_meteo:nemunas-kaunas",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 1),
        )

    assert len(chunk.observations) == 0


# ======================================================================
# Registry
# ======================================================================


def test_connector_is_registered():
    """The connector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("lithuania_meteo")
    assert cls is LithuaniaMeteoConnector
