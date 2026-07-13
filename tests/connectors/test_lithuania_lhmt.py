"""Tests for the Lithuania meteo.lt connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.lithuania_lhmt import LithuaniaLhmtConnector
from csfs.core.models import Resolution, Variable

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

    async with LithuaniaLhmtConnector() as conn:
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

    async with LithuaniaLhmtConnector() as conn:
        stations = await conn.fetch_stations()

    kaunas = next(
        s for s in stations if s.native_id == "nemunas-kaunas"
    )
    assert kaunas.id == "lithuania_lhmt:nemunas-kaunas"
    assert kaunas.provider == "lithuania_lhmt"
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

    async with LithuaniaLhmtConnector() as conn:
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

    async with LithuaniaLhmtConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


# ======================================================================
# Observation tests
# ======================================================================


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_single_day():
    """Stage (cm -> m) and water temperature are emitted; no discharge."""
    respx.get(
        f"{BASE_URL}/v1/hydro-stations/nemunas-kaunas"
        f"/observations/measured/2024-06-01",
    ).mock(
        return_value=httpx.Response(200, json=MOCK_OBS_DAY1),
    )

    async with LithuaniaLhmtConnector() as conn:
        chunk = await conn.fetch_observations(
            "lithuania_lhmt:nemunas-kaunas",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 1),
        )

    assert chunk.provider == "lithuania_lhmt"
    assert chunk.station_id == "lithuania_lhmt:nemunas-kaunas"
    # 2 entries x (stage + water_temperature)
    assert len(chunk.observations) == 4

    # The API measures water level/temperature only -- never discharge.
    assert all(
        o.variable is not Variable.DISCHARGE for o in chunk.observations
    )
    assert all(
        o.resolution is Resolution.INSTANTANEOUS for o in chunk.observations
    )

    stage = chunk.observations[0]
    assert stage.variable is Variable.STAGE
    assert stage.value == pytest.approx(1.523)  # 152.3 cm -> 1.523 m
    assert stage.quality.value == "raw"

    temp = chunk.observations[1]
    assert temp.variable is Variable.WATER_TEMPERATURE
    assert temp.value == pytest.approx(15.2)  # degC, unconverted
    assert temp.quality.value == "raw"


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

    async with LithuaniaLhmtConnector() as conn:
        chunk = await conn.fetch_observations(
            "lithuania_lhmt:nemunas-kaunas",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    # 3 entries x (stage + water_temperature)
    assert len(chunk.observations) == 6


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_null_water_level():
    """Null level -> MISSING stage; null temperature emits nothing."""
    respx.get(
        f"{BASE_URL}/v1/hydro-stations/nemunas-kaunas"
        f"/observations/measured/2024-06-01",
    ).mock(
        return_value=httpx.Response(200, json=MOCK_OBS_WITH_NULL),
    )

    async with LithuaniaLhmtConnector() as conn:
        chunk = await conn.fetch_observations(
            "lithuania_lhmt:nemunas-kaunas",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 1),
        )

    assert len(chunk.observations) == 1
    obs = chunk.observations[0]
    assert obs.variable is Variable.STAGE
    assert obs.value is None
    assert obs.quality.value == "missing"


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

    async with LithuaniaLhmtConnector() as conn:
        chunk = await conn.fetch_observations(
            "lithuania_lhmt:nemunas-kaunas",
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

    cls = get_connector("lithuania_lhmt")
    assert cls is LithuaniaLhmtConnector
    assert cls.supported_variables == ("stage", "water_temperature")


# ======================================================================
# Additional coverage tests — error branches, edge cases
# ======================================================================


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_http_error_raises_connector_error():
    """HTTPStatusError on station listing raises ConnectorError (lines 66-67)."""
    from csfs.core.exceptions import ConnectorError

    respx.get(f"{BASE_URL}/v1/hydro-stations").mock(
        return_value=httpx.Response(500),
    )

    async with LithuaniaLhmtConnector() as conn:
        with pytest.raises(ConnectorError, match="Failed to fetch station list"):
            await conn.fetch_stations()


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_non_list_returns_empty():
    """Non-list response returns empty station list (line 75)."""
    respx.get(f"{BASE_URL}/v1/hydro-stations").mock(
        return_value=httpx.Response(200, json={"unexpected": "dict"}),
    )

    async with LithuaniaLhmtConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_day_non_404_error_raises():
    """Non-404 HTTPStatusError on day fetch raises ConnectorError (lines 147)."""
    from csfs.core.exceptions import ConnectorError

    respx.get(
        f"{BASE_URL}/v1/hydro-stations/nemunas-kaunas"
        f"/observations/measured/2024-06-01",
    ).mock(
        return_value=httpx.Response(500),
    )

    async with LithuaniaLhmtConnector() as conn:
        with pytest.raises(ConnectorError, match="Failed to fetch observations"):
            await conn.fetch_observations(
                "lithuania_lhmt:nemunas-kaunas",
                start=datetime(2024, 6, 1),
                end=datetime(2024, 6, 1),
            )


@pytest.mark.asyncio
@respx.mock
async def test_parse_stations_invalid_coords_skipped():
    """Stations with non-numeric coordinates are skipped (lines 183-190)."""
    bad_stations = [
        {
            "code": "bad-station",
            "name": "Bad Coords",
            "coordinates": {"latitude": "not_a_number", "longitude": "bad"},
        },
        {
            "code": "good-station",
            "name": "Good Station",
            "coordinates": {"latitude": 54.0, "longitude": 24.0},
            "waterBody": "Nemunas",
        },
    ]
    respx.get(f"{BASE_URL}/v1/hydro-stations").mock(
        return_value=httpx.Response(200, json=bad_stations),
    )

    async with LithuaniaLhmtConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "good-station"


@pytest.mark.asyncio
@respx.mock
async def test_parse_observations_missing_timestamp_skipped():
    """Observations with None observationTimeUtc are skipped (line 219)."""
    obs_data = {
        "observations": [
            {
                "observationTimeUtc": None,
                "waterLevel": 150.0,
            },
            {
                "observationTimeUtc": "2024-06-01T06:00:00",
                "waterLevel": 152.0,
            },
        ],
    }
    respx.get(
        f"{BASE_URL}/v1/hydro-stations/nemunas-kaunas"
        f"/observations/measured/2024-06-01",
    ).mock(
        return_value=httpx.Response(200, json=obs_data),
    )

    async with LithuaniaLhmtConnector() as conn:
        chunk = await conn.fetch_observations(
            "lithuania_lhmt:nemunas-kaunas",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 1),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].variable is Variable.STAGE
    assert chunk.observations[0].value == pytest.approx(1.52)


@pytest.mark.asyncio
@respx.mock
async def test_parse_observations_invalid_timestamp_skipped():
    """Observations with invalid timestamp are skipped (lines 223-229)."""
    obs_data = {
        "observations": [
            {
                "observationTimeUtc": "not-a-date",
                "waterLevel": 150.0,
            },
            {
                "observationTimeUtc": "2024-06-01T06:00:00",
                "waterLevel": 152.0,
            },
        ],
    }
    respx.get(
        f"{BASE_URL}/v1/hydro-stations/nemunas-kaunas"
        f"/observations/measured/2024-06-01",
    ).mock(
        return_value=httpx.Response(200, json=obs_data),
    )

    async with LithuaniaLhmtConnector() as conn:
        chunk = await conn.fetch_observations(
            "lithuania_lhmt:nemunas-kaunas",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 1),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].variable is Variable.STAGE
    assert chunk.observations[0].value == pytest.approx(1.52)
