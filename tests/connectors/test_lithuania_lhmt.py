"""Tests for the Lithuania LHMT hydrology connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.lithuania_lhmt import LithuaniaLHMTConnector
from csfs.core.exceptions import ConnectorError, DataFormatError

LHMT_BASE = "https://api.meteo.lt/v1"

MOCK_STATIONS_RESPONSE = [
    {
        "code": "nemunas-kaunas",
        "name": "Nemunas - Kaunas",
        "coordinates": {"latitude": 54.898, "longitude": 23.886},
        "waterBody": "Nemunas",
    },
    {
        "code": "neris-vilnius",
        "name": "Neris - Vilnius",
        "coordinates": {"latitude": 54.687, "longitude": 25.283},
        "waterBody": "Neris",
    },
    {
        "code": "minija-gargzdai",
        "name": "Minija - Gargzdai",
        "coordinates": {"latitude": 55.713, "longitude": 21.392},
        "waterBody": "Minija",
    },
]

MOCK_OBSERVATIONS_RESPONSE = {
    "observations": [
        {
            "observationTimeUtc": "2024-06-01T06:00:00",
            "waterLevel": 152.3,
            "waterTemperature": 14.5,
        },
        {
            "observationTimeUtc": "2024-06-01T12:00:00",
            "waterLevel": 153.1,
            "waterTemperature": 15.0,
        },
        {
            "observationTimeUtc": "2024-06-01T18:00:00",
            "waterLevel": None,
            "waterTemperature": 14.8,
        },
    ],
}

MOCK_EMPTY_OBSERVATIONS = {"observations": []}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_all():
    """All stations in the response are returned."""
    respx.get(f"{LHMT_BASE}/hydro/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with LithuaniaLHMTConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 3
    native_ids = {s.native_id for s in stations}
    assert native_ids == {"nemunas-kaunas", "neris-vilnius", "minija-gargzdai"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_field_mapping():
    """Station fields are correctly mapped from the LHMT response."""
    respx.get(f"{LHMT_BASE}/hydro/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with LithuaniaLHMTConnector() as conn:
        stations = await conn.fetch_stations()

    kaunas = next(s for s in stations if s.native_id == "nemunas-kaunas")
    assert kaunas.id == "lithuania_lhmt:nemunas-kaunas"
    assert kaunas.provider == "lithuania_lhmt"
    assert kaunas.name == "Nemunas - Kaunas"
    assert kaunas.latitude == pytest.approx(54.898)
    assert kaunas.longitude == pytest.approx(23.886)
    assert kaunas.country_code == "LT"
    assert kaunas.river == "Nemunas"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty station list returns no stations."""
    respx.get(f"{LHMT_BASE}/hydro/stations").mock(
        return_value=httpx.Response(200, json=[])
    )

    async with LithuaniaLHMTConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_missing_code():
    """Stations without a 'code' field are silently skipped."""
    data = [
        {"name": "No Code", "coordinates": {"latitude": 55.0, "longitude": 24.0}},
        {
            "code": "valid-station",
            "name": "Valid",
            "coordinates": {"latitude": 55.1, "longitude": 24.1},
            "waterBody": "SomeRiver",
        },
    ]
    respx.get(f"{LHMT_BASE}/hydro/stations").mock(
        return_value=httpx.Response(200, json=data)
    )

    async with LithuaniaLHMTConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "valid-station"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_json():
    """Observations are correctly parsed from the daily endpoint."""
    respx.get(
        f"{LHMT_BASE}/hydro/stations/nemunas-kaunas/observations/2024-06-01"
    ).mock(return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE))

    async with LithuaniaLHMTConnector() as conn:
        chunk = await conn.fetch_observations(
            "lithuania_lhmt:nemunas-kaunas",
            start=datetime(2024, 6, 1, 0, 0),
            end=datetime(2024, 6, 1, 23, 59),
        )

    assert chunk.station_id == "lithuania_lhmt:nemunas-kaunas"
    assert chunk.provider == "lithuania_lhmt"
    assert len(chunk.observations) == 3

    # First observation has a water level value
    assert chunk.observations[0].discharge_m3s == pytest.approx(152.3)
    assert chunk.observations[0].quality.value == "raw"

    # Third observation has None waterLevel => MISSING
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty_day():
    """An empty observations array returns zero observations."""
    respx.get(
        f"{LHMT_BASE}/hydro/stations/nemunas-kaunas/observations/2024-06-01"
    ).mock(return_value=httpx.Response(200, json=MOCK_EMPTY_OBSERVATIONS))

    async with LithuaniaLHMTConnector() as conn:
        chunk = await conn.fetch_observations(
            "lithuania_lhmt:nemunas-kaunas",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 1),
        )

    assert len(chunk.observations) == 0
    assert chunk.station_id == "lithuania_lhmt:nemunas-kaunas"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_invalid_timestamp():
    """Invalid timestamps raise DataFormatError."""
    data = {
        "observations": [
            {
                "observationTimeUtc": "not-a-date",
                "waterLevel": 100.0,
            },
        ],
    }
    respx.get(
        f"{LHMT_BASE}/hydro/stations/nemunas-kaunas/observations/2024-06-01"
    ).mock(return_value=httpx.Response(200, json=data))

    async with LithuaniaLHMTConnector() as conn:
        with pytest.raises(DataFormatError, match="Invalid timestamp"):
            await conn.fetch_observations(
                "lithuania_lhmt:nemunas-kaunas",
                start=datetime(2024, 6, 1),
                end=datetime(2024, 6, 1),
            )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_api_error_raises():
    """A server error on the stations endpoint raises ConnectorError."""
    respx.get(f"{LHMT_BASE}/hydro/stations").mock(
        return_value=httpx.Response(500)
    )

    async with LithuaniaLHMTConnector() as conn:
        with pytest.raises(ConnectorError, match="lithuania_lhmt"):
            await conn.fetch_stations()


def test_connector_is_registered():
    """The connector registers itself under the expected slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("lithuania_lhmt")
    assert cls is LithuaniaLHMTConnector


def test_connector_metadata():
    """Verify class-level attributes."""
    assert LithuaniaLHMTConnector.slug == "lithuania_lhmt"
    assert LithuaniaLHMTConnector.country_codes == ["LT"]
    assert "api.meteo.lt" in LithuaniaLHMTConnector.base_url
