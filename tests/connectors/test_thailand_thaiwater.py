"""Tests for the ThaiWater (Thailand) connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.thailand_thaiwater import ThailandThaiWaterConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import QualityFlag

# -- Mock response data ------------------------------------------------

MOCK_WATERLEVEL_RESPONSE = {
    "data": [
        {
            "station": {
                "id": "WL001",
                "tele_station_name": {"en": "Chao Phraya at Nakhon Sawan"},
                "tele_station_lat": 15.7,
                "tele_station_long": 100.13,
            },
            "datetime": "2024-06-01T12:00:00",
            "discharge": 450.5,
        },
        {
            "station": {
                "id": "WL002",
                "tele_station_name": {"en": "Mun River at Ubon"},
                "tele_station_lat": 15.25,
                "tele_station_long": 104.85,
            },
            "datetime": "2024-06-01T12:00:00",
            "discharge": 123.8,
        },
    ],
}

MOCK_WATERLEVEL_FLAT_RESPONSE = [
    {
        "id": "WL001",
        "name": "Chao Phraya at Nakhon Sawan",
        "lat": 15.7,
        "lon": 100.13,
        "datetime": "2024-06-01T12:00:00",
        "discharge": 450.5,
    },
    {
        "id": "WL002",
        "name": "Mun River at Ubon",
        "lat": 15.25,
        "lon": 104.85,
        "datetime": "2024-06-01T12:00:00",
        "discharge": None,
    },
]

MOCK_WATERLEVEL_EMPTY = {"data": []}

BASE = "https://api-v3.thaiwater.net/api/v1"


# -- Tests: fetch_stations ---------------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_nested_response():
    """Stations are correctly parsed from the nested waterlevel_load response."""
    respx.get(f"{BASE}/thaiwater30/public/waterlevel_load").mock(
        return_value=httpx.Response(200, json=MOCK_WATERLEVEL_RESPONSE),
    )

    async with ThailandThaiWaterConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2

    cp = next(s for s in stations if s.native_id == "WL001")
    assert cp.id == "thailand_thaiwater:WL001"
    assert cp.provider == "thailand_thaiwater"
    assert cp.name == "Chao Phraya at Nakhon Sawan"
    assert cp.latitude == pytest.approx(15.7)
    assert cp.longitude == pytest.approx(100.13)
    assert cp.country_code == "TH"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_flat_response():
    """Stations are correctly parsed from a flat list response."""
    respx.get(f"{BASE}/thaiwater30/public/waterlevel_load").mock(
        return_value=httpx.Response(200, json=MOCK_WATERLEVEL_FLAT_RESPONSE),
    )

    async with ThailandThaiWaterConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    assert stations[0].name == "Chao Phraya at Nakhon Sawan"
    assert stations[1].name == "Mun River at Ubon"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty data array returns no stations."""
    respx.get(f"{BASE}/thaiwater30/public/waterlevel_load").mock(
        return_value=httpx.Response(200, json=MOCK_WATERLEVEL_EMPTY),
    )

    async with ThailandThaiWaterConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_deduplicates():
    """Duplicate station IDs are only included once."""
    duplicated = {
        "data": [
            {
                "station": {
                    "id": "WL001",
                    "tele_station_name": "Station A",
                    "tele_station_lat": 15.0,
                    "tele_station_long": 100.0,
                },
                "datetime": "2024-06-01T12:00:00",
                "discharge": 100.0,
            },
            {
                "station": {
                    "id": "WL001",
                    "tele_station_name": "Station A duplicate",
                    "tele_station_lat": 15.0,
                    "tele_station_long": 100.0,
                },
                "datetime": "2024-06-01T13:00:00",
                "discharge": 101.0,
            },
        ],
    }
    respx.get(f"{BASE}/thaiwater30/public/waterlevel_load").mock(
        return_value=httpx.Response(200, json=duplicated),
    )

    async with ThailandThaiWaterConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1


# -- Tests: fetch_observations ----------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_returns_latest_snapshot():
    """fetch_observations returns the latest reading for the given station."""
    respx.get(f"{BASE}/thaiwater30/public/waterlevel_load").mock(
        return_value=httpx.Response(200, json=MOCK_WATERLEVEL_RESPONSE),
    )

    async with ThailandThaiWaterConnector() as conn:
        chunk = await conn.fetch_observations(
            "thailand_thaiwater:WL001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "thailand_thaiwater"
    assert chunk.station_id == "thailand_thaiwater:WL001"
    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(450.5)
    assert chunk.observations[0].quality == QualityFlag.RAW


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_station_not_found_returns_empty():
    """When the station is not in the response, returns empty observations."""
    respx.get(f"{BASE}/thaiwater30/public/waterlevel_load").mock(
        return_value=httpx.Response(200, json=MOCK_WATERLEVEL_RESPONSE),
    )

    async with ThailandThaiWaterConnector() as conn:
        chunk = await conn.fetch_observations(
            "thailand_thaiwater:NONEXISTENT",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_null_discharge_is_missing():
    """When discharge is null, quality is MISSING."""
    respx.get(f"{BASE}/thaiwater30/public/waterlevel_load").mock(
        return_value=httpx.Response(200, json=MOCK_WATERLEVEL_FLAT_RESPONSE),
    )

    async with ThailandThaiWaterConnector() as conn:
        chunk = await conn.fetch_observations(
            "thailand_thaiwater:WL002",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[0].quality == QualityFlag.MISSING


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest_delegates():
    """fetch_latest returns the latest snapshot for the station."""
    respx.get(f"{BASE}/thaiwater30/public/waterlevel_load").mock(
        return_value=httpx.Response(200, json=MOCK_WATERLEVEL_RESPONSE),
    )

    async with ThailandThaiWaterConnector() as conn:
        chunk = await conn.fetch_latest("thailand_thaiwater:WL001")

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(450.5)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_waterlevel_unexpected_type_raises():
    """A non-list/non-dict response raises DataFormatError."""
    respx.get(f"{BASE}/thaiwater30/public/waterlevel_load").mock(
        return_value=httpx.Response(200, json="unexpected string"),
    )

    async with ThailandThaiWaterConnector() as conn:
        with pytest.raises(DataFormatError, match="Unexpected response type"):
            await conn.fetch_stations()


# -- Tests: registration -----------------------------------------------

def test_connector_is_registered():
    """The connector is registered with the expected slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("thailand_thaiwater")
    assert cls is ThailandThaiWaterConnector


def test_connector_class_attributes():
    """Class-level attributes match expectations."""
    assert ThailandThaiWaterConnector.slug == "thailand_thaiwater"
    assert ThailandThaiWaterConnector.country_codes == ["TH"]
    assert "thaiwater.net" in ThailandThaiWaterConnector.base_url
