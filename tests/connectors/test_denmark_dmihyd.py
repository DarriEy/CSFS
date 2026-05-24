"""Tests for the DMI Hydrological Data (Denmark) connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.denmark_dmihyd import DenmarkDmihydConnector, _map_quality
from csfs.core.exceptions import DataFormatError
from csfs.core.models import QualityFlag

# -- Mock response data ------------------------------------------------

MOCK_STATIONS_RESPONSE = [
    {
        "stationId": "21000042",
        "name": "Gudenaa ved Randers",
        "latitude": 56.461,
        "longitude": 10.037,
        "waterBodyName": "Gudenaa",
    },
    {
        "stationId": "25000180",
        "name": "Skjern Aa ved Loevenholm",
        "latitude": 55.972,
        "longitude": 8.534,
        "waterBodyName": "Skjern Aa",
    },
]

MOCK_OBSERVATIONS_RESPONSE = [
    {
        "observed": "2024-06-01T12:00:00+02:00",
        "value": 45.3,
        "quality": "approved",
    },
    {
        "observed": "2024-06-01T12:15:00+02:00",
        "value": 46.1,
        "quality": "raw",
    },
    {
        "observed": "2024-06-01T12:30:00+02:00",
        "value": None,
        "quality": "raw",
    },
]

BASE = "https://dmigw.govcloud.dk/v1/waterflow"


# -- Tests: _map_quality -----------------------------------------------

def test_map_quality_approved():
    assert _map_quality("approved") == QualityFlag.GOOD


def test_map_quality_controlled():
    assert _map_quality("controlled") == QualityFlag.GOOD


def test_map_quality_raw():
    assert _map_quality("raw") == QualityFlag.RAW


def test_map_quality_suspect():
    assert _map_quality("suspect") == QualityFlag.SUSPECT


def test_map_quality_none():
    assert _map_quality(None) == QualityFlag.RAW


def test_map_quality_unknown():
    assert _map_quality("unknown_value") == QualityFlag.RAW


# -- Tests: fetch_stations ---------------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_response():
    """Station list is correctly parsed from the DMI JSON response."""
    respx.get(f"{BASE}/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE),
    )

    async with DenmarkDmihydConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2

    gudenaa = next(s for s in stations if s.native_id == "21000042")
    assert gudenaa.id == "denmark_dmihyd:21000042"
    assert gudenaa.provider == "denmark_dmihyd"
    assert gudenaa.name == "Gudenaa ved Randers"
    assert gudenaa.latitude == pytest.approx(56.461)
    assert gudenaa.longitude == pytest.approx(10.037)
    assert gudenaa.country_code == "DK"
    assert gudenaa.river == "Gudenaa"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty station list returns no stations."""
    respx.get(f"{BASE}/stations").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with DenmarkDmihydConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_malformed_entries():
    """Entries without stationId are skipped."""
    response = [
        {
            "stationId": "21000042",
            "name": "Gudenaa ved Randers",
            "latitude": 56.461,
            "longitude": 10.037,
            "waterBodyName": "Gudenaa",
        },
        {
            # Missing stationId
            "name": "Bad Station",
            "latitude": 55.0,
            "longitude": 9.0,
        },
    ]
    respx.get(f"{BASE}/stations").mock(
        return_value=httpx.Response(200, json=response),
    )

    async with DenmarkDmihydConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "21000042"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_with_api_key():
    """API key is passed as query parameter when configured."""
    route = respx.get(f"{BASE}/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE),
    )

    async with DenmarkDmihydConnector(config={"api_key": "test-key-123"}) as conn:
        await conn.fetch_stations()

    assert route.calls.last.request.url.params["api_key"] == "test-key-123"


# -- Tests: fetch_observations ----------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_json():
    """Observations are correctly parsed into a TimeSeriesChunk."""
    respx.get(f"{BASE}/observations").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE),
    )

    async with DenmarkDmihydConnector() as conn:
        chunk = await conn.fetch_observations(
            "denmark_dmihyd:21000042",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "denmark_dmihyd"
    assert chunk.station_id == "denmark_dmihyd:21000042"
    assert len(chunk.observations) == 3

    # First observation — approved quality
    assert chunk.observations[0].discharge_m3s == pytest.approx(45.3)
    assert chunk.observations[0].quality == QualityFlag.GOOD

    # Second observation — raw quality
    assert chunk.observations[1].discharge_m3s == pytest.approx(46.1)
    assert chunk.observations[1].quality == QualityFlag.RAW

    # Third observation — None value should yield MISSING
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality == QualityFlag.MISSING


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty():
    """An empty observations array returns zero observations."""
    respx.get(f"{BASE}/observations").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with DenmarkDmihydConnector() as conn:
        chunk = await conn.fetch_observations(
            "denmark_dmihyd:21000042",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_invalid_timestamp_raises():
    """Invalid timestamp in observation data raises DataFormatError."""
    bad_response = [
        {"observed": "NOT-A-TIMESTAMP", "value": 100.0, "quality": "raw"},
    ]
    respx.get(f"{BASE}/observations").mock(
        return_value=httpx.Response(200, json=bad_response),
    )

    async with DenmarkDmihydConnector() as conn:
        with pytest.raises(DataFormatError, match="Invalid timestamp"):
            await conn.fetch_observations(
                "denmark_dmihyd:21000042",
                start=datetime(2024, 6, 1),
                end=datetime(2024, 6, 2),
            )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_with_api_key():
    """API key is passed when fetching observations."""
    route = respx.get(f"{BASE}/observations").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with DenmarkDmihydConnector(config={"api_key": "test-key-123"}) as conn:
        await conn.fetch_observations(
            "denmark_dmihyd:21000042",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert route.calls.last.request.url.params["api_key"] == "test-key-123"


# -- Tests: registration -----------------------------------------------

def test_connector_is_registered():
    """The connector is registered with the expected slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("denmark_dmihyd")
    assert cls is DenmarkDmihydConnector


def test_connector_class_attributes():
    """Class-level attributes match expectations."""
    assert DenmarkDmihydConnector.slug == "denmark_dmihyd"
    assert DenmarkDmihydConnector.country_codes == ["DK"]
    assert "dmigw.govcloud.dk" in DenmarkDmihydConnector.base_url
