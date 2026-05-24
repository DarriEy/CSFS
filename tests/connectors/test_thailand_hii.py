"""Tests for the Thailand HII connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.thailand_hii import ThailandHIIConnector

PRIMARY_BASE = "https://data.hii.or.th/api/v1"
FALLBACK_BASE = "https://api-v3.thaiwater.net/api/v1"

MOCK_STATIONS = [
    {
        "station_id": "TH001",
        "station_name": "Chao Phraya at Nakhon Sawan",
        "latitude": 15.70,
        "longitude": 100.12,
        "basin_name": "Chao Phraya",
        "river_name": "Chao Phraya",
    },
    {
        "station_id": "TH002",
        "station_name": "Mekong at Nong Khai",
        "latitude": 17.87,
        "longitude": 102.74,
        "basin_name": "Mekong",
        "river_name": "Mekong",
    },
]

MOCK_OBSERVATIONS = [
    {"datetime": "2024-06-01T06:00:00", "value": 245.3},
    {"datetime": "2024-06-01T12:00:00", "value": 260.1},
    {"datetime": "2024-06-01T18:00:00", "value": None},
]


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_primary():
    """Stations are fetched from the primary HII endpoint."""
    respx.get(f"{PRIMARY_BASE}/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS),
    )

    async with ThailandHIIConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"TH001", "TH002"}

    st = next(s for s in stations if s.native_id == "TH001")
    assert st.id == "thailand_hii:TH001"
    assert st.provider == "thailand_hii"
    assert st.country_code == "TH"
    assert st.river == "Chao Phraya"
    assert st.latitude == pytest.approx(15.70)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fallback():
    """When primary fails, fallback ThaiWater endpoint is used."""
    respx.get(f"{PRIMARY_BASE}/stations").mock(
        return_value=httpx.Response(500, text="Server Error"),
    )
    respx.get(f"{FALLBACK_BASE}/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS),
    )

    async with ThailandHIIConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_both_fail():
    """When both endpoints fail, an empty list is returned."""
    respx.get(f"{PRIMARY_BASE}/stations").mock(
        return_value=httpx.Response(500, text="Error"),
    )
    respx.get(f"{FALLBACK_BASE}/stations").mock(
        return_value=httpx.Response(503, text="Unavailable"),
    )

    async with ThailandHIIConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_primary():
    """Observations are parsed from the primary endpoint."""
    respx.get(f"{PRIMARY_BASE}/data").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS),
    )

    async with ThailandHIIConnector() as conn:
        chunk = await conn.fetch_observations(
            "thailand_hii:TH001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert chunk.provider == "thailand_hii"
    assert chunk.station_id == "thailand_hii:TH001"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(245.3)
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_fallback():
    """When primary fails, fallback endpoint is used for observations."""
    respx.get(f"{PRIMARY_BASE}/data").mock(
        return_value=httpx.Response(500, text="Error"),
    )
    respx.get(f"{FALLBACK_BASE}/data").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS),
    )

    async with ThailandHIIConnector() as conn:
        chunk = await conn.fetch_observations(
            "thailand_hii:TH001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 3


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty():
    """An empty observations list returns zero observations."""
    respx.get(f"{PRIMARY_BASE}/data").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with ThailandHIIConnector() as conn:
        chunk = await conn.fetch_observations(
            "thailand_hii:TH001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_wrapped_response():
    """Stations wrapped in a 'data' key are parsed correctly."""
    wrapped = {"data": MOCK_STATIONS}
    respx.get(f"{PRIMARY_BASE}/stations").mock(
        return_value=httpx.Response(200, json=wrapped),
    )

    async with ThailandHIIConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


def test_connector_is_registered():
    """The connector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("thailand_hii")
    assert cls is ThailandHIIConnector
