"""Tests for the Hungary OVF connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.hungary_ovf import HungaryOVFConnector

PRIMARY_BASE = "https://www.hydroinfo.hu"
FALLBACK_BASE = "https://data.vizugy.hu/api"

MOCK_STATIONS = [
    {
        "allomas_id": "HU001",
        "nev": "Budapest - Vigado ter",
        "szelesseg": 47.49,
        "hosszusag": 19.05,
        "vizfolyas": "Duna",
    },
    {
        "allomas_id": "HU002",
        "nev": "Szolnok",
        "szelesseg": 47.17,
        "hosszusag": 20.19,
        "vizfolyas": "Tisza",
    },
]

MOCK_OBSERVATIONS = [
    {"datum": "2024-06-01T06:00:00", "ertek": 1250.0},
    {"datum": "2024-06-01T12:00:00", "ertek": 1310.5},
    {"datum": "2024-06-01T18:00:00", "ertek": None},
]


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_primary():
    """Stations are fetched from the primary hydroinfo.hu endpoint."""
    respx.get(f"{PRIMARY_BASE}/api/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS),
    )

    async with HungaryOVFConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"HU001", "HU002"}

    bp = next(s for s in stations if s.native_id == "HU001")
    assert bp.id == "hungary_ovf:HU001"
    assert bp.provider == "hungary_ovf"
    assert bp.country_code == "HU"
    assert bp.river == "Duna"
    assert bp.latitude == pytest.approx(47.49)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fallback():
    """When primary fails, fallback data.vizugy.hu is used."""
    respx.get(f"{PRIMARY_BASE}/api/stations").mock(
        return_value=httpx.Response(500, text="Server Error"),
    )
    respx.get(f"{FALLBACK_BASE}/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS),
    )

    async with HungaryOVFConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_both_fail():
    """When both endpoints fail, an empty list is returned."""
    respx.get(f"{PRIMARY_BASE}/api/stations").mock(
        return_value=httpx.Response(500, text="Error"),
    )
    respx.get(f"{FALLBACK_BASE}/stations").mock(
        return_value=httpx.Response(503, text="Unavailable"),
    )

    async with HungaryOVFConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_primary():
    """Observations are parsed from the primary endpoint."""
    respx.get(f"{PRIMARY_BASE}/api/data").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS),
    )

    async with HungaryOVFConnector() as conn:
        chunk = await conn.fetch_observations(
            "hungary_ovf:HU001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert chunk.provider == "hungary_ovf"
    assert chunk.station_id == "hungary_ovf:HU001"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(1250.0)
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_fallback():
    """When primary fails, fallback endpoint is used for observations."""
    respx.get(f"{PRIMARY_BASE}/api/data").mock(
        return_value=httpx.Response(500, text="Error"),
    )
    respx.get(f"{FALLBACK_BASE}/data").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS),
    )

    async with HungaryOVFConnector() as conn:
        chunk = await conn.fetch_observations(
            "hungary_ovf:HU001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 3


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty():
    """An empty observations list returns zero observations."""
    respx.get(f"{PRIMARY_BASE}/api/data").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with HungaryOVFConnector() as conn:
        chunk = await conn.fetch_observations(
            "hungary_ovf:HU001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_both_fail():
    """When both endpoints fail, an empty chunk is returned."""
    respx.get(f"{PRIMARY_BASE}/api/data").mock(
        return_value=httpx.Response(500, text="Error"),
    )
    respx.get(f"{FALLBACK_BASE}/data").mock(
        return_value=httpx.Response(503, text="Unavailable"),
    )

    async with HungaryOVFConnector() as conn:
        chunk = await conn.fetch_observations(
            "hungary_ovf:HU001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0
    assert chunk.provider == "hungary_ovf"


def test_connector_is_registered():
    """The connector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("hungary_ovf")
    assert cls is HungaryOVFConnector
