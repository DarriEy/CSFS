"""Tests for the Romania INHGA connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.romania_inhga import RomaniaINHGAConnector

PRIMARY_BASE = "https://www.inhga.ro"
FALLBACK_BASE = "https://www.hidro.ro/api"

MOCK_STATIONS = [
    {
        "cod": "RO001",
        "denumire": "Bucuresti - Baneasa",
        "latitudine": 44.50,
        "longitudine": 26.08,
        "rau": "Colentina",
        "bazin": "Arges",
    },
    {
        "cod": "RO002",
        "denumire": "Orsova",
        "latitudine": 44.72,
        "longitudine": 22.41,
        "rau": "Dunarea",
        "bazin": "Dunarea",
    },
]

MOCK_OBSERVATIONS = [
    {"data": "2024-06-01T06:00:00", "valoare": 5200.0},
    {"data": "2024-06-01T12:00:00", "valoare": 5350.8},
    {"data": "2024-06-01T18:00:00", "valoare": None},
]


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_primary():
    """Stations are fetched from the primary inhga.ro endpoint."""
    respx.get(f"{PRIMARY_BASE}/api/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS),
    )

    async with RomaniaINHGAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"RO001", "RO002"}

    buc = next(s for s in stations if s.native_id == "RO001")
    assert buc.id == "romania_inhga:RO001"
    assert buc.provider == "romania_inhga"
    assert buc.country_code == "RO"
    assert buc.river == "Colentina"
    assert buc.latitude == pytest.approx(44.50)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fallback():
    """When primary fails, fallback hidro.ro is used."""
    respx.get(f"{PRIMARY_BASE}/api/stations").mock(
        return_value=httpx.Response(500, text="Server Error"),
    )
    respx.get(f"{FALLBACK_BASE}/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS),
    )

    async with RomaniaINHGAConnector() as conn:
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

    async with RomaniaINHGAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_primary():
    """Observations are parsed from the primary endpoint."""
    respx.get(f"{PRIMARY_BASE}/api/data").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS),
    )

    async with RomaniaINHGAConnector() as conn:
        chunk = await conn.fetch_observations(
            "romania_inhga:RO002",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert chunk.provider == "romania_inhga"
    assert chunk.station_id == "romania_inhga:RO002"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(5200.0)
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

    async with RomaniaINHGAConnector() as conn:
        chunk = await conn.fetch_observations(
            "romania_inhga:RO002",
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

    async with RomaniaINHGAConnector() as conn:
        chunk = await conn.fetch_observations(
            "romania_inhga:RO001",
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

    async with RomaniaINHGAConnector() as conn:
        chunk = await conn.fetch_observations(
            "romania_inhga:RO001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0
    assert chunk.provider == "romania_inhga"


def test_connector_is_registered():
    """The connector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("romania_inhga")
    assert cls is RomaniaINHGAConnector
