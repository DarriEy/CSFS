"""Tests for the Bulgaria NIMH connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.bulgaria_nimh import (
    _SEED_STATIONS,
    BulgariaNimhConnector,
)

BASE_URL = "https://info.meteo.bg"

MOCK_STATIONS_JSON = [
    {
        "id": "BG0001",
        "name": "Novo Selo",
        "lat": 44.17,
        "lon": 22.78,
        "river": "Timok",
    },
    {
        "id": "BG0007",
        "name": "Plovdiv",
        "lat": 42.15,
        "lon": 24.75,
        "river": "Maritsa",
    },
    {
        "id": "",
        "name": "Missing ID",
        "lat": 42.0,
        "lon": 25.0,
    },
    {
        "id": "BG9999",
        "name": "No Coords",
    },
]

MOCK_RUNOFF_CSV = """# Daily runoff data
2024-06-01,55.3
2024-06-02,60.1
2024-06-03,
2024-06-04,58.0
"""


@pytest.mark.asyncio
async def test_fetch_stations_returns_seed_list():
    """Seed stations are returned when API is unreachable."""
    async with BulgariaNimhConnector() as conn:
        # No mocked endpoints — will fall back to seed
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    native_ids = {s.native_id for s in stations}
    assert "6842200" in native_ids
    assert "6865100" in native_ids


@pytest.mark.asyncio
async def test_fetch_stations_seed_fields():
    """Seed stations have correct metadata."""
    async with BulgariaNimhConnector() as conn:
        stations = await conn.fetch_stations()

    plovdiv = next(s for s in stations if s.native_id == "6865100")
    assert plovdiv.id == "bulgaria_nimh:6865100"
    assert plovdiv.provider == "bulgaria_nimh"
    assert plovdiv.name == "Plovdiv"
    assert plovdiv.country_code == "BG"
    assert plovdiv.river == "Maritsa"
    assert plovdiv.latitude == pytest.approx(42.15)
    assert plovdiv.catchment_area_km2 is None


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_from_json_api():
    """Station list is parsed from JSON when API responds."""
    respx.get(f"{BASE_URL}/openData/").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_JSON),
    )

    async with BulgariaNimhConnector() as conn:
        stations = await conn.fetch_stations()

    # Only 2 valid entries (empty ID and no-coords are skipped)
    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"BG0001", "BG0007"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_from_html_directory():
    """Station list is parsed from HTML directory listing."""
    html = """
    <html><body>
    <a href="station_001.csv">station_001.csv</a>
    <a href="station_002.txt">station_002.txt</a>
    <a href="readme.html">readme.html</a>
    </body></html>
    """
    # JSON endpoint returns non-list (triggers fallback to HTML)
    respx.get(f"{BASE_URL}/openData/").mock(
        side_effect=[
            httpx.Response(200, json={"error": "not found"}),
            httpx.Response(200, text=html),
        ],
    )

    async with BulgariaNimhConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"station_001", "station_002"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_csv():
    """Runoff CSV data is correctly parsed with date filtering."""
    respx.get(f"{BASE_URL}/openData/BG0007.csv").mock(
        return_value=httpx.Response(200, text=MOCK_RUNOFF_CSV),
    )

    async with BulgariaNimhConnector() as conn:
        chunk = await conn.fetch_observations(
            "bulgaria_nimh:BG0007",
            start=datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 2, 23, 59, 59, tzinfo=UTC),
        )

    assert chunk.provider == "bulgaria_nimh"
    assert chunk.station_id == "bulgaria_nimh:BG0007"
    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(55.3)
    assert chunk.observations[1].discharge_m3s == pytest.approx(60.1)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_missing_values():
    """Empty values produce MISSING quality flag."""
    data = "2024-06-01,\n2024-06-02,nan\n2024-06-03,42.0\n"
    respx.get(f"{BASE_URL}/openData/BG0010.csv").mock(
        return_value=httpx.Response(200, text=data),
    )

    async with BulgariaNimhConnector() as conn:
        chunk = await conn.fetch_observations(
            "bulgaria_nimh:BG0010",
            start=datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 3, 23, 59, 59, tzinfo=UTC),
        )

    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[0].quality.value == "missing"
    assert chunk.observations[1].discharge_m3s is None
    assert chunk.observations[1].quality.value == "missing"
    assert chunk.observations[2].discharge_m3s == pytest.approx(42.0)
    assert chunk.observations[2].quality.value == "raw"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_returns_empty_on_failure():
    """Server errors return an empty chunk."""
    respx.get(f"{BASE_URL}/openData/BG0001.csv").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE_URL}/openData/BG0001.txt").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE_URL}/openData/BG0001.dat").mock(
        return_value=httpx.Response(500),
    )

    async with BulgariaNimhConnector() as conn:
        chunk = await conn.fetch_observations(
            "bulgaria_nimh:BG0001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0
    assert chunk.provider == "bulgaria_nimh"


def test_connector_is_registered():
    """The connector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("bulgaria_nimh")
    assert cls is BulgariaNimhConnector
