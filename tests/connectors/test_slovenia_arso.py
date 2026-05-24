"""Tests for the Slovenia ARSO connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.slovenia_arso import SloveniaArsoConnector

BASE_URL = "https://vode.arso.gov.si"

MOCK_STATIONS = [
    {
        "sifra": "3060",
        "ime": "Litija",
        "lat": 46.0600,
        "lon": 14.8300,
        "vodotok": "Sava",
    },
    {
        "sifra": "4200",
        "ime": "Celje",
        "lat": 46.2300,
        "lon": 15.2700,
        "vodotok": "Savinja",
    },
    {
        "sifra": "",
        "ime": "Missing Code",
        "lat": 46.0,
        "lon": 14.0,
    },
    {
        "sifra": "9999",
        "ime": "No Coords",
        "vodotok": "Drava",
    },
]

MOCK_OBSERVATIONS = [
    {"datum": "2024-06-01T06:00:00", "pretok": 55.0},
    {"datum": "2024-06-01T12:00:00", "pretok": 60.2},
    {"datum": "2024-06-01T18:00:00", "pretok": None},
    {"datum": "2024-06-02T06:00:00", "pretok": 58.0},
]


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_list():
    """Station list is parsed and invalid entries are skipped."""
    respx.get(f"{BASE_URL}/hidarhiv/pov_arhiv_tab.php").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS),
    )

    async with SloveniaArsoConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"3060", "4200"}

    litija = next(s for s in stations if s.native_id == "3060")
    assert litija.id == "slovenia_arso:3060"
    assert litija.provider == "slovenia_arso"
    assert litija.country_code == "SI"
    assert litija.river == "Sava"
    assert litija.latitude == pytest.approx(46.06)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty station list returns no stations."""
    respx.get(f"{BASE_URL}/hidarhiv/pov_arhiv_tab.php").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with SloveniaArsoConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_filters_by_date_range():
    """Only observations within the requested range are returned."""
    respx.get(f"{BASE_URL}/hidarhiv/pov_arhiv_data.php").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS),
    )

    async with SloveniaArsoConnector() as conn:
        chunk = await conn.fetch_observations(
            "slovenia_arso:3060",
            start=datetime(2024, 6, 1, 10, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 1, 20, 0, 0, tzinfo=UTC),
        )

    # Only 12:00 and 18:00 are within range
    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(60.2)
    assert chunk.observations[1].discharge_m3s is None


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_all_within_range():
    """All observations within the full date range are returned."""
    respx.get(f"{BASE_URL}/hidarhiv/pov_arhiv_data.php").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS),
    )

    async with SloveniaArsoConnector() as conn:
        chunk = await conn.fetch_observations(
            "slovenia_arso:3060",
            start=datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 2, 23, 59, 59, tzinfo=UTC),
        )

    assert chunk.provider == "slovenia_arso"
    assert chunk.station_id == "slovenia_arso:3060"
    assert len(chunk.observations) == 4


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty():
    """An empty observation list returns zero observations."""
    respx.get(f"{BASE_URL}/hidarhiv/pov_arhiv_data.php").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with SloveniaArsoConnector() as conn:
        chunk = await conn.fetch_observations(
            "slovenia_arso:3060",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_wrapped_response():
    """Observations wrapped in a 'podatki' key are parsed correctly."""
    wrapped = {"podatki": MOCK_OBSERVATIONS[:2]}
    respx.get(f"{BASE_URL}/hidarhiv/pov_arhiv_data.php").mock(
        return_value=httpx.Response(200, json=wrapped),
    )

    async with SloveniaArsoConnector() as conn:
        chunk = await conn.fetch_observations(
            "slovenia_arso:3060",
            start=datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 2, 0, 0, 0, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_missing_value():
    """None pretok values produce MISSING quality flag."""
    data = [{"datum": "2024-06-01T12:00:00", "pretok": None}]
    respx.get(f"{BASE_URL}/hidarhiv/pov_arhiv_data.php").mock(
        return_value=httpx.Response(200, json=data),
    )

    async with SloveniaArsoConnector() as conn:
        chunk = await conn.fetch_observations(
            "slovenia_arso:3060",
            start=datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 2, 0, 0, 0, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[0].quality.value == "missing"


def test_connector_is_registered():
    """The connector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("slovenia_arso")
    assert cls is SloveniaArsoConnector
