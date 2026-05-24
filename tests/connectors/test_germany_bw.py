"""Tests for the LUBW Baden-Wuerttemberg connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.germany_bw import GermanyBWConnector

MOCK_STATIONS_JSON = [
    {
        "id": "BW001",
        "name": "Heidelberg / Neckar",
        "latitude": 49.4094,
        "longitude": 8.6942,
        "river": "Neckar",
    },
    {
        "id": "BW002",
        "name": "Plochingen / Fils",
        "latitude": 48.7117,
        "longitude": 9.4167,
        "river": "Fils",
    },
]

MOCK_VALUES_JSON = [
    {"timestamp": "2024-06-01T00:00:00", "value": 42.5},
    {"timestamp": "2024-06-01T01:00:00", "value": 43.1},
    {"timestamp": "2024-06-01T02:00:00", "value": None},
]


@respx.mock
async def test_fetch_stations():
    """Stations are fetched and parsed from JSON endpoint."""
    respx.get(
        "https://udo.lubw.baden-wuerttemberg.de/api/stations"
    ).mock(return_value=httpx.Response(200, json=MOCK_STATIONS_JSON))

    async with GermanyBWConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    s = next(s for s in stations if s.native_id == "BW001")
    assert s.id == "germany_bw:BW001"
    assert s.name == "Heidelberg / Neckar"
    assert s.river == "Neckar"
    assert s.country_code == "DE"


@respx.mock
async def test_fetch_stations_failure():
    """Returns empty list on HTTP failure."""
    respx.get(
        "https://udo.lubw.baden-wuerttemberg.de/api/stations"
    ).mock(return_value=httpx.Response(500))

    async with GermanyBWConnector() as conn:
        stations = await conn.fetch_stations()

    assert stations == []


@respx.mock
async def test_fetch_observations():
    """Observations are parsed correctly."""
    respx.get(
        "https://udo.lubw.baden-wuerttemberg.de/api/data"
    ).mock(return_value=httpx.Response(200, json=MOCK_VALUES_JSON))

    async with GermanyBWConnector() as conn:
        chunk = await conn.fetch_observations(
            "germany_bw:BW001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert chunk.provider == "germany_bw"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(42.5)
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@respx.mock
async def test_fetch_observations_failure():
    """Returns empty chunk on failure."""
    respx.get(
        "https://udo.lubw.baden-wuerttemberg.de/api/data"
    ).mock(return_value=httpx.Response(500))

    async with GermanyBWConnector() as conn:
        chunk = await conn.fetch_observations(
            "germany_bw:BW001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert chunk.observations == []


@respx.mock
async def test_fetch_stations_dict_wrapper():
    """Handles response wrapped in a dict."""
    respx.get(
        "https://udo.lubw.baden-wuerttemberg.de/api/stations"
    ).mock(
        return_value=httpx.Response(
            200, json={"stations": MOCK_STATIONS_JSON}
        )
    )

    async with GermanyBWConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@respx.mock
async def test_fetch_observations_empty():
    """Empty array returns zero observations."""
    respx.get(
        "https://udo.lubw.baden-wuerttemberg.de/api/data"
    ).mock(return_value=httpx.Response(200, json=[]))

    async with GermanyBWConnector() as conn:
        chunk = await conn.fetch_observations(
            "germany_bw:BW001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@respx.mock
async def test_connector_registration():
    """Connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("germany_bw")
    assert cls is GermanyBWConnector
