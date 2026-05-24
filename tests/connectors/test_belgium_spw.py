"""Tests for the Belgium SPW (Wallonia) connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.belgium_spw import BelgiumSPWConnector

MOCK_STATIONS_JSON = [
    {
        "code": "SPW001",
        "nom": "Namur / Meuse",
        "latitude": 50.4667,
        "longitude": 4.8667,
        "cours_eau": "Meuse",
    },
    {
        "code": "SPW002",
        "nom": "Liege / Ourthe",
        "latitude": 50.6333,
        "longitude": 5.5667,
        "cours_eau": "Ourthe",
    },
]

MOCK_DATA_JSON = {
    "data": [
        {"timestamp": "2024-06-01T00:00:00", "debit": 320.5},
        {"timestamp": "2024-06-01T01:00:00", "debit": 325.0},
        {"timestamp": "2024-06-01T02:00:00", "debit": None},
    ]
}


@respx.mock
async def test_fetch_stations():
    """Stations are fetched and parsed from SPW API."""
    respx.get("https://hydrometrie.wallonie.be/api/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_JSON)
    )

    async with BelgiumSPWConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    s = next(s for s in stations if s.native_id == "SPW001")
    assert s.id == "belgium_spw:SPW001"
    assert s.name == "Namur / Meuse"
    assert s.river == "Meuse"
    assert s.country_code == "BE"


@respx.mock
async def test_fetch_stations_failure():
    """Returns empty list on HTTP failure."""
    respx.get("https://hydrometrie.wallonie.be/api/stations").mock(
        return_value=httpx.Response(500)
    )

    async with BelgiumSPWConnector() as conn:
        stations = await conn.fetch_stations()

    assert stations == []


@respx.mock
async def test_fetch_observations():
    """Observations are parsed correctly."""
    respx.get("https://hydrometrie.wallonie.be/api/data").mock(
        return_value=httpx.Response(200, json=MOCK_DATA_JSON)
    )

    async with BelgiumSPWConnector() as conn:
        chunk = await conn.fetch_observations(
            "belgium_spw:SPW001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert chunk.provider == "belgium_spw"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(320.5)
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@respx.mock
async def test_fetch_observations_failure():
    """Returns empty chunk on failure."""
    respx.get("https://hydrometrie.wallonie.be/api/data").mock(
        return_value=httpx.Response(500)
    )

    async with BelgiumSPWConnector() as conn:
        chunk = await conn.fetch_observations(
            "belgium_spw:SPW001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert chunk.observations == []


@respx.mock
async def test_fetch_observations_empty():
    """Empty data returns zero observations."""
    respx.get("https://hydrometrie.wallonie.be/api/data").mock(
        return_value=httpx.Response(200, json={"data": []})
    )

    async with BelgiumSPWConnector() as conn:
        chunk = await conn.fetch_observations(
            "belgium_spw:SPW001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@respx.mock
async def test_fetch_stations_skips_missing_code():
    """Entries without code are skipped."""
    data = [
        {"nom": "No Code", "latitude": 50.0, "longitude": 4.0},
        {"code": "SPW099", "nom": "Valid", "latitude": 50.5, "longitude": 4.5},
    ]
    respx.get("https://hydrometrie.wallonie.be/api/stations").mock(
        return_value=httpx.Response(200, json=data)
    )

    async with BelgiumSPWConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "SPW099"


@respx.mock
async def test_connector_registration():
    """Connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("belgium_spw")
    assert cls is BelgiumSPWConnector
