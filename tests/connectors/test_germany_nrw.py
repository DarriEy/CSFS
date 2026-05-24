"""Tests for the ELWAS NRW connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.germany_nrw import GermanyNRWConnector

MOCK_STATIONS_JSON = [
    {
        "id": "NRW001",
        "name": "Koeln / Rhein",
        "latitude": 50.9375,
        "longitude": 6.9603,
        "gewaesser": "Rhein",
    },
    {
        "id": "NRW002",
        "name": "Essen / Ruhr",
        "latitude": 51.4556,
        "longitude": 7.0116,
        "gewaesser": "Ruhr",
    },
]

MOCK_VALUES_JSON = {
    "values": [
        {"timestamp": "2024-06-01T06:00:00", "value": 1520.0},
        {"timestamp": "2024-06-01T07:00:00", "value": 1535.5},
    ]
}


@respx.mock
async def test_fetch_stations():
    """Stations are fetched and parsed."""
    respx.get("https://www.elwasweb.nrw.de/elwas/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_JSON)
    )

    async with GermanyNRWConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    s = next(s for s in stations if s.native_id == "NRW001")
    assert s.id == "germany_nrw:NRW001"
    assert s.name == "Koeln / Rhein"
    assert s.river == "Rhein"
    assert s.country_code == "DE"


@respx.mock
async def test_fetch_stations_failure():
    """Returns empty list on HTTP failure."""
    respx.get("https://www.elwasweb.nrw.de/elwas/stations").mock(
        return_value=httpx.Response(500)
    )

    async with GermanyNRWConnector() as conn:
        stations = await conn.fetch_stations()

    assert stations == []


@respx.mock
async def test_fetch_observations():
    """Observations are parsed correctly."""
    respx.get("https://www.elwasweb.nrw.de/elwas/data").mock(
        return_value=httpx.Response(200, json=MOCK_VALUES_JSON)
    )

    async with GermanyNRWConnector() as conn:
        chunk = await conn.fetch_observations(
            "germany_nrw:NRW001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert chunk.provider == "germany_nrw"
    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(1520.0)


@respx.mock
async def test_fetch_observations_failure():
    """Returns empty chunk on failure."""
    respx.get("https://www.elwasweb.nrw.de/elwas/data").mock(
        return_value=httpx.Response(500)
    )

    async with GermanyNRWConnector() as conn:
        chunk = await conn.fetch_observations(
            "germany_nrw:NRW001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert chunk.observations == []


@respx.mock
async def test_fetch_observations_empty_list():
    """Empty list returns zero observations."""
    respx.get("https://www.elwasweb.nrw.de/elwas/data").mock(
        return_value=httpx.Response(200, json=[])
    )

    async with GermanyNRWConnector() as conn:
        chunk = await conn.fetch_observations(
            "germany_nrw:NRW001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@respx.mock
async def test_fetch_stations_skips_missing_id():
    """Entries without id are skipped."""
    data = [
        {"name": "No ID", "latitude": 50.0, "longitude": 7.0},
        {"id": "NRW099", "name": "Valid", "latitude": 51.0, "longitude": 7.5},
    ]
    respx.get("https://www.elwasweb.nrw.de/elwas/stations").mock(
        return_value=httpx.Response(200, json=data)
    )

    async with GermanyNRWConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "NRW099"


@respx.mock
async def test_connector_registration():
    """Connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("germany_nrw")
    assert cls is GermanyNRWConnector
