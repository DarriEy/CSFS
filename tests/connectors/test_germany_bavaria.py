"""Tests for the GKD Bayern connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.germany_bavaria import GermanyBavariaConnector

MOCK_STATIONS_JSON = [
    {
        "id": "16515009",
        "name": "Muenchen / Isar",
        "latitude": 48.1351,
        "longitude": 11.5820,
        "gewaesser": "Isar",
    },
    {
        "id": "16004509",
        "name": "Kempten / Iller",
        "latitude": 47.7333,
        "longitude": 10.3167,
        "gewaesser": "Iller",
    },
]

MOCK_VALUES_JSON = {
    "values": [
        {"timestamp": "2024-06-01T12:00:00", "value": 85.3},
        {"timestamp": "2024-06-01T13:00:00", "value": 87.1},
        {"timestamp": "2024-06-01T14:00:00", "value": None},
    ]
}


@respx.mock
async def test_fetch_stations_json():
    """Stations are fetched and parsed from JSON endpoint."""
    respx.get("https://www.gkd.bayern.de/gkd/abfluss/stations.json").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_JSON)
    )

    async with GermanyBavariaConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    s = next(s for s in stations if s.native_id == "16515009")
    assert s.id == "germany_bavaria:16515009"
    assert s.name == "Muenchen / Isar"
    assert s.river == "Isar"
    assert s.country_code == "DE"


@respx.mock
async def test_fetch_stations_html_fallback():
    """Falls back to HTML parsing when JSON fails."""
    respx.get("https://www.gkd.bayern.de/gkd/abfluss/stations.json").mock(
        return_value=httpx.Response(500)
    )
    html = """<html><body><table>
    <tr><td>16515009</td><td>Muenchen / Isar</td></tr>
    </table></body></html>"""
    respx.get("https://www.gkd.bayern.de/de/fluesse/abfluss/tabellen").mock(
        return_value=httpx.Response(200, text=html)
    )

    async with GermanyBavariaConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "16515009"


@respx.mock
async def test_fetch_stations_all_fail():
    """Returns empty list when all endpoints fail."""
    respx.get("https://www.gkd.bayern.de/gkd/abfluss/stations.json").mock(
        return_value=httpx.Response(500)
    )
    respx.get("https://www.gkd.bayern.de/de/fluesse/abfluss/tabellen").mock(
        return_value=httpx.Response(503)
    )

    async with GermanyBavariaConnector() as conn:
        stations = await conn.fetch_stations()

    assert stations == []


@respx.mock
async def test_fetch_observations_json():
    """Observations are parsed from JSON endpoint."""
    respx.get(
        "https://www.gkd.bayern.de/gkd/abfluss/16515009/values.json"
    ).mock(return_value=httpx.Response(200, json=MOCK_VALUES_JSON))

    async with GermanyBavariaConnector() as conn:
        chunk = await conn.fetch_observations(
            "germany_bavaria:16515009",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert chunk.provider == "germany_bavaria"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(85.3)
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@respx.mock
async def test_fetch_observations_failure_returns_empty():
    """Returns empty chunk on failure."""
    respx.get(
        "https://www.gkd.bayern.de/gkd/abfluss/16515009/values.json"
    ).mock(return_value=httpx.Response(500))

    async with GermanyBavariaConnector() as conn:
        chunk = await conn.fetch_observations(
            "germany_bavaria:16515009",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert chunk.observations == []


@respx.mock
async def test_fetch_stations_skips_entries_without_id():
    """Entries missing id are skipped."""
    data = [
        {"name": "No ID", "latitude": 48.0, "longitude": 11.0},
        {"id": "99999", "name": "Good", "latitude": 48.1, "longitude": 11.5},
    ]
    respx.get("https://www.gkd.bayern.de/gkd/abfluss/stations.json").mock(
        return_value=httpx.Response(200, json=data)
    )

    async with GermanyBavariaConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "99999"


@respx.mock
async def test_connector_registration():
    """Connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("germany_bavaria")
    assert cls is GermanyBavariaConnector
