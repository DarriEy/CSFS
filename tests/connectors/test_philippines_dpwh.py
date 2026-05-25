"""Tests for the Philippines DPWH connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.philippines_dpwh import (
    _SEED_STATIONS,
    PhilippinesDPWHConnector,
)

MOCK_STATIONS_RESPONSE = [
    {
        "station_id": "PH-CAG-001",
        "station_name": "Aparri",
        "latitude": 18.36,
        "longitude": 121.63,
        "river_name": "Cagayan",
        "region": "Region II",
    },
    {
        "station_id": "PH-AGN-001",
        "station_name": "Bayambang",
        "latitude": 15.81,
        "longitude": 120.45,
        "river_name": "Agno",
        "region": "Region I",
    },
]

MOCK_OBSERVATIONS_RESPONSE = {
    "observations": [
        {"date": "2024-07-01", "value": 250.0},
        {"date": "2024-07-02", "value": 310.5},
        {"date": "2024-07-03", "value": None},
    ],
}

BASE = "https://apps.dpwh.gov.ph/streams_public"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_primary_endpoint():
    """Stations are fetched from the primary endpoint."""
    respx.get(f"{BASE}/station_public.aspx").mock(
        return_value=httpx.Response(
            200, json=MOCK_STATIONS_RESPONSE,
        ),
    )

    async with PhilippinesDPWHConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    aparri = next(
        s for s in stations
        if s.native_id == "PH-CAG-001"
    )
    assert aparri.id == "philippines_dpwh:PH-CAG-001"
    assert aparri.provider == "philippines_dpwh"
    assert aparri.name == "Aparri"
    assert aparri.country_code == "PH"
    assert aparri.river == "Cagayan"
    assert aparri.latitude == pytest.approx(18.36)
    assert aparri.longitude == pytest.approx(121.63)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fallback_to_seed():
    """Falls back to seed list when all endpoints fail."""
    respx.get(f"{BASE}/station_public.aspx").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE}/api/stations").mock(
        return_value=httpx.Response(500),
    )

    async with PhilippinesDPWHConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    cagayan = next(
        s for s in stations
        if s.native_id == "5654300"
    )
    assert cagayan.river == "Cagayan River"
    assert cagayan.country_code == "PH"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fallback_endpoint():
    """Falls back to second endpoint when primary fails."""
    respx.get(f"{BASE}/station_public.aspx").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE}/api/stations").mock(
        return_value=httpx.Response(
            200, json=MOCK_STATIONS_RESPONSE,
        ),
    )

    async with PhilippinesDPWHConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_primary_endpoint():
    """Observations are parsed from the primary endpoint."""
    respx.get(f"{BASE}/station_summary.aspx").mock(
        return_value=httpx.Response(
            200, json=MOCK_OBSERVATIONS_RESPONSE,
        ),
    )

    async with PhilippinesDPWHConnector() as conn:
        chunk = await conn.fetch_observations(
            "philippines_dpwh:PH-CAG-001",
            start=datetime(2024, 7, 1, tzinfo=UTC),
            end=datetime(2024, 7, 4, tzinfo=UTC),
        )

    assert chunk.provider == "philippines_dpwh"
    assert chunk.station_id == "philippines_dpwh:PH-CAG-001"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(
        250.0,
    )
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_all_fail_returns_empty():
    """Returns empty chunk when all endpoints fail."""
    respx.get(f"{BASE}/station_summary.aspx").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE}/api/observations").mock(
        return_value=httpx.Response(500),
    )

    async with PhilippinesDPWHConnector() as conn:
        chunk = await conn.fetch_observations(
            "philippines_dpwh:PH-CAG-001",
            start=datetime(2024, 7, 1, tzinfo=UTC),
            end=datetime(2024, 7, 3, tzinfo=UTC),
        )

    assert chunk.observations == []
    assert chunk.station_id == "philippines_dpwh:PH-CAG-001"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_entries_without_id():
    """Entries without station_id are skipped."""
    data = [
        {"station_name": "No ID", "latitude": 14.0},
        {"station_id": "", "station_name": "Empty"},
        MOCK_STATIONS_RESPONSE[0],
    ]
    respx.get(f"{BASE}/station_public.aspx").mock(
        return_value=httpx.Response(200, json=data),
    )

    async with PhilippinesDPWHConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "PH-CAG-001"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_list_format():
    """Observations as a bare list (not wrapped in dict)."""
    bare_list = [
        {"date": "2024-07-01", "value": 180.0},
        {"date": "2024-07-02", "value": 200.5},
    ]
    respx.get(f"{BASE}/station_summary.aspx").mock(
        return_value=httpx.Response(200, json=bare_list),
    )

    async with PhilippinesDPWHConnector() as conn:
        chunk = await conn.fetch_observations(
            "philippines_dpwh:PH-AGN-001",
            start=datetime(2024, 7, 1, tzinfo=UTC),
            end=datetime(2024, 7, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[1].discharge_m3s == pytest.approx(
        200.5,
    )


@pytest.mark.asyncio
@respx.mock
async def test_connector_registration():
    """Connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("philippines_dpwh")
    assert cls is PhilippinesDPWHConnector
