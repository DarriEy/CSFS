"""Tests for the Malaysia DID connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.malaysia_did import (
    _SEED_STATIONS,
    MalaysiaDIDConnector,
)

MOCK_STATIONS_RESPONSE = [
    {
        "station_id": "3527412",
        "station_name": "Temerloh",
        "latitude": 3.45,
        "longitude": 102.42,
        "river_name": "Pahang",
    },
    {
        "station_id": "5721442",
        "station_name": "Kuala Krai",
        "latitude": 5.53,
        "longitude": 102.20,
        "river_name": "Kelantan",
    },
]

MOCK_OBSERVATIONS_RESPONSE = {
    "data": [
        {"date": "2024-08-01", "flow_rate": 85.2},
        {"date": "2024-08-02", "flow_rate": 92.7},
        {"date": "2024-08-03", "flow_rate": None},
    ],
}

BASE = "https://publicinfobanjir.water.gov.my"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_primary_endpoint():
    """Stations are fetched from the primary endpoint."""
    respx.get(
        f"{BASE}/aras-air/data-paras-air/",
    ).mock(
        return_value=httpx.Response(
            200, json=MOCK_STATIONS_RESPONSE,
        ),
    )

    async with MalaysiaDIDConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    temerloh = next(
        s for s in stations if s.native_id == "3527412"
    )
    assert temerloh.id == "malaysia_did:3527412"
    assert temerloh.provider == "malaysia_did"
    assert temerloh.name == "Temerloh"
    assert temerloh.country_code == "MY"
    assert temerloh.river == "Pahang"
    assert temerloh.latitude == pytest.approx(3.45)
    assert temerloh.longitude == pytest.approx(102.42)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fallback_to_seed():
    """Falls back to seed list when all endpoints fail."""
    respx.get(
        f"{BASE}/aras-air/data-paras-air/",
    ).mock(return_value=httpx.Response(500))
    respx.get(f"{BASE}/api/stations").mock(
        return_value=httpx.Response(500),
    )

    async with MalaysiaDIDConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    pahang = next(
        s for s in stations if s.native_id == "3527412"
    )
    assert pahang.river == "Pahang"
    assert pahang.country_code == "MY"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fallback_endpoint():
    """Falls back to second endpoint when primary fails."""
    respx.get(
        f"{BASE}/aras-air/data-paras-air/",
    ).mock(return_value=httpx.Response(500))
    respx.get(f"{BASE}/api/stations").mock(
        return_value=httpx.Response(
            200, json=MOCK_STATIONS_RESPONSE,
        ),
    )

    async with MalaysiaDIDConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_primary_endpoint():
    """Flow rate observations are parsed correctly."""
    respx.get(
        f"{BASE}/cerapan/kadar-alir/data-kadar-alir/",
    ).mock(
        return_value=httpx.Response(
            200, json=MOCK_OBSERVATIONS_RESPONSE,
        ),
    )

    async with MalaysiaDIDConnector() as conn:
        chunk = await conn.fetch_observations(
            "malaysia_did:3527412",
            start=datetime(2024, 8, 1, tzinfo=UTC),
            end=datetime(2024, 8, 4, tzinfo=UTC),
        )

    assert chunk.provider == "malaysia_did"
    assert chunk.station_id == "malaysia_did:3527412"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(
        85.2,
    )
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_all_fail_returns_empty():
    """Returns empty chunk when all endpoints fail."""
    respx.get(
        f"{BASE}/cerapan/kadar-alir/data-kadar-alir/",
    ).mock(return_value=httpx.Response(500))
    respx.get(f"{BASE}/api/flow-rate").mock(
        return_value=httpx.Response(500),
    )

    async with MalaysiaDIDConnector() as conn:
        chunk = await conn.fetch_observations(
            "malaysia_did:3527412",
            start=datetime(2024, 8, 1, tzinfo=UTC),
            end=datetime(2024, 8, 3, tzinfo=UTC),
        )

    assert chunk.observations == []
    assert chunk.station_id == "malaysia_did:3527412"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_entries_without_id():
    """Entries without station_id are skipped."""
    data = [
        {"station_name": "No ID", "latitude": 3.0},
        {"station_id": "", "station_name": "Empty"},
        MOCK_STATIONS_RESPONSE[0],
    ]
    respx.get(
        f"{BASE}/aras-air/data-paras-air/",
    ).mock(
        return_value=httpx.Response(200, json=data),
    )

    async with MalaysiaDIDConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "3527412"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_malay_field_names():
    """Observations with Malay field names are parsed."""
    malay_data = [
        {
            "tarikh": "2024-08-01",
            "kadar_alir": 75.0,
        },
        {
            "tarikh": "2024-08-02",
            "kadar_alir": 80.5,
        },
    ]
    respx.get(
        f"{BASE}/cerapan/kadar-alir/data-kadar-alir/",
    ).mock(
        return_value=httpx.Response(
            200, json=malay_data,
        ),
    )

    async with MalaysiaDIDConnector() as conn:
        chunk = await conn.fetch_observations(
            "malaysia_did:5721442",
            start=datetime(2024, 8, 1, tzinfo=UTC),
            end=datetime(2024, 8, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(
        75.0,
    )
    assert chunk.observations[1].discharge_m3s == pytest.approx(
        80.5,
    )


@pytest.mark.asyncio
@respx.mock
async def test_connector_registration():
    """Connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("malaysia_did")
    assert cls is MalaysiaDIDConnector
