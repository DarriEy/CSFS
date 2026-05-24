"""Tests for the Iceland LamaH-Ice connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.iceland_lamahice import IcelandLamahIceConnector

MOCK_LIVE_STATIONS = [
    {"id": "LIVE01", "name": "Skogafoss", "lat": 63.53, "lon": -19.51},
]

MOCK_OBSERVATIONS_JSON = {
    "data": [
        {"time": "2024-06-01T00:00:00", "discharge": 150.5},
        {"time": "2024-06-01T01:00:00", "discharge": 152.3},
        {"time": "2024-06-01T02:00:00", "discharge": None},
    ]
}


@respx.mock
async def test_fetch_stations_returns_seed_list():
    """Always returns the seed station list."""
    respx.get("https://api.vedur.is/hydro/stations.json").mock(
        return_value=httpx.Response(500)
    )

    async with IcelandLamahIceConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 30
    s = next(s for s in stations if s.native_id == "VHM001")
    assert s.id == "iceland_lamahice:VHM001"
    assert s.name == "Selfoss"
    assert s.river == "Olfusa"
    assert s.country_code == "IS"


@respx.mock
async def test_fetch_stations_augments_from_live():
    """Live stations are merged with the seed list."""
    respx.get("https://api.vedur.is/hydro/stations.json").mock(
        return_value=httpx.Response(200, json=MOCK_LIVE_STATIONS)
    )

    async with IcelandLamahIceConnector() as conn:
        stations = await conn.fetch_stations()

    # 30 seed + 1 live
    assert len(stations) == 31
    assert any(s.native_id == "LIVE01" for s in stations)


@respx.mock
async def test_fetch_observations():
    """Observations are parsed from Vedur.is API."""
    respx.get("https://api.vedur.is/hydro/latest.json").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_JSON)
    )

    async with IcelandLamahIceConnector() as conn:
        chunk = await conn.fetch_observations(
            "iceland_lamahice:VHM001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert chunk.provider == "iceland_lamahice"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(150.5)
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@respx.mock
async def test_fetch_observations_failure():
    """Returns empty chunk on API failure."""
    respx.get("https://api.vedur.is/hydro/latest.json").mock(
        return_value=httpx.Response(500)
    )

    async with IcelandLamahIceConnector() as conn:
        chunk = await conn.fetch_observations(
            "iceland_lamahice:VHM001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert chunk.observations == []


@respx.mock
async def test_fetch_observations_empty_data():
    """Empty data array returns zero observations."""
    respx.get("https://api.vedur.is/hydro/latest.json").mock(
        return_value=httpx.Response(200, json={"data": []})
    )

    async with IcelandLamahIceConnector() as conn:
        chunk = await conn.fetch_observations(
            "iceland_lamahice:VHM001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@respx.mock
async def test_connector_registration():
    """Connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("iceland_lamahice")
    assert cls is IcelandLamahIceConnector
