"""Tests for the Kazakhstan Kazhydromet connector."""

from datetime import datetime

import pytest

from csfs.connectors.kazakhstan_kazhydromet import (
    KazakhstanKazhydrometConnector,
)


@pytest.mark.asyncio
async def test_fetch_stations_returns_seed_list():
    """Seed list is always returned (portal is unreliable)."""
    async with KazakhstanKazhydrometConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 25
    rivers = {s.river for s in stations}
    assert "Irtysh" in rivers
    assert "Ili" in rivers
    assert "Syr Darya" in rivers
    assert "Ural" in rivers


@pytest.mark.asyncio
async def test_seed_station_fields():
    """Seed stations have correct field values."""
    async with KazakhstanKazhydrometConnector() as conn:
        stations = await conn.fetch_stations()

    st = stations[0]
    assert st.id.startswith("kazakhstan_kazhydromet:")
    assert st.provider == "kazakhstan_kazhydromet"
    assert st.country_code == "KZ"
    assert st.latitude != 0
    assert st.longitude != 0
    assert st.river is not None


@pytest.mark.asyncio
async def test_fetch_observations_returns_empty():
    """Observations return empty (portal unreliable, no live fetch)."""
    async with KazakhstanKazhydrometConnector() as conn:
        chunk = await conn.fetch_observations(
            "kazakhstan_kazhydromet:KZ-001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "kazakhstan_kazhydromet"
    assert chunk.station_id == "kazakhstan_kazhydromet:KZ-001"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_connector_registration():
    """Connector is discoverable via the registry."""
    from csfs.core.registry import discover, get_connector

    discover()
    cls = get_connector("kazakhstan_kazhydromet")
    assert cls is KazakhstanKazhydrometConnector
