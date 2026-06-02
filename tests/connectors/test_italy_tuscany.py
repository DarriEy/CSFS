"""Tests for the italy_tuscany connector.

italy_tuscany is intentionally non-functional: SIR Toscana publishes hydrometric
LEVEL only (no open discharge/portata API), so the connector is kept registered
(status: research) but returns no data. These tests lock in that honest empty
behavior so it can't silently regress into a fake seed.
"""

from datetime import datetime

import pytest

from csfs.connectors.italy_tuscany import ItalyTuscanyConnector
from csfs.core.registry import get_connector


def test_connector_is_registered():
    assert get_connector("italy_tuscany") is ItalyTuscanyConnector


@pytest.mark.asyncio
async def test_fetch_stations_returns_empty():
    """Level-only upstream — must return no stations (no fake seed)."""
    async with ItalyTuscanyConnector() as conn:
        stations = await conn.fetch_stations()
    assert stations == []


@pytest.mark.asyncio
async def test_fetch_observations_returns_empty_chunk():
    """No discharge feed — returns an empty chunk without touching the network."""
    async with ItalyTuscanyConnector() as conn:
        chunk = await conn.fetch_observations(
            "italy_tuscany:TOS01000741",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )
    assert chunk.provider == "italy_tuscany"
    assert chunk.station_id == "italy_tuscany:TOS01000741"
    assert chunk.observations == []
