"""Tests for the germany_nrw connector.

germany_nrw is intentionally non-functional: North Rhine-Westphalia exposes no
open discharge (Abfluss, m3/s) API — only water level / precipitation. The
connector is kept registered (status: research) but returns no data. These tests
lock in that honest empty behavior so it can't silently regress into a fake seed.
"""

from datetime import datetime

import pytest

from csfs.connectors.germany_nrw import GermanyNRWConnector
from csfs.core.registry import get_connector


def test_connector_is_registered():
    assert get_connector("germany_nrw") is GermanyNRWConnector


@pytest.mark.asyncio
async def test_fetch_stations_returns_empty():
    """No open discharge catalogue — must return no stations (no fake seed)."""
    async with GermanyNRWConnector() as conn:
        stations = await conn.fetch_stations()
    assert stations == []


@pytest.mark.asyncio
async def test_fetch_observations_returns_empty_chunk():
    """No discharge feed — returns an empty chunk without touching the network."""
    async with GermanyNRWConnector() as conn:
        chunk = await conn.fetch_observations(
            "germany_nrw:27180400",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 31),
        )
    assert chunk.provider == "germany_nrw"
    assert chunk.station_id == "germany_nrw:27180400"
    assert chunk.observations == []
