"""Tests for the Philippines PAGASA (GeoRiskPH) connector.

PAGASA / GeoRiskPH is a NON-DISCHARGE provider for CSFS purposes: every public
hydro endpoint publishes water level (m) and/or rainfall (mm), never discharge
(m3/s). The connector is therefore kept registered and importable but returns
empty results gracefully -- no fabricated seed stations, no synthetic flow.

These tests pin that contract. They are fully hermetic: no HTTP is issued
(fetch_stations / fetch_observations short-circuit to empty), so the autouse
network guard in tests/conftest.py is satisfied without any respx mocks.
"""

from datetime import UTC, datetime, timedelta

import pytest

from csfs.connectors.philippines_pagasa import PhilippinesPagasaConnector
from csfs.core.registry import discover, get_connector


def test_registered_under_slug():
    """The connector is discoverable via the registry under its slug."""
    discover()
    cls = get_connector("philippines_pagasa")
    assert cls is PhilippinesPagasaConnector
    assert cls.slug == "philippines_pagasa"
    assert cls.country_codes == ["PH"]


@pytest.mark.asyncio
async def test_fetch_stations_returns_empty():
    """No discharge stations exist -> empty list, no fabricated seed."""
    async with PhilippinesPagasaConnector() as conn:
        stations = await conn.fetch_stations()
    assert stations == []


@pytest.mark.asyncio
async def test_fetch_observations_returns_empty_chunk():
    """Observations are empty and carry no discharge values."""
    end = datetime.now(UTC)
    start = end - timedelta(days=7)
    async with PhilippinesPagasaConnector() as conn:
        chunk = await conn.fetch_observations(
            "philippines_pagasa:ANGATDAM", start, end
        )

    assert chunk.provider == "philippines_pagasa"
    assert chunk.station_id == "philippines_pagasa:ANGATDAM"
    assert chunk.observations == []
    # No discharge is ever fabricated.
    assert all(o.discharge_m3s is None for o in chunk.observations)
