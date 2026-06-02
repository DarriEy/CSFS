# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Tests for the Romania INHGA connector.

INHGA / "RoWater" exposes no open machine-readable discharge API (only PDF
bulletins and warnings on hidro.ro). The connector therefore stays in
``research`` status: it must remain registered/importable and degrade
gracefully -- ``fetch_stations`` returns an empty list and
``fetch_observations`` returns an empty chunk without raising, even if some
upstream HTTP were attempted and errored.
"""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.romania_inhga import RomaniaInhgaConnector
from csfs.core.registry import get_connector


def test_registration():
    """The slug resolves to the connector class via the registry."""
    assert get_connector("romania_inhga") is RomaniaInhgaConnector


def test_connector_metadata():
    assert RomaniaInhgaConnector.slug == "romania_inhga"
    assert RomaniaInhgaConnector.country_codes == ["RO"]
    assert RomaniaInhgaConnector.base_url.startswith("http")


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_returns_empty():
    """No open station catalogue exists -> documented empty result."""
    async with RomaniaInhgaConnector() as conn:
        stations = await conn.fetch_stations()

    assert stations == []


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_returns_empty_chunk():
    """fetch_observations is a graceful no-op returning an empty chunk."""
    station_id = "romania_inhga:BAZIAS"
    async with RomaniaInhgaConnector() as conn:
        chunk = await conn.fetch_observations(
            station_id,
            start=datetime(2026, 6, 1, tzinfo=UTC),
            end=datetime(2026, 6, 2, tzinfo=UTC),
        )

    assert chunk.station_id == station_id
    assert chunk.provider == "romania_inhga"
    assert chunk.observations == []


@pytest.mark.asyncio
@respx.mock
async def test_unreachable_upstream_does_not_raise():
    """Even if any HTTP were attempted and the host errored (e.g. the dead
    /date-operative path returning 404), the connector must not propagate an
    uncaught exception -- it stays graceful for the acquisition scheduler.

    We mock the documented portal host to fail; the current implementation does
    not call it, so this both guards future regressions and proves no real
    network is touched (conftest blocks DNS).
    """
    respx.get("https://www.hidro.ro/date-operative").mock(
        return_value=httpx.Response(404)
    )

    async with RomaniaInhgaConnector() as conn:
        stations = await conn.fetch_stations()
        chunk = await conn.fetch_observations(
            "romania_inhga:BAZIAS",
            start=datetime(2026, 6, 1, tzinfo=UTC),
            end=datetime(2026, 6, 2, tzinfo=UTC),
        )

    assert stations == []
    assert chunk.observations == []
