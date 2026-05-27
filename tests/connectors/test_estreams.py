"""Tests for the EStreams (European Streamflow Dataset) connector."""

from datetime import datetime

import pytest

from csfs.connectors.estreams import EStreamsConnector


@pytest.mark.asyncio
async def test_fetch_stations_returns_seed_catalogue():
    """fetch_stations returns the curated seed list."""
    async with EStreamsConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 28
    country_codes = {s.country_code for s in stations}
    assert country_codes == {"LU", "AL", "ME", "MK"}


@pytest.mark.asyncio
async def test_fetch_stations_field_mapping():
    """Seed stations have correct provider and ID format."""
    async with EStreamsConnector() as conn:
        stations = await conn.fetch_stations()

    lu = next(s for s in stations if s.native_id == "LU_0001")
    assert lu.id == "estreams:LU_0001"
    assert lu.provider == "estreams"
    assert lu.name == "Esch-sur-Sûre"
    assert lu.country_code == "LU"
    assert lu.river == "Sûre"
    assert lu.latitude == pytest.approx(49.89)
    assert lu.longitude == pytest.approx(5.93)
    assert lu.catchment_area_km2 == pytest.approx(407.0)


@pytest.mark.asyncio
async def test_fetch_observations_returns_empty():
    """fetch_observations returns an empty TimeSeriesChunk (no raw Q)."""
    async with EStreamsConnector() as conn:
        chunk = await conn.fetch_observations(
            "estreams:LU_0001",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 12, 31),
        )

    assert chunk.station_id == "estreams:LU_0001"
    assert chunk.provider == "estreams"
    assert len(chunk.observations) == 0
    assert chunk.fetched_at is not None


@pytest.mark.asyncio
async def test_fetch_latest_returns_empty():
    """fetch_latest delegates to fetch_observations and returns empty."""
    async with EStreamsConnector() as conn:
        chunk = await conn.fetch_latest("estreams:LU_0001")

    assert chunk.station_id == "estreams:LU_0001"
    assert len(chunk.observations) == 0


def test_connector_registration():
    """The connector is registered under the 'estreams' slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("estreams")
    assert cls is EStreamsConnector


def test_connector_class_attributes():
    """Class-level attributes match expectations."""
    assert EStreamsConnector.slug == "estreams"
    assert EStreamsConnector.country_codes == ["LU", "AL", "ME", "MK"]
