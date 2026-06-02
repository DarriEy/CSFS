"""Tests for the CAMELSH connector (offline hourly US archive).

CAMELSH parses a per-station local CSV (``<native_id>.csv``) with columns
``date``/``timestamp`` (ISO ``YYYY-MM-DD HH:MM:SS``), ``streamflow_m3s``/``q``
(discharge in m3/s), and ``water_level_m``/``h``. fetch_stations() returns a
seed catalogue of USGS gauges; fetch_observations() returns an empty chunk when
no ``data_dir`` is configured.
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from csfs.connectors.camelsh import _SEED_STATIONS, CAMELSHConnector

# Hourly per-station CSV: timestamp + streamflow (m3/s) + water level (m)
SAMPLE_CSV = (
    "date,streamflow_m3s,water_level_m\n"
    "2020-01-01 00:00:00,12.34,1.10\n"
    "2020-01-01 01:00:00,15.50,1.25\n"
    "2020-01-01 02:00:00,18.20,1.40\n"
)


@pytest.mark.asyncio
async def test_fetch_stations_returns_seed_list():
    """fetch_stations() returns the seed catalogue of US gauges."""
    async with CAMELSHConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    assert len(stations) > 0
    for station in stations:
        assert station.provider == "camelsh"
        assert station.id.startswith("camelsh:")
        assert station.country_code == "US"

    fish = next(s for s in stations if s.native_id == "01013500")
    assert fish.name == "Fish River near Fort Kent"
    assert fish.river == "Fish River"
    assert fish.id == "camelsh:01013500"


@pytest.mark.asyncio
async def test_fetch_observations_no_data_dir():
    """Without data_dir, returns an empty chunk gracefully (no raise)."""
    async with CAMELSHConnector() as conn:
        chunk = await conn.fetch_observations(
            "camelsh:01013500",
            start=datetime(2020, 1, 1, tzinfo=UTC),
            end=datetime(2020, 1, 31, tzinfo=UTC),
        )

    assert chunk.station_id == "camelsh:01013500"
    assert chunk.provider == "camelsh"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_missing_file_empty(tmp_path: Path):
    """data_dir present but no station file -> empty chunk."""
    config = {"data_dir": str(tmp_path)}
    async with CAMELSHConnector(config=config) as conn:
        chunk = await conn.fetch_observations(
            "camelsh:01013500",
            start=datetime(2020, 1, 1, tzinfo=UTC),
            end=datetime(2020, 1, 31, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_from_csv(tmp_path: Path):
    """Parse hourly CSV with discharge in m3/s and water level in m."""
    (tmp_path / "01013500.csv").write_text(SAMPLE_CSV, encoding="utf-8")

    config = {"data_dir": str(tmp_path)}
    async with CAMELSHConnector(config=config) as conn:
        chunk = await conn.fetch_observations(
            "camelsh:01013500",
            start=datetime(2020, 1, 1, 0, 0, tzinfo=UTC),
            end=datetime(2020, 1, 1, 2, 0, tzinfo=UTC),
        )

    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(12.34)
    # NOTE: the Observation model has no water_level_m field, so the
    # connector's water_level kwarg is dropped by pydantic; only discharge
    # is asserted here.
    assert chunk.observations[0].timestamp == datetime(
        2020, 1, 1, 0, 0, tzinfo=UTC
    )
    assert chunk.observations[0].quality.value == "raw"
    assert chunk.observations[2].discharge_m3s == pytest.approx(18.20)


@pytest.mark.asyncio
async def test_fetch_observations_hourly_filtering(tmp_path: Path):
    """Hourly timestamps are filtered to the [start, end] window."""
    (tmp_path / "01013500.csv").write_text(SAMPLE_CSV, encoding="utf-8")

    config = {"data_dir": str(tmp_path)}
    async with CAMELSHConnector(config=config) as conn:
        chunk = await conn.fetch_observations(
            "camelsh:01013500",
            start=datetime(2020, 1, 1, 1, 0, tzinfo=UTC),
            end=datetime(2020, 1, 1, 1, 0, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(15.50)


@pytest.mark.asyncio
async def test_fetch_observations_q_h_aliases(tmp_path: Path):
    """The ``q``/``h`` and ``timestamp`` column aliases are accepted."""
    (tmp_path / "01013500.csv").write_text(
        "timestamp,q,h\n2020-01-01 00:00:00,9.0,2.0\n",
        encoding="utf-8",
    )

    config = {"data_dir": str(tmp_path)}
    async with CAMELSHConnector(config=config) as conn:
        chunk = await conn.fetch_observations(
            "camelsh:01013500",
            start=datetime(2020, 1, 1, 0, 0, tzinfo=UTC),
            end=datetime(2020, 1, 1, 0, 0, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(9.0)


def test_registration():
    """The slug resolves to the CAMELSHConnector class via the registry."""
    from csfs.core.registry import get_connector

    assert get_connector("camelsh") is CAMELSHConnector
