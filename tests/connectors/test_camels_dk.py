"""Tests for the CAMELS-DK connector (offline Zenodo archive).

CAMELS-DK parses a per-station local CSV (``<native_id>.csv``) with columns
``date`` (YYYY-MM-DD) and ``discharge``/``q`` in m3/s. fetch_stations() returns
a small seed catalogue; fetch_observations() returns an empty chunk when no
``data_dir`` is configured.
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from csfs.connectors.camels_dk import CAMELSDKConnector

# Per-station CSV: date + discharge in m3/s
SAMPLE_CSV = (
    "date,discharge\n"
    "2020-01-01,12.34\n"
    "2020-01-02,15.50\n"
    "2020-01-03,18.20\n"
)


@pytest.mark.asyncio
async def test_fetch_stations_returns_seed_list():
    """fetch_stations() returns the seed catalogue for Denmark."""
    async with CAMELSDKConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) > 0
    for station in stations:
        assert station.provider == "camels_dk"
        assert station.id.startswith("camels_dk:")
        assert station.country_code == "DK"

    seed = stations[0]
    assert seed.native_id == "DK_21000040"
    assert seed.id == "camels_dk:DK_21000040"


@pytest.mark.asyncio
async def test_fetch_observations_no_data_dir():
    """Without data_dir, returns an empty chunk gracefully (no raise)."""
    async with CAMELSDKConnector() as conn:
        chunk = await conn.fetch_observations(
            "camels_dk:DK_21000040",
            start=datetime(2020, 1, 1, tzinfo=UTC),
            end=datetime(2020, 1, 31, tzinfo=UTC),
        )

    assert chunk.station_id == "camels_dk:DK_21000040"
    assert chunk.provider == "camels_dk"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_missing_file_empty(tmp_path: Path):
    """data_dir present but no station file -> empty chunk."""
    config = {"data_dir": str(tmp_path)}
    async with CAMELSDKConnector(config=config) as conn:
        chunk = await conn.fetch_observations(
            "camels_dk:DK_21000040",
            start=datetime(2020, 1, 1, tzinfo=UTC),
            end=datetime(2020, 1, 31, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_from_csv(tmp_path: Path):
    """Parse a per-station CSV with discharge in m3/s."""
    (tmp_path / "DK_21000040.csv").write_text(SAMPLE_CSV, encoding="utf-8")

    config = {"data_dir": str(tmp_path)}
    async with CAMELSDKConnector(config=config) as conn:
        chunk = await conn.fetch_observations(
            "camels_dk:DK_21000040",
            start=datetime(2020, 1, 1, tzinfo=UTC),
            end=datetime(2020, 1, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(12.34)
    assert chunk.observations[0].timestamp == datetime(2020, 1, 1, tzinfo=UTC)
    assert chunk.observations[0].quality.value == "raw"
    assert chunk.observations[2].discharge_m3s == pytest.approx(18.20)


@pytest.mark.asyncio
async def test_fetch_observations_date_filtering(tmp_path: Path):
    """Only observations within [start, end] are returned."""
    (tmp_path / "DK_21000040.csv").write_text(SAMPLE_CSV, encoding="utf-8")

    config = {"data_dir": str(tmp_path)}
    async with CAMELSDKConnector(config=config) as conn:
        chunk = await conn.fetch_observations(
            "camels_dk:DK_21000040",
            start=datetime(2020, 1, 2, tzinfo=UTC),
            end=datetime(2020, 1, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(15.50)


@pytest.mark.asyncio
async def test_fetch_observations_q_column_alias(tmp_path: Path):
    """The ``q`` column is accepted as a discharge alias."""
    (tmp_path / "DK_21000040.csv").write_text(
        "date,q\n2020-01-01,7.5\n", encoding="utf-8"
    )

    config = {"data_dir": str(tmp_path)}
    async with CAMELSDKConnector(config=config) as conn:
        chunk = await conn.fetch_observations(
            "camels_dk:DK_21000040",
            start=datetime(2020, 1, 1, tzinfo=UTC),
            end=datetime(2020, 1, 1, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(7.5)


def test_registration():
    """The slug resolves to the CAMELSDKConnector class via the registry."""
    from csfs.core.registry import get_connector

    assert get_connector("camels_dk") is CAMELSDKConnector
