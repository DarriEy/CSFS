"""Tests for the Israel Caravan Extension connector."""

from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from csfs.connectors.israel_caravan import (
    _SEED_STATIONS,
    IsraelCaravanConnector,
)

# ------------------------------------------------------------------
# Mock data
# ------------------------------------------------------------------

MOCK_ZENODO_RECORD = {
    "id": 15003600,
    "metadata": {
        "title": "Caravan Extension - Israel",
    },
    "files": [
        {
            "key": "caravan_israel_timeseries.zip",
            "links": {
                "self": (
                    "https://zenodo.org/api/records/15003600"
                    "/files/caravan_israel_timeseries.zip/content"
                ),
            },
        },
        {
            "key": "caravan_israel_attributes.zip",
            "links": {
                "self": (
                    "https://zenodo.org/api/records/15003600"
                    "/files/caravan_israel_attributes.zip/content"
                ),
            },
        },
    ],
}

MOCK_ZENODO_EMPTY = {
    "id": 15003600,
    "metadata": {"title": "Empty"},
    "files": [],
}

SAMPLE_CSV = (
    "date,streamflow\n"
    "2010-01-01,8.5\n"
    "2010-01-02,9.2\n"
    "2010-01-03,7.8\n"
    "2010-01-04,10.1\n"
    "2010-01-05,12.3\n"
)

SAMPLE_CSV_ALT = (
    "date,discharge\n"
    "2015-06-01,3.2\n"
    "2015-06-02,2.8\n"
)


# ------------------------------------------------------------------
# Station listing tests
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_stations_seed_default():
    """Default mode returns the curated seed catalogue."""
    async with IsraelCaravanConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    first = stations[0]
    assert first.provider == "israel_caravan"
    assert first.id.startswith("israel_caravan:")
    assert first.country_code == "IL"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_zenodo_discovery():
    """When seed_only=False, connector queries Zenodo for metadata."""
    respx.get(
        "https://zenodo.org/api/records/15003600",
    ).mock(
        return_value=httpx.Response(
            200, json=MOCK_ZENODO_RECORD,
        ),
    )

    async with IsraelCaravanConnector(
        config={"seed_only": False},
    ) as conn:
        stations = await conn.fetch_stations()

    # Falls back to seed after verifying Zenodo record
    assert len(stations) == len(_SEED_STATIONS)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_zenodo_error_falls_back():
    """If Zenodo returns error, connector falls back to seed."""
    respx.get(
        "https://zenodo.org/api/records/15003600",
    ).mock(
        return_value=httpx.Response(500),
    )

    async with IsraelCaravanConnector(
        config={"seed_only": False},
    ) as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_zenodo_empty_files_falls_back():
    """DataFormatError when Zenodo record has no files."""
    respx.get(
        "https://zenodo.org/api/records/15003600",
    ).mock(
        return_value=httpx.Response(
            200, json=MOCK_ZENODO_EMPTY,
        ),
    )

    async with IsraelCaravanConnector(
        config={"seed_only": False},
    ) as conn:
        stations = await conn.fetch_stations()

    # DataFormatError triggers fallback to seed
    assert len(stations) == len(_SEED_STATIONS)


# ------------------------------------------------------------------
# Observation / file-parsing tests
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_no_data_dir():
    """Without data_dir, returns empty chunk."""
    async with IsraelCaravanConnector() as conn:
        chunk = await conn.fetch_observations(
            "israel_caravan:il_yarkon",
            start=datetime(2010, 1, 1, tzinfo=UTC),
            end=datetime(2010, 1, 5, tzinfo=UTC),
        )

    assert chunk.station_id == "israel_caravan:il_yarkon"
    assert chunk.provider == "israel_caravan"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_parses_csv(tmp_path: Path):
    """Parse Caravan-format CSV with date and streamflow columns."""
    csv_file = tmp_path / "il_yarkon.csv"
    csv_file.write_text(SAMPLE_CSV, encoding="utf-8")

    async with IsraelCaravanConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "israel_caravan:il_yarkon",
            start=datetime(2010, 1, 1, tzinfo=UTC),
            end=datetime(2010, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 5
    assert chunk.observations[0].discharge_m3s == pytest.approx(8.5)
    assert chunk.observations[4].discharge_m3s == pytest.approx(12.3)


@pytest.mark.asyncio
async def test_fetch_observations_nested_path(tmp_path: Path):
    """Connector finds CSV in timeseries/csv/ subdirectory."""
    nested_dir = tmp_path / "timeseries" / "csv"
    nested_dir.mkdir(parents=True)
    csv_file = nested_dir / "il_kishon.csv"
    csv_file.write_text(SAMPLE_CSV, encoding="utf-8")

    async with IsraelCaravanConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "israel_caravan:il_kishon",
            start=datetime(2010, 1, 1, tzinfo=UTC),
            end=datetime(2010, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 5


@pytest.mark.asyncio
async def test_fetch_observations_date_filtering(tmp_path: Path):
    """Only observations within [start, end] are returned."""
    csv_file = tmp_path / "il_yarkon.csv"
    csv_file.write_text(SAMPLE_CSV, encoding="utf-8")

    async with IsraelCaravanConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "israel_caravan:il_yarkon",
            start=datetime(2010, 1, 2, tzinfo=UTC),
            end=datetime(2010, 1, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    dates = [obs.timestamp.day for obs in chunk.observations]
    assert dates == [2, 3]


@pytest.mark.asyncio
async def test_seed_station_ids_are_canonical():
    """Every seed station has a properly formatted CSFS station ID."""
    async with IsraelCaravanConnector() as conn:
        stations = await conn.fetch_stations()

    for station in stations:
        assert station.id == f"israel_caravan:{station.native_id}"
        assert station.provider == "israel_caravan"
        assert station.country_code == "IL"
        assert station.latitude != 0.0 or station.longitude != 0.0


@pytest.mark.asyncio
async def test_connector_registration():
    """The connector is registered under the 'israel_caravan' slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("israel_caravan")
    assert cls is IsraelCaravanConnector
