"""Tests for the LamaH-CE connector with file-based parsing and respx mocks."""

from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from csfs.connectors.lamah_ce import (
    _SEED_STATIONS,
    ZENODO_RECORD_ID,
    LamaHCEConnector,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_LAMAH_CSV = """\
date;qobs
1990-01-01;45.2
1990-01-02;47.8
1990-01-03;42.1
1990-01-04;nan
1990-01-05;50.3
"""

SAMPLE_LAMAH_CSV_COMMA = """\
date,qobs
1990-01-01,45.2
1990-01-02,47.8
"""

MOCK_ZENODO_RESPONSE = {
    "id": 5153305,
    "metadata": {
        "title": "LamaH-CE: Large-Sample Data for Hydrology",
        "doi": "10.5281/zenodo.5153305",
    },
}


# ---------------------------------------------------------------------------
# Station listing tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_stations_seed_default():
    """Default mode returns the curated seed catalogue."""
    async with LamaHCEConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    first = stations[0]
    assert first.provider == "lamah_ce"
    assert first.id.startswith("lamah_ce:")
    assert first.country_code in ("AT", "DE", "CZ")


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_with_zenodo_check():
    """Zenodo metadata check succeeds silently."""
    respx.get(
        f"https://zenodo.org/api/records/{ZENODO_RECORD_ID}"
    ).mock(
        return_value=httpx.Response(200, json=MOCK_ZENODO_RESPONSE),
    )

    async with LamaHCEConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_zenodo_failure_still_returns_seed():
    """If Zenodo check fails, seed list is still returned."""
    respx.get(
        f"https://zenodo.org/api/records/{ZENODO_RECORD_ID}"
    ).mock(return_value=httpx.Response(500))

    async with LamaHCEConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)


# ---------------------------------------------------------------------------
# Observation / file-parsing tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_no_data_dir():
    """Without data_dir configured, returns empty chunk with guidance."""
    async with LamaHCEConnector() as conn:
        chunk = await conn.fetch_observations(
            "lamah_ce:1",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 5, tzinfo=UTC),
        )

    assert chunk.station_id == "lamah_ce:1"
    assert chunk.provider == "lamah_ce"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_file_not_found(tmp_path: Path):
    """When data_dir exists but file is missing, returns empty chunk."""
    async with LamaHCEConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "lamah_ce:9999",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_parses_csv(tmp_path: Path):
    """Full parse of a LamaH CSV file with semicolon delimiter."""
    csv_file = tmp_path / "1.csv"
    csv_file.write_text(SAMPLE_LAMAH_CSV, encoding="utf-8")

    async with LamaHCEConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "lamah_ce:1",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 5, tzinfo=UTC),
        )

    assert chunk.station_id == "lamah_ce:1"
    assert chunk.provider == "lamah_ce"
    assert len(chunk.observations) == 5

    # First obs: valid discharge
    assert chunk.observations[0].discharge_m3s == pytest.approx(45.2)
    assert chunk.observations[0].quality.value == "raw"

    # Fourth obs: nan -> missing
    assert chunk.observations[3].discharge_m3s is None
    assert chunk.observations[3].quality.value == "missing"

    # Fifth obs: valid
    assert chunk.observations[4].discharge_m3s == pytest.approx(50.3)


@pytest.mark.asyncio
async def test_fetch_observations_date_filtering(tmp_path: Path):
    """Only observations within [start, end] are returned."""
    csv_file = tmp_path / "1.csv"
    csv_file.write_text(SAMPLE_LAMAH_CSV, encoding="utf-8")

    async with LamaHCEConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "lamah_ce:1",
            start=datetime(1990, 1, 2, tzinfo=UTC),
            end=datetime(1990, 1, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    dates = [obs.timestamp.day for obs in chunk.observations]
    assert dates == [2, 3]


@pytest.mark.asyncio
async def test_seed_station_ids_are_canonical():
    """Every seed station has a properly formatted CSFS station ID."""
    async with LamaHCEConnector() as conn:
        stations = await conn.fetch_stations()

    for station in stations:
        assert station.id == f"lamah_ce:{station.native_id}"
        assert station.provider == "lamah_ce"
        assert station.latitude != 0.0 or station.longitude != 0.0
