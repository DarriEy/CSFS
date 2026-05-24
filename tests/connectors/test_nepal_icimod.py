"""Tests for the Nepal ICIMOD RDS connector with mocked HTTP and CSV."""

from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from csfs.connectors.nepal_icimod import (
    _SEED_STATIONS,
    NepalICIMODConnector,
)

# ---------------------------------------------------------------------------
# Sample CSV data
# ---------------------------------------------------------------------------

SAMPLE_ICIMOD_CSV = """\
date,discharge,station
2024-06-01,1500.5,koshi_chatara
2024-06-02,1620.3,koshi_chatara
2024-06-03,1580.0,koshi_chatara
"""

SAMPLE_ICIMOD_CSV_NO_STATION = """\
date,discharge
2024-06-01,1500.5
2024-06-02,1620.3
"""

SAMPLE_ICIMOD_CSV_MISSING = """\
date,discharge,station
2024-06-01,1500.5,koshi_chatara
2024-06-02,NA,koshi_chatara
2024-06-03,,koshi_chatara
"""

MOCK_API_RESPONSE = {
    "data": [
        {
            "date": "2024-06-01",
            "discharge": 1500.5,
            "quality": "good",
        },
        {
            "date": "2024-06-02",
            "discharge": 1620.3,
            "quality": "good",
        },
        {
            "date": "2024-06-03",
            "discharge": None,
            "quality": "missing",
        },
    ]
}


# ---------------------------------------------------------------------------
# Station listing tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_stations_returns_seed_list():
    """Seed list returns all 14 curated stations."""
    async with NepalICIMODConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    assert len(stations) == 14


@pytest.mark.asyncio
async def test_fetch_stations_metadata():
    """Seed stations have correct metadata fields."""
    async with NepalICIMODConnector() as conn:
        stations = await conn.fetch_stations()

    chatara = next(
        s for s in stations
        if s.native_id == "koshi_chatara"
    )
    assert chatara.id == "nepal_icimod:koshi_chatara"
    assert chatara.provider == "nepal_icimod"
    assert chatara.name == "Chatara (Koshi)"
    assert chatara.latitude == pytest.approx(26.867)
    assert chatara.longitude == pytest.approx(87.157)
    assert chatara.country_code == "NP"
    assert chatara.river == "Koshi"
    assert chatara.catchment_area_km2 == pytest.approx(54100.0)


# ---------------------------------------------------------------------------
# API endpoint tests (respx mocks)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_api_endpoint():
    """ICIMOD API response is parsed correctly."""
    respx.get("https://rds.icimod.org/Home/DataDetail").mock(
        return_value=httpx.Response(
            200, json=MOCK_API_RESPONSE,
        ),
    )

    async with NepalICIMODConnector() as conn:
        chunk = await conn.fetch_observations(
            "nepal_icimod:koshi_chatara",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 4, tzinfo=UTC),
        )

    assert chunk.provider == "nepal_icimod"
    assert chunk.station_id == "nepal_icimod:koshi_chatara"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(
        1500.5,
    )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_api_fails_csv_fallback(
    tmp_path: Path,
):
    """Falls back to CSV when API endpoint fails."""
    respx.get("https://rds.icimod.org/Home/DataDetail").mock(
        return_value=httpx.Response(500),
    )

    csv_file = tmp_path / "nepal_discharge.csv"
    csv_file.write_text(SAMPLE_ICIMOD_CSV, encoding="utf-8")

    async with NepalICIMODConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "nepal_icimod:koshi_chatara",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(
        1500.5,
    )


# ---------------------------------------------------------------------------
# Local CSV tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_csv_parses_correctly(
    tmp_path: Path,
):
    """ICIMOD CSV with station column is parsed correctly."""
    csv_file = tmp_path / "nepal_discharge.csv"
    csv_file.write_text(SAMPLE_ICIMOD_CSV, encoding="utf-8")

    async with NepalICIMODConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "nepal_icimod:koshi_chatara",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 3, tzinfo=UTC),
        )

    assert chunk.station_id == "nepal_icimod:koshi_chatara"
    assert chunk.provider == "nepal_icimod"
    assert len(chunk.observations) == 3
    assert chunk.observations[1].discharge_m3s == pytest.approx(
        1620.3,
    )
    assert chunk.observations[0].quality.value == "raw"


@pytest.mark.asyncio
async def test_fetch_observations_csv_handles_missing(
    tmp_path: Path,
):
    """NA and empty values are treated as missing."""
    csv_file = tmp_path / "nepal_discharge.csv"
    csv_file.write_text(
        SAMPLE_ICIMOD_CSV_MISSING, encoding="utf-8",
    )

    async with NepalICIMODConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "nepal_icimod:koshi_chatara",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s is not None
    assert chunk.observations[1].discharge_m3s is None
    assert chunk.observations[1].quality.value == "missing"
    assert chunk.observations[2].discharge_m3s is None


@pytest.mark.asyncio
async def test_fetch_observations_no_data_dir():
    """Without data_dir and no web, returns empty chunk."""
    async with NepalICIMODConnector() as conn:
        chunk = await conn.fetch_observations(
            "nepal_icimod:koshi_chatara",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 3, tzinfo=UTC),
        )

    assert chunk.station_id == "nepal_icimod:koshi_chatara"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_date_filtering(
    tmp_path: Path,
):
    """Only observations within [start, end] are returned."""
    csv_file = tmp_path / "nepal_discharge.csv"
    csv_file.write_text(SAMPLE_ICIMOD_CSV, encoding="utf-8")

    async with NepalICIMODConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "nepal_icimod:koshi_chatara",
            start=datetime(2024, 6, 2, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].timestamp.day == 2


@pytest.mark.asyncio
async def test_connector_registration():
    """Connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("nepal_icimod")
    assert cls is NepalICIMODConnector


@pytest.mark.asyncio
async def test_seed_station_ids_are_canonical():
    """Every seed station has a properly formatted CSFS station ID."""
    async with NepalICIMODConnector() as conn:
        stations = await conn.fetch_stations()

    for station in stations:
        assert station.id == (
            f"nepal_icimod:{station.native_id}"
        )
        assert station.provider == "nepal_icimod"
        assert station.country_code == "NP"
