"""Tests for the Vietnam Mekong Delta EIDC connector with mocked HTTP and CSV."""

from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from csfs.connectors.vietnam_mekong import (
    _SEED_STATIONS,
    EIDC_CATALOGUE_DOC,
    VietnamMekongConnector,
)

# ---------------------------------------------------------------------------
# Sample CSV data
# ---------------------------------------------------------------------------

SAMPLE_EIDC_CSV = """\
datetime,discharge_m3s,sediment
2010-06-01 00:00:00,8500.0,120.5
2010-06-01 01:00:00,8520.0,121.0
2010-06-01 02:00:00,8480.0,119.8
"""

SAMPLE_EIDC_CSV_WITH_STATION = """\
datetime,discharge_m3s,sediment,station
2010-06-01 00:00:00,8500.0,120.5,chau_doc
2010-06-01 01:00:00,8520.0,121.0,chau_doc
2010-06-01 02:00:00,3200.0,55.0,tan_chau
"""

SAMPLE_EIDC_CSV_MISSING = """\
datetime,discharge_m3s,sediment
2010-06-01 00:00:00,8500.0,120.5
2010-06-01 01:00:00,NA,
2010-06-01 02:00:00,,119.8
"""

MOCK_EIDC_RESPONSE = {
    "data": [
        {
            "datetime": "2010-06-01T00:00:00",
            "discharge_m3s": 8500.0,
        },
        {
            "datetime": "2010-06-01T01:00:00",
            "discharge_m3s": 8520.0,
        },
    ]
}


# ---------------------------------------------------------------------------
# Station listing tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_stations_returns_seed_list():
    """Seed list returns all 4 Mekong Delta stations."""
    async with VietnamMekongConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    assert len(stations) == 4


@pytest.mark.asyncio
async def test_fetch_stations_metadata():
    """Seed stations have correct metadata fields."""
    async with VietnamMekongConnector() as conn:
        stations = await conn.fetch_stations()

    chau_doc = next(
        s for s in stations if s.native_id == "chau_doc"
    )
    assert chau_doc.id == "vietnam_mekong:chau_doc"
    assert chau_doc.provider == "vietnam_mekong"
    assert chau_doc.name == "Chau Doc"
    assert chau_doc.latitude == pytest.approx(10.70)
    assert chau_doc.longitude == pytest.approx(105.12)
    assert chau_doc.country_code == "VN"
    assert chau_doc.river == "Mekong (Bassac)"


# ---------------------------------------------------------------------------
# EIDC API tests (respx mocks)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_eidc_api():
    """EIDC catalogue API response is parsed correctly."""
    url = (
        f"https://catalogue.ceh.ac.uk/documents/"
        f"{EIDC_CATALOGUE_DOC}"
    )
    respx.get(url).mock(
        return_value=httpx.Response(
            200, json=MOCK_EIDC_RESPONSE,
        ),
    )

    async with VietnamMekongConnector() as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 2, tzinfo=UTC),
        )

    assert chunk.provider == "vietnam_mekong"
    assert chunk.station_id == "vietnam_mekong:chau_doc"
    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(
        8500.0,
    )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_api_fails_csv_fallback(
    tmp_path: Path,
):
    """Falls back to CSV when EIDC catalogue API fails."""
    url = (
        f"https://catalogue.ceh.ac.uk/documents/"
        f"{EIDC_CATALOGUE_DOC}"
    )
    respx.get(url).mock(
        return_value=httpx.Response(500),
    )

    csv_file = tmp_path / "chau_doc.csv"
    csv_file.write_text(SAMPLE_EIDC_CSV, encoding="utf-8")

    async with VietnamMekongConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 1, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(
        8500.0,
    )


# ---------------------------------------------------------------------------
# Local CSV tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_csv_parses_correctly(
    tmp_path: Path,
):
    """EIDC CSV with hourly discharge data is parsed correctly."""
    csv_file = tmp_path / "chau_doc.csv"
    csv_file.write_text(SAMPLE_EIDC_CSV, encoding="utf-8")

    async with VietnamMekongConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 1, 3, tzinfo=UTC),
        )

    assert chunk.station_id == "vietnam_mekong:chau_doc"
    assert chunk.provider == "vietnam_mekong"
    assert len(chunk.observations) == 3
    assert chunk.observations[1].discharge_m3s == pytest.approx(
        8520.0,
    )
    assert chunk.observations[0].quality.value == "raw"


@pytest.mark.asyncio
async def test_fetch_observations_csv_handles_missing(
    tmp_path: Path,
):
    """NA and empty values are treated as missing."""
    csv_file = tmp_path / "chau_doc.csv"
    csv_file.write_text(
        SAMPLE_EIDC_CSV_MISSING, encoding="utf-8",
    )

    async with VietnamMekongConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 1, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s is not None
    assert chunk.observations[1].discharge_m3s is None
    assert chunk.observations[1].quality.value == "missing"
    assert chunk.observations[2].discharge_m3s is None


@pytest.mark.asyncio
async def test_fetch_observations_csv_station_filter(
    tmp_path: Path,
):
    """CSV with station column filters by station."""
    csv_file = tmp_path / "mekong_data.csv"
    csv_file.write_text(
        SAMPLE_EIDC_CSV_WITH_STATION, encoding="utf-8",
    )

    async with VietnamMekongConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 1, 3, tzinfo=UTC),
        )

    # Only chau_doc rows, not tan_chau
    assert len(chunk.observations) == 2
    for obs in chunk.observations:
        assert obs.discharge_m3s != pytest.approx(3200.0)


@pytest.mark.asyncio
async def test_fetch_observations_no_data_dir():
    """Without data_dir and no web, returns empty chunk."""
    async with VietnamMekongConnector() as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 2, tzinfo=UTC),
        )

    assert chunk.station_id == "vietnam_mekong:chau_doc"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_connector_registration():
    """Connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("vietnam_mekong")
    assert cls is VietnamMekongConnector


@pytest.mark.asyncio
async def test_seed_station_ids_are_canonical():
    """Every seed station has a properly formatted CSFS station ID."""
    async with VietnamMekongConnector() as conn:
        stations = await conn.fetch_stations()

    for station in stations:
        assert station.id == (
            f"vietnam_mekong:{station.native_id}"
        )
        assert station.provider == "vietnam_mekong"
        assert station.country_code == "VN"
