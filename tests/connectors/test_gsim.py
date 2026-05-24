"""Tests for the GSIM (Global Streamflow Indices) connector."""

from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from csfs.connectors.gsim import (
    _SEED_STATIONS,
    GSIMConnector,
)

# ------------------------------------------------------------------
# Mock data
# ------------------------------------------------------------------

SAMPLE_GSIM_TEXT = """\
# GSIM Station: GSIM_US_0001
# River: Mississippi
# Country: US
# Indices: mean monthly flow (m3/s)
# Missing value: -999.0
#
year\tmonth\tmean\tmin\tmax
1960\t1\t8500.3\t6200.0\t12400.0
1960\t2\t9100.7\t7000.0\t13200.0
1960\t3\t-999.0\t-999.0\t-999.0
1960\t4\t11200.5\t8500.0\t15600.0
1960\t5\t14300.2\t10200.0\t19800.0
"""

SAMPLE_GSIM_CSV = (
    "year,month,mean,min,max\n"
    "1970,1,7200.0,5100.0,10300.0\n"
    "1970,2,7800.5,5500.0,11200.0\n"
    "1970,3,8400.0,6000.0,12100.0\n"
)

SAMPLE_GSIM_CSV_WITH_DATE = (
    "date,mean\n"
    "1980-01,5000.0\n"
    "1980-02,5500.0\n"
    "1980-03,-999.0\n"
)


# ------------------------------------------------------------------
# Station listing tests
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_stations_seed_default():
    """Default mode returns curated seed catalogue (no network)."""
    async with GSIMConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    first = stations[0]
    assert first.provider == "gsim"
    assert first.id.startswith("gsim:")


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_pangaea_verification():
    """When seed_only=False, connector verifies PANGAEA record."""
    respx.get(
        "https://doi.pangaea.de/10.1594/PANGAEA.887477",
    ).mock(
        return_value=httpx.Response(200, text="OK"),
    )

    async with GSIMConnector(
        config={"seed_only": False},
    ) as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_pangaea_unreachable_falls_back():
    """If PANGAEA is unreachable, connector falls back to seed."""
    respx.get(
        "https://doi.pangaea.de/10.1594/PANGAEA.887477",
    ).mock(
        return_value=httpx.Response(500),
    )

    async with GSIMConnector(
        config={"seed_only": False},
    ) as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)


# ------------------------------------------------------------------
# Observation / file-parsing tests
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_no_data_dir():
    """Without data_dir, returns empty chunk with guidance."""
    async with GSIMConnector() as conn:
        chunk = await conn.fetch_observations(
            "gsim:GSIM_US_0001",
            start=datetime(1960, 1, 1, tzinfo=UTC),
            end=datetime(1960, 12, 31, tzinfo=UTC),
        )

    assert chunk.station_id == "gsim:GSIM_US_0001"
    assert chunk.provider == "gsim"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_parses_text_file(
    tmp_path: Path,
):
    """Parse GSIM native text format with monthly indices."""
    gsim_file = tmp_path / "GSIM_US_0001.mon"
    gsim_file.write_text(SAMPLE_GSIM_TEXT, encoding="utf-8")

    async with GSIMConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "gsim:GSIM_US_0001",
            start=datetime(1960, 1, 1, tzinfo=UTC),
            end=datetime(1960, 12, 31, tzinfo=UTC),
        )

    assert len(chunk.observations) == 5
    # Normal value
    assert chunk.observations[0].discharge_m3s == pytest.approx(
        8500.3,
    )
    # Missing value (-999.0)
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
async def test_fetch_observations_parses_csv_file(
    tmp_path: Path,
):
    """Parse CSV-formatted GSIM data with year/month columns."""
    csv_file = tmp_path / "GSIM_DE_0001.csv"
    csv_file.write_text(SAMPLE_GSIM_CSV, encoding="utf-8")

    async with GSIMConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "gsim:GSIM_DE_0001",
            start=datetime(1970, 1, 1, tzinfo=UTC),
            end=datetime(1970, 12, 31, tzinfo=UTC),
        )

    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(
        7200.0,
    )
    assert chunk.observations[2].discharge_m3s == pytest.approx(
        8400.0,
    )


@pytest.mark.asyncio
async def test_fetch_observations_csv_with_date_column(
    tmp_path: Path,
):
    """Parse CSV with date column in YYYY-MM format."""
    csv_file = tmp_path / "GSIM_FR_0001.csv"
    csv_file.write_text(
        SAMPLE_GSIM_CSV_WITH_DATE, encoding="utf-8",
    )

    async with GSIMConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "gsim:GSIM_FR_0001",
            start=datetime(1980, 1, 1, tzinfo=UTC),
            end=datetime(1980, 12, 31, tzinfo=UTC),
        )

    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(
        5000.0,
    )
    # Missing value
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
async def test_seed_station_ids_are_canonical():
    """Every seed station has a properly formatted CSFS station ID."""
    async with GSIMConnector() as conn:
        stations = await conn.fetch_stations()

    for station in stations:
        assert station.id == f"gsim:{station.native_id}"
        assert station.provider == "gsim"
        assert station.latitude != 0.0 or station.longitude != 0.0


@pytest.mark.asyncio
async def test_connector_registration():
    """The connector is registered under the 'gsim' slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("gsim")
    assert cls is GSIMConnector
