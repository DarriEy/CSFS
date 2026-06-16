"""Tests for the CAMELS-AUS connector (ML/day wide matrix + bare master-table attrs)."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from csfs.connectors.camels_aus import _MLD_TO_M3S, CAMELSAUSConnector

# year,month,day then one column per station id (ML/day); missing = -99.99.
SAMPLE_MATRIX = (
    "year,month,day,912101A,915011A\n"
    "1990,1,1,-99.99,864.0\n"
    "1990,1,2,8640.0,1728.0\n"
    "1990,1,3,4320.0,-99.99\n"
)
# Bare master table: row per station with outlet coords.
SAMPLE_MASTER = (
    "station_id,station_name,lat_outlet,long_outlet\n"
    "912101A,Test River,-17.5,145.2\n"
    "915011A,Other River,-18.0,146.0\n"
)


def _matrix_dir(tmp_path: Path) -> Path:
    (tmp_path / "streamflow_MLd.csv").write_text(SAMPLE_MATRIX, encoding="utf-8")
    return tmp_path


@pytest.mark.asyncio
async def test_fetch_observations_converts_mld_to_m3s(tmp_path: Path):
    _matrix_dir(tmp_path)
    async with CAMELSAUSConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_aus:912101A",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 5, tzinfo=UTC),
        )
    assert chunk.provider == "camels_aus"
    assert len(chunk.observations) == 3
    # -99.99 sentinel -> missing.
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[0].quality.value == "missing"
    # 8640 ML/day = 8640 * 1000 / 86400 = 100 m3/s.
    assert chunk.observations[1].discharge_m3s == pytest.approx(8640.0 * _MLD_TO_M3S)
    assert chunk.observations[1].discharge_m3s == pytest.approx(100.0)


@pytest.mark.asyncio
async def test_fetch_observations_independent_columns(tmp_path: Path):
    _matrix_dir(tmp_path)
    async with CAMELSAUSConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_aus:915011A",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 5, tzinfo=UTC),
        )
    assert chunk.observations[0].discharge_m3s == pytest.approx(864.0 * _MLD_TO_M3S)
    assert chunk.observations[2].discharge_m3s is None  # -99.99 for 915011A on day 3


@pytest.mark.asyncio
async def test_fetch_observations_unknown_station_empty(tmp_path: Path):
    _matrix_dir(tmp_path)
    async with CAMELSAUSConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_aus:000000A",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 5, tzinfo=UTC),
        )
    assert chunk.observations == []


@pytest.mark.asyncio
async def test_fetch_stations_from_bare_master_table(tmp_path: Path):
    (tmp_path / "CAMELS_AUS_Attributes&Indices_MasterTable.csv").write_text(
        SAMPLE_MASTER, encoding="utf-8")
    async with CAMELSAUSConnector(config={"data_dir": str(tmp_path)}) as conn:
        stations = await conn.fetch_stations()
    assert len(stations) == 2
    s = next(st for st in stations if st.native_id == "912101A")
    assert s.id == "camels_aus:912101A"
    assert s.country_code == "AU"
    assert s.latitude == pytest.approx(-17.5)
    assert s.longitude == pytest.approx(145.2)
