"""Tests for the CAMELS-IND connector (wide matrix with year/month/day columns)."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from csfs.connectors.camels_ind import CAMELSINDConnector

# year,month,day then one column per gauge id; missing = empty cell.
SAMPLE_MATRIX = (
    "year,month,day,3002,3005\n"
    "1980,1,1,,23.2\n"
    "1980,1,2,5.5,36.1\n"
    "1980,1,3,6.0,\n"
    "1980,1,4,7.1,40.0\n"
)
SAMPLE_TOPO = (
    "gauge_id,cwc_lat,cwc_lon,elev_mean\n"
    "3002,19.2967,81.7889,622.65\n"
    "3005,20.0,82.0,500.0\n"
)


def _matrix_dir(tmp_path: Path) -> Path:
    d = tmp_path / "streamflow_timeseries"
    d.mkdir(parents=True)
    (d / "streamflow_observed.csv").write_text(SAMPLE_MATRIX, encoding="utf-8")
    return tmp_path


@pytest.mark.asyncio
async def test_fetch_observations_selects_gauge_and_marks_empty_missing(tmp_path: Path):
    _matrix_dir(tmp_path)
    async with CAMELSINDConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_ind:3002",
            start=datetime(1980, 1, 1, tzinfo=UTC),
            end=datetime(1980, 1, 5, tzinfo=UTC),
        )
    assert chunk.provider == "camels_ind"
    assert len(chunk.observations) == 4
    assert chunk.observations[0].discharge_m3s is None  # empty cell
    assert chunk.observations[0].quality.value == "missing"
    assert chunk.observations[1].discharge_m3s == pytest.approx(5.5)


@pytest.mark.asyncio
async def test_fetch_observations_independent_columns(tmp_path: Path):
    _matrix_dir(tmp_path)
    async with CAMELSINDConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_ind:3005",
            start=datetime(1980, 1, 1, tzinfo=UTC),
            end=datetime(1980, 1, 5, tzinfo=UTC),
        )
    assert chunk.observations[0].discharge_m3s == pytest.approx(23.2)
    assert chunk.observations[2].discharge_m3s is None  # empty for 3005 on day 3


@pytest.mark.asyncio
async def test_fetch_observations_unknown_gauge_empty(tmp_path: Path):
    _matrix_dir(tmp_path)
    async with CAMELSINDConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_ind:9999",
            start=datetime(1980, 1, 1, tzinfo=UTC),
            end=datetime(1980, 1, 5, tzinfo=UTC),
        )
    assert chunk.observations == []


@pytest.mark.asyncio
async def test_fetch_stations_from_topo(tmp_path: Path):
    d = tmp_path / "attributes_csv"
    d.mkdir(parents=True)
    (d / "camels_ind_topo.csv").write_text(SAMPLE_TOPO, encoding="utf-8")
    async with CAMELSINDConnector(config={"data_dir": str(tmp_path)}) as conn:
        stations = await conn.fetch_stations()
    assert len(stations) == 2
    s = next(st for st in stations if st.native_id == "3002")
    assert s.id == "camels_ind:3002"
    assert s.country_code == "IN"
    assert s.latitude == pytest.approx(19.2967)
    assert s.longitude == pytest.approx(81.7889)
