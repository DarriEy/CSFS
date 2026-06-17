"""Tests for the CAMELS-GB connector (per-gauge CSV with date-range filename)."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from csfs.connectors.camels_gb import CAMELSGBConnector

SAMPLE_TS = (
    "date,precipitation,pet,temperature,discharge_spec,discharge_vol\n"
    "1970-10-01,0.03,1.84,13.27,0.16,0.77\n"
    "1970-10-02,1.0,2.0,12.0,0.2,0.85\n"
    "1970-10-03,0.0,1.5,11.0,,\n"
)
SAMPLE_TOPO = (
    "gauge_id,gauge_name,gauge_lat,gauge_lon,area\n"
    "41004,Test Brook,50.9,-0.5,100.0\n"
    "41005,Other,51.0,-0.6,200.0\n"
)


def _ts_dir(tmp_path: Path) -> Path:
    d = tmp_path / "timeseries"
    d.mkdir(parents=True)
    (d / "CAMELS_GB_hydromet_timeseries_41004_19701001-20150930.csv").write_text(
        SAMPLE_TS, encoding="utf-8")
    return tmp_path


@pytest.mark.asyncio
async def test_fetch_observations_glob_matches_dated_filename(tmp_path: Path):
    _ts_dir(tmp_path)
    async with CAMELSGBConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_gb:41004",
            start=datetime(1970, 10, 1, tzinfo=UTC),
            end=datetime(1970, 10, 5, tzinfo=UTC),
        )
    assert chunk.provider == "camels_gb"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(0.77)
    assert chunk.observations[2].discharge_m3s is None  # blank discharge_vol
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
async def test_fetch_observations_missing_file_empty(tmp_path: Path):
    async with CAMELSGBConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_gb:99999",
            start=datetime(1970, 10, 1, tzinfo=UTC),
            end=datetime(1970, 10, 5, tzinfo=UTC),
        )
    assert chunk.observations == []


@pytest.mark.asyncio
async def test_fetch_stations(tmp_path: Path):
    (tmp_path / "CAMELS_GB_topographic_attributes.csv").write_text(SAMPLE_TOPO, encoding="utf-8")
    async with CAMELSGBConnector(config={"data_dir": str(tmp_path)}) as conn:
        stations = await conn.fetch_stations()
    assert len(stations) == 2
    s = next(st for st in stations if st.native_id == "41004")
    assert s.id == "camels_gb:41004"
    assert s.country_code == "GB"
    assert s.latitude == pytest.approx(50.9)
    assert s.longitude == pytest.approx(-0.5)
