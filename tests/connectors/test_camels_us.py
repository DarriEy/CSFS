"""Tests for the CAMELS-US connector (per-basin qc txt, cfs->m3/s; tab gauge info)."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from csfs.connectors.camels_us import _CFS_TO_M3S, CAMELSUSConnector

# gaugeID YYYY MM DD discharge_cfs qc  (whitespace; -999 = missing)
SAMPLE_QC = (
    "01013500 1980 01 01 1000.00 A\n"
    "01013500 1980 01 02 -999.00 M\n"
    "01013500 1980 01 03 500.00 A\n"
)
# tab-separated gauge metadata
SAMPLE_INFO = (
    "HUC_02\tGAGE_ID\tGAGE_NAME\tLAT\tLONG\tDRAINAGE_AREA\n"
    "01\t01013500\tFish River\t47.2373\t-68.5827\t2252.7\n"
    "01\t01030500\tOther River\t45.5\t-69.0\t3676.2\n"
)


def _qc_dir(tmp_path: Path) -> Path:
    d = tmp_path / "usgs_streamflow" / "01"
    d.mkdir(parents=True)
    (d / "01013500_streamflow_qc.txt").write_text(SAMPLE_QC, encoding="utf-8")
    return tmp_path


@pytest.mark.asyncio
async def test_fetch_observations_converts_cfs(tmp_path: Path):
    _qc_dir(tmp_path)
    async with CAMELSUSConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_us:01013500",
            start=datetime(1980, 1, 1, tzinfo=UTC),
            end=datetime(1980, 1, 5, tzinfo=UTC),
        )
    assert chunk.provider == "camels_us"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(1000.0 * _CFS_TO_M3S)
    # -999 -> missing
    assert chunk.observations[1].discharge_m3s is None
    assert chunk.observations[1].quality.value == "missing"
    assert chunk.observations[2].discharge_m3s == pytest.approx(500.0 * _CFS_TO_M3S)


@pytest.mark.asyncio
async def test_fetch_observations_window(tmp_path: Path):
    _qc_dir(tmp_path)
    async with CAMELSUSConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_us:01013500",
            start=datetime(1980, 1, 3, tzinfo=UTC),
            end=datetime(1980, 1, 3, tzinfo=UTC),
        )
    assert [o.timestamp.day for o in chunk.observations] == [3]


@pytest.mark.asyncio
async def test_fetch_observations_missing_file_empty(tmp_path: Path):
    async with CAMELSUSConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_us:99999999",
            start=datetime(1980, 1, 1, tzinfo=UTC),
            end=datetime(1980, 1, 5, tzinfo=UTC),
        )
    assert chunk.observations == []


@pytest.mark.asyncio
async def test_fetch_stations_from_gauge_information(tmp_path: Path):
    d = tmp_path / "basin_metadata"
    d.mkdir(parents=True)
    (d / "gauge_information.txt").write_text(SAMPLE_INFO, encoding="utf-8")
    async with CAMELSUSConnector(config={"data_dir": str(tmp_path)}) as conn:
        stations = await conn.fetch_stations()
    assert len(stations) == 2
    s = next(st for st in stations if st.native_id == "01013500")
    assert s.id == "camels_us:01013500"
    assert s.country_code == "US"
    assert s.latitude == pytest.approx(47.2373)
    assert s.longitude == pytest.approx(-68.5827)
