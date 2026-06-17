"""Tests for the CAMELS-PE connector (flow_obs mm/day -> m3/s via catchment area)."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from csfs.connectors.camels_pe import CAMELSPEConnector

# flow_obs is OBSERVED streamflow in mm/day; flow_sim must be ignored.
SAMPLE_TS = (
    '"date","prec","flow_obs","flow_sim","pet"\n'
    "1981-01-01,0.34,0.952,0.921,4.0\n"
    "1981-01-02,0.92,0.975,0.514,4.1\n"
    "1981-01-03,1.07,NA,0.394,3.5\n"      # NA -> missing
    "1981-01-04,1.62,-1,0.294,3.6\n"       # negative -> missing
)
SAMPLE_STATIONS = (
    '"gauge_id","gauge_name","gauge_lat","gauge_lon"\n'
    '"PE_211408","Condorcerro",-8.658,-78.262\n'
)
SAMPLE_TOPO = (
    '"gauge_id","area","slope_mean"\n'
    '"PE_211408",10391.389,11.3\n'
)
_AREA = 10391.389


def _bundle(tmp_path: Path) -> Path:
    ts = tmp_path / "03_timeseries" / "by_catchment"
    ts.mkdir(parents=True)
    (ts / "PE_211408.csv").write_text(SAMPLE_TS, encoding="utf-8")
    (tmp_path / "02_attributes").mkdir()
    (tmp_path / "02_attributes" / "topographic_attributes.csv").write_text(SAMPLE_TOPO, encoding="utf-8")
    (tmp_path / "01_metadata").mkdir()
    (tmp_path / "01_metadata" / "stations.csv").write_text(SAMPLE_STATIONS, encoding="utf-8")
    return tmp_path


@pytest.mark.asyncio
async def test_fetch_observations_converts_mm_per_day(tmp_path: Path):
    _bundle(tmp_path)
    async with CAMELSPEConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_pe:PE_211408",
            start=datetime(1981, 1, 1, tzinfo=UTC),
            end=datetime(1981, 1, 10, tzinfo=UTC),
        )
    assert chunk.provider == "camels_pe"
    assert len(chunk.observations) == 4
    # 0.952 mm/day * 10391.389 km2 / 86.4 = 114.5 m3/s
    assert chunk.observations[0].discharge_m3s == pytest.approx(0.952 * _AREA / 86.4, rel=1e-6)
    assert chunk.observations[0].discharge_m3s == pytest.approx(114.5, abs=0.1)
    assert chunk.observations[2].discharge_m3s is None  # NA
    assert chunk.observations[3].discharge_m3s is None  # negative


@pytest.mark.asyncio
async def test_fetch_observations_missing_file_empty(tmp_path: Path):
    _bundle(tmp_path)
    async with CAMELSPEConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_pe:PE_999999",
            start=datetime(1981, 1, 1, tzinfo=UTC),
            end=datetime(1981, 1, 10, tzinfo=UTC),
        )
    assert chunk.observations == []


@pytest.mark.asyncio
async def test_fetch_stations(tmp_path: Path):
    _bundle(tmp_path)
    async with CAMELSPEConnector(config={"data_dir": str(tmp_path)}) as conn:
        stations = await conn.fetch_stations()
    assert len(stations) == 1
    s = stations[0]
    assert s.id == "camels_pe:PE_211408"
    assert s.country_code == "PE"
    assert s.latitude == pytest.approx(-8.658, abs=1e-3)
    assert s.longitude == pytest.approx(-78.262, abs=1e-3)
