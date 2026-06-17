"""Tests for the CAMELS-CH connector (per-gauge obs-based CSV; '#'-commented attrs)."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from csfs.connectors.camels_ch import CAMELSCHConnector

SAMPLE_TS = (
    "date,discharge_vol(m3/s),discharge_spec(mm/d),waterlevel(m)\n"
    "1981-01-01,NaN,NaN,428.99\n"
    "1981-01-02,12.5,0.3,429.10\n"
    "1981-01-03,13.0,0.31,429.20\n"
)
# Leading '#' comment line before the header, as in the real attributes file.
SAMPLE_TOPO = (
    "# Topographic attributes derived from BAFU/Swisstopo\n"
    "gauge_id,country,gauge_name,gauge_lon,gauge_lat,area\n"
    "2004,CH,Murten,7.12,46.93,712.7\n"
    "2007,CH,Other,8.0,47.0,500.0\n"
)


def _ts_dir(tmp_path: Path) -> Path:
    d = tmp_path / "timeseries" / "observation_based"
    d.mkdir(parents=True)
    (d / "CAMELS_CH_obs_based_2004.csv").write_text(SAMPLE_TS, encoding="utf-8")
    return tmp_path


@pytest.mark.asyncio
async def test_fetch_observations_parses_discharge_vol(tmp_path: Path):
    _ts_dir(tmp_path)
    async with CAMELSCHConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_ch:2004",
            start=datetime(1981, 1, 1, tzinfo=UTC),
            end=datetime(1981, 1, 5, tzinfo=UTC),
        )
    assert chunk.provider == "camels_ch"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s is None  # NaN -> missing
    assert chunk.observations[0].quality.value == "missing"
    assert chunk.observations[1].discharge_m3s == pytest.approx(12.5)


@pytest.mark.asyncio
async def test_fetch_observations_window(tmp_path: Path):
    _ts_dir(tmp_path)
    async with CAMELSCHConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_ch:2004",
            start=datetime(1981, 1, 2, tzinfo=UTC),
            end=datetime(1981, 1, 2, tzinfo=UTC),
        )
    assert [o.timestamp.day for o in chunk.observations] == [2]


@pytest.mark.asyncio
async def test_fetch_observations_missing_file_empty(tmp_path: Path):
    async with CAMELSCHConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_ch:9999",
            start=datetime(1981, 1, 1, tzinfo=UTC),
            end=datetime(1981, 1, 5, tzinfo=UTC),
        )
    assert chunk.observations == []


@pytest.mark.asyncio
async def test_fetch_stations_skips_comment_line(tmp_path: Path):
    d = tmp_path / "static_attributes"
    d.mkdir(parents=True)
    (d / "CAMELS_CH_topographic_attributes.csv").write_text(SAMPLE_TOPO, encoding="utf-8")
    async with CAMELSCHConnector(config={"data_dir": str(tmp_path)}) as conn:
        stations = await conn.fetch_stations()
    assert len(stations) == 2
    s = next(st for st in stations if st.native_id == "2004")
    assert s.id == "camels_ch:2004"
    assert s.country_code == "CH"
    assert s.name == "Murten"
    assert s.latitude == pytest.approx(46.93)
    assert s.longitude == pytest.approx(7.12)
