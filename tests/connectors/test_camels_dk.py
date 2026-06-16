"""Tests for the CAMELS-DK connector (per-catchment Qobs CSV; UTM32N reprojection)."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from csfs.connectors.camels_dk import CAMELSDKConnector

SAMPLE_TS = (
    "time,catch_id,precipitation,Qdkm,Abstraction,Qobs\n"
    "1989-01-01,12410011,0.5,,,0.75\n"
    "1989-01-02,12410011,1.0,,,0.82\n"
    "1989-01-03,12410011,0.0,,,\n"
)
# catch_outlet_lon/lat are EASTING/NORTHING in EPSG:25832.
SAMPLE_TOPO = (
    "catch_id,catch_outlet_lon,catch_outlet_lat,catch_area\n"
    "12410011,474625.634,6329837.494,32195062\n"
    "11100001,500000.0,6200000.0,10000000\n"
)


def _ts_dir(tmp_path: Path) -> Path:
    (tmp_path / "CAMELS_DK_obs_based_12410011.csv").write_text(SAMPLE_TS, encoding="utf-8")
    return tmp_path


@pytest.mark.asyncio
async def test_fetch_observations_reads_qobs(tmp_path: Path):
    _ts_dir(tmp_path)
    async with CAMELSDKConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_dk:12410011",
            start=datetime(1989, 1, 1, tzinfo=UTC),
            end=datetime(1989, 1, 5, tzinfo=UTC),
        )
    assert chunk.provider == "camels_dk"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(0.75)
    assert chunk.observations[2].discharge_m3s is None  # blank Qobs
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
async def test_fetch_observations_missing_file_empty(tmp_path: Path):
    async with CAMELSDKConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_dk:99999999",
            start=datetime(1989, 1, 1, tzinfo=UTC),
            end=datetime(1989, 1, 5, tzinfo=UTC),
        )
    assert chunk.observations == []


@pytest.mark.asyncio
async def test_fetch_stations_reprojects_utm32n_to_wgs84(tmp_path: Path):
    (tmp_path / "CAMELS_DK_topography.csv").write_text(SAMPLE_TOPO, encoding="utf-8")
    async with CAMELSDKConnector(config={"data_dir": str(tmp_path)}) as conn:
        stations = await conn.fetch_stations()
    assert len(stations) == 2
    s = next(st for st in stations if st.native_id == "12410011")
    assert s.id == "camels_dk:12410011"
    assert s.country_code == "DK"
    # 474625.6 E, 6329837.5 N in EPSG:25832 is in Denmark (~9.0E, ~57.1N).
    assert 54.0 < s.latitude < 58.0
    assert 8.0 < s.longitude < 13.0
