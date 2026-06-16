"""Tests for the CAMELS-FI connector (CAMELS-GB layout + EPSG:3067 coords)."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from csfs.connectors.camels_fi import CAMELSFIConnector

SAMPLE_TS = (
    "date,discharge_vol,discharge_spec,precipitation\n"
    "2000-01-01,2.0,0.5,5.5\n"
    "2000-01-02,2.1,0.52,2.6\n"
    "2000-01-03,,,0.5\n"      # blank -> missing
)
SAMPLE_META = (
    "gauge_id,gauge_name,gauge_lon,gauge_lat,gauge_easting,gauge_northing,area\n"
    "896,Kontturi,30.320002,61.977772,673965,6876155,379.33\n"
)


def _ts_dir(tmp_path: Path) -> Path:
    d = tmp_path / "data" / "timeseries"
    d.mkdir(parents=True)
    (d / "CAMELS_FI_hydromet_timeseries_896_19610101-20231231.csv").write_text(
        SAMPLE_TS, encoding="utf-8")
    return tmp_path


@pytest.mark.asyncio
async def test_fetch_observations_globs_gauge_id(tmp_path: Path):
    _ts_dir(tmp_path)
    async with CAMELSFIConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_fi:896",
            start=datetime(2000, 1, 1, tzinfo=UTC),
            end=datetime(2000, 1, 10, tzinfo=UTC),
        )
    assert chunk.provider == "camels_fi"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(2.0)
    assert chunk.observations[2].discharge_m3s is None  # blank
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
async def test_fetch_observations_missing_file_empty(tmp_path: Path):
    async with CAMELSFIConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_fi:99999",
            start=datetime(2000, 1, 1, tzinfo=UTC),
            end=datetime(2000, 1, 10, tzinfo=UTC),
        )
    assert chunk.observations == []


@pytest.mark.asyncio
async def test_fetch_stations_reprojects_tm35fin(tmp_path: Path):
    pytest.importorskip("pyproj")
    (tmp_path / "CAMELS_FI_meta_attributes.csv").write_text(SAMPLE_META, encoding="utf-8")
    async with CAMELSFIConnector(config={"data_dir": str(tmp_path)}) as conn:
        stations = await conn.fetch_stations()
    assert len(stations) == 1
    s = stations[0]
    assert s.id == "camels_fi:896"
    assert s.country_code == "FI"
    # EPSG:3067 (673965, 6876155) reprojects to ~ (61.98, 30.32).
    assert s.latitude == pytest.approx(61.978, abs=1e-2)
    assert s.longitude == pytest.approx(30.320, abs=1e-2)
