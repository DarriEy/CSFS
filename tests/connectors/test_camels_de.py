"""Tests for the CAMELS-DE connector (file-based parsing of the Zenodo bundle)."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from csfs.connectors.camels_de import CAMELSDEConnector

# date,discharge_vol_obs,discharge_spec_obs,... (comma-separated, blank = missing)
SAMPLE_TIMESERIES = """\
date,discharge_vol_obs,discharge_spec_obs,water_level_obs
1990-01-01,12.5,0.3,1.1
1990-01-02,13.0,0.31,1.2
1990-01-03,,,
1990-01-04,14.25,0.34,1.3
"""

SAMPLE_TOPO = """\
gauge_id,provider_id,gauge_name,water_body_name,federal_state,gauge_lat,gauge_lon,gauge_easting
DE210480,X,Sample Gauge,Sample River,BW,48.1234,9.5678,500000
DE214380,Y,Other,Other River,BY,49.0,11.0,600000
"""


def _ts_dir(tmp_path: Path) -> Path:
    d = tmp_path / "timeseries"
    d.mkdir(parents=True)
    (d / "CAMELS_DE_hydromet_timeseries_DE210480.csv").write_text(SAMPLE_TIMESERIES, encoding="utf-8")
    return tmp_path


@pytest.mark.asyncio
async def test_fetch_observations_parses_discharge_vol_obs(tmp_path: Path):
    _ts_dir(tmp_path)
    async with CAMELSDEConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_de:DE210480",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 5, tzinfo=UTC),
        )
    assert chunk.station_id == "camels_de:DE210480"
    assert chunk.provider == "camels_de"
    assert len(chunk.observations) == 4  # incl. the blank row as MISSING
    assert chunk.observations[0].discharge_m3s == pytest.approx(12.5)
    # Blank discharge -> missing.
    missing = chunk.observations[2]
    assert missing.discharge_m3s is None
    assert missing.quality.value == "missing"
    assert chunk.observations[3].discharge_m3s == pytest.approx(14.25)


@pytest.mark.asyncio
async def test_fetch_observations_window_filtering(tmp_path: Path):
    _ts_dir(tmp_path)
    async with CAMELSDEConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_de:DE210480",
            start=datetime(1990, 1, 2, tzinfo=UTC),
            end=datetime(1990, 1, 2, tzinfo=UTC),
        )
    assert [o.timestamp.day for o in chunk.observations] == [2]


@pytest.mark.asyncio
async def test_fetch_observations_missing_file_is_empty(tmp_path: Path):
    async with CAMELSDEConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_de:DE999999",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 5, tzinfo=UTC),
        )
    assert chunk.observations == []
    assert chunk.provider == "camels_de"


@pytest.mark.asyncio
async def test_fetch_stations_from_topographic_attributes(tmp_path: Path):
    (tmp_path / "CAMELS_DE_topographic_attributes.csv").write_text(SAMPLE_TOPO, encoding="utf-8")
    async with CAMELSDEConnector(config={"data_dir": str(tmp_path)}) as conn:
        stations = await conn.fetch_stations()
    assert len(stations) == 2
    s = next(st for st in stations if st.native_id == "DE210480")
    assert s.id == "camels_de:DE210480"
    assert s.country_code == "DE"
    assert s.latitude == pytest.approx(48.1234)
    assert s.longitude == pytest.approx(9.5678)
