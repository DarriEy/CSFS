"""Tests for the CAMELS-BR connector (file-based parsing of the Zenodo archive)."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from csfs.connectors.camels_br import CAMELSBRConnector

# year month day streamflow_m3s qual_control_by_ana qual_flag  (whitespace-sep)
SAMPLE_STREAMFLOW = """\
year month day streamflow_m3s qual_control_by_ana qual_flag
1995 01 01 42611.270000 0 1
1995 01 02 42829.609000 0 1
1995 01 03 -999.000000 0 0
1995 01 04 43011.785000 0 1
"""

# gauge_id gauge_name gauge_region gauge_lat gauge_lon area_ana ...
SAMPLE_LOCATION = """\
gauge_id gauge_name gauge_region gauge_lat gauge_lon area_ana area_gsim area_gsim_quality
10100000 tabatinga amazon -4.23470 -69.94470 874000.0 883138.5 high
10200000 palmeiras amazon -5.13890 -72.81360 16500.0 16558.0 high
"""


def _streamflow_dir(tmp_path: Path) -> Path:
    d = tmp_path / "02_CAMELS_BR_streamflow_m3s"
    d.mkdir(parents=True)
    (d / "10100000_streamflow_m3s.txt").write_text(SAMPLE_STREAMFLOW, encoding="utf-8")
    return tmp_path


@pytest.mark.asyncio
async def test_fetch_observations_parses_and_filters(tmp_path: Path):
    _streamflow_dir(tmp_path)
    async with CAMELSBRConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_br:10100000",
            start=datetime(1995, 1, 1, tzinfo=UTC),
            end=datetime(1995, 1, 5, tzinfo=UTC),
        )
    assert chunk.station_id == "camels_br:10100000"
    assert chunk.provider == "camels_br"
    # 4 rows minus the -999 missing-data sentinel = 3 observations.
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(42611.27)
    assert all(o.discharge_m3s is not None and o.discharge_m3s >= 0 for o in chunk.observations)
    assert [o.timestamp.day for o in chunk.observations] == [1, 2, 4]


@pytest.mark.asyncio
async def test_fetch_observations_window_filtering(tmp_path: Path):
    _streamflow_dir(tmp_path)
    async with CAMELSBRConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_br:10100000",
            start=datetime(1995, 1, 2, tzinfo=UTC),
            end=datetime(1995, 1, 2, tzinfo=UTC),
        )
    assert [o.timestamp.day for o in chunk.observations] == [2]


@pytest.mark.asyncio
async def test_fetch_observations_missing_file_is_empty(tmp_path: Path):
    async with CAMELSBRConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_br:99999999",
            start=datetime(1995, 1, 1, tzinfo=UTC),
            end=datetime(1995, 1, 5, tzinfo=UTC),
        )
    assert chunk.observations == []
    assert chunk.provider == "camels_br"


@pytest.mark.asyncio
async def test_fetch_stations_from_location_file(tmp_path: Path):
    attr_dir = tmp_path / "01_CAMELS_BR_attributes"
    attr_dir.mkdir(parents=True)
    (attr_dir / "camels_br_location.txt").write_text(SAMPLE_LOCATION, encoding="utf-8")

    async with CAMELSBRConnector(config={"data_dir": str(tmp_path)}) as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    s0 = next(s for s in stations if s.native_id == "10100000")
    assert s0.id == "camels_br:10100000"
    assert s0.provider == "camels_br"
    assert s0.country_code == "BR"
    assert s0.latitude == pytest.approx(-4.2347)
    assert s0.longitude == pytest.approx(-69.9447)
