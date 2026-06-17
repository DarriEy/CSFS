"""Tests for the CAMELS-COL connector (manual / access-gated, best-effort parser).

CAMELS-COL is access-restricted (HTTP 403); the conventions exercised here are
the documented-but-unverified ones the connector targets. These tests prove the
parser is internally consistent against that documented layout — not that the
layout matches the real (inaccessible) archive.
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from csfs.connectors.camels_col import CAMELSCOLConnector

SAMPLE_TS = (
    "date,streamflow,precip\n"
    "2000-01-01,12.5,3.0\n"
    "2000-01-02,13.1,0.0\n"
    "2000-01-03,,0.0\n"      # blank -> missing
    "2000-01-04,-1,0.0\n"    # negative -> missing
)
SAMPLE_ATTRS = (
    "gauge_id,gauge_name,gauge_lat,gauge_lon\n"
    "13077030,Rio Test,4.5981,-74.0758\n"
)


@pytest.mark.asyncio
async def test_fetch_observations_streamflow_column(tmp_path: Path):
    (tmp_path / "CAMELS_COL_13077030.csv").write_text(SAMPLE_TS, encoding="utf-8")
    async with CAMELSCOLConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_col:13077030",
            start=datetime(2000, 1, 1, tzinfo=UTC),
            end=datetime(2000, 1, 10, tzinfo=UTC),
        )
    assert chunk.provider == "camels_col"
    assert len(chunk.observations) == 4
    assert chunk.observations[0].discharge_m3s == pytest.approx(12.5)
    assert chunk.observations[2].discharge_m3s is None  # blank
    assert chunk.observations[3].discharge_m3s is None  # negative


@pytest.mark.asyncio
async def test_attributes_file_not_treated_as_timeseries(tmp_path: Path):
    # An attributes file containing the gauge id must not be picked as the series.
    (tmp_path / "CAMELS_COL_attributes.csv").write_text(SAMPLE_ATTRS, encoding="utf-8")
    async with CAMELSCOLConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_col:13077030",
            start=datetime(2000, 1, 1, tzinfo=UTC),
            end=datetime(2000, 1, 10, tzinfo=UTC),
        )
    assert chunk.observations == []


@pytest.mark.asyncio
async def test_fetch_stations_from_attributes(tmp_path: Path):
    (tmp_path / "CAMELS_COL_attributes.csv").write_text(SAMPLE_ATTRS, encoding="utf-8")
    async with CAMELSCOLConnector(config={"data_dir": str(tmp_path)}) as conn:
        stations = await conn.fetch_stations()
    assert len(stations) == 1
    s = stations[0]
    assert s.id == "camels_col:13077030"
    assert s.country_code == "CO"
    assert s.latitude == pytest.approx(4.5981, abs=1e-3)
    assert s.longitude == pytest.approx(-74.0758, abs=1e-3)


@pytest.mark.asyncio
async def test_no_data_dir_returns_empty(tmp_path: Path):
    # No data_dir and no auto-download -> graceful empty (access-gated).
    async with CAMELSCOLConnector(config={"auto_download": False}) as conn:
        stations = await conn.fetch_stations()
    assert stations == []
