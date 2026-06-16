"""Tests for the CAMELS-NZ connector (per-station CSV + WGS84 info CSV)."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from csfs.connectors.camels_nz import CAMELSNZConnector

SAMPLE_TS = (
    '"time","flow"\n'
    "2000-01-01,0.640166666666667\n"
    "2000-01-02,0.71\n"
    "2000-01-03,NA\n"       # NA -> missing
    "2000-01-04,-1\n"        # negative -> missing
)
# The info CSV ships with a UTF-8 BOM and a "1." filename prefix.
SAMPLE_INFO = (
    "﻿Station_ID,RID,Station Name,Latitude (WGS 84),Longitude(WGS 84),uparea\n"
    "29605,9014161,Wainuiomata at Leonard Wood Park,-41.28376281,174.9478655,30.0\n"
)


def _ts_dir(tmp_path: Path) -> Path:
    (tmp_path / "daily_flow_station_id_29605.csv").write_text(SAMPLE_TS, encoding="utf-8")
    return tmp_path


@pytest.mark.asyncio
async def test_fetch_observations_flow_and_na(tmp_path: Path):
    _ts_dir(tmp_path)
    async with CAMELSNZConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_nz:29605",
            start=datetime(2000, 1, 1, tzinfo=UTC),
            end=datetime(2000, 1, 10, tzinfo=UTC),
        )
    assert chunk.provider == "camels_nz"
    assert len(chunk.observations) == 4
    assert chunk.observations[0].discharge_m3s == pytest.approx(0.640166, abs=1e-5)
    assert chunk.observations[2].discharge_m3s is None  # NA
    assert chunk.observations[2].quality.value == "missing"
    assert chunk.observations[3].discharge_m3s is None  # negative


@pytest.mark.asyncio
async def test_fetch_observations_missing_file_empty(tmp_path: Path):
    async with CAMELSNZConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_nz:99999",
            start=datetime(2000, 1, 1, tzinfo=UTC),
            end=datetime(2000, 1, 10, tzinfo=UTC),
        )
    assert chunk.observations == []


@pytest.mark.asyncio
async def test_fetch_stations_reads_bom_prefixed_info(tmp_path: Path):
    (tmp_path / "1.CAMELS_NZ_Catchment_information.csv").write_text(
        SAMPLE_INFO, encoding="utf-8")
    async with CAMELSNZConnector(config={"data_dir": str(tmp_path)}) as conn:
        stations = await conn.fetch_stations()
    assert len(stations) == 1
    s = stations[0]
    assert s.id == "camels_nz:29605"
    assert s.native_id == "29605"
    assert s.country_code == "NZ"
    assert s.latitude == pytest.approx(-41.2838, abs=1e-3)
    assert s.longitude == pytest.approx(174.9479, abs=1e-3)
