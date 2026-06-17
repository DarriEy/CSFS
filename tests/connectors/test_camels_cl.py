"""Tests for the CAMELS-CL connector (wide-matrix streamflow + transposed attributes)."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from csfs.connectors.camels_cl import CAMELSCLConnector

# Wide matrix: row 0 = gauge_id + gauge IDs; col 0 = ISO date; missing = " ".
# Every field double-quoted and tab-separated, as in the PANGAEA file.
SAMPLE_MATRIX = (
    '"gauge_id"\t"1001001"\t"1001002"\n'
    '"1990-01-01"\t"5.96"\t" "\n'
    '"1990-01-02"\t"6.10"\t"0.50"\n'
    '"1990-01-03"\t" "\t"0.55"\n'
    '"1990-01-04"\t"7.20"\t"0.60"\n'
)

# Transposed attributes: attribute per row, gauge per column.
SAMPLE_ATTRS = (
    '"gauge_id"\t"1001001"\t"1001002"\n'
    '"gauge_name"\t"Rio Uno"\t"Rio Dos"\n'
    '"gauge_lat"\t"-17.5"\t"-18.2"\n'
    '"gauge_lon"\t"-69.5"\t"-70.1"\n'
    '"area"\t"123.0"\t"456.0"\n'
)


def _matrix_dir(tmp_path: Path) -> Path:
    (tmp_path / "2_CAMELScl_streamflow_m3s.txt").write_text(SAMPLE_MATRIX, encoding="utf-8")
    return tmp_path


@pytest.mark.asyncio
async def test_fetch_observations_selects_the_gauge_column(tmp_path: Path):
    _matrix_dir(tmp_path)
    async with CAMELSCLConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_cl:1001001",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 5, tzinfo=UTC),
        )
    assert chunk.station_id == "camels_cl:1001001"
    assert chunk.provider == "camels_cl"
    assert len(chunk.observations) == 4
    assert chunk.observations[0].discharge_m3s == pytest.approx(5.96)
    # 1990-01-03 is a quoted space for gauge 1001001 -> missing.
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"
    assert chunk.observations[3].discharge_m3s == pytest.approx(7.20)


@pytest.mark.asyncio
async def test_fetch_observations_second_gauge_independent_column(tmp_path: Path):
    _matrix_dir(tmp_path)
    async with CAMELSCLConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_cl:1001002",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 5, tzinfo=UTC),
        )
    # First row of gauge 1001002 is missing (" "); the rest are present.
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[1].discharge_m3s == pytest.approx(0.50)


@pytest.mark.asyncio
async def test_fetch_observations_window_filtering(tmp_path: Path):
    _matrix_dir(tmp_path)
    async with CAMELSCLConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_cl:1001001",
            start=datetime(1990, 1, 2, tzinfo=UTC),
            end=datetime(1990, 1, 2, tzinfo=UTC),
        )
    assert [o.timestamp.day for o in chunk.observations] == [2]


@pytest.mark.asyncio
async def test_fetch_observations_unknown_gauge_is_empty(tmp_path: Path):
    _matrix_dir(tmp_path)
    async with CAMELSCLConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_cl:9999999",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 5, tzinfo=UTC),
        )
    assert chunk.observations == []


@pytest.mark.asyncio
async def test_fetch_stations_from_transposed_attributes(tmp_path: Path):
    (tmp_path / "1_CAMELScl_attributes.txt").write_text(SAMPLE_ATTRS, encoding="utf-8")
    async with CAMELSCLConnector(config={"data_dir": str(tmp_path)}) as conn:
        stations = await conn.fetch_stations()
    assert len(stations) == 2
    s = next(st for st in stations if st.native_id == "1001001")
    assert s.id == "camels_cl:1001001"
    assert s.country_code == "CL"
    assert s.name == "Rio Uno"
    assert s.latitude == pytest.approx(-17.5)
    assert s.longitude == pytest.approx(-69.5)
