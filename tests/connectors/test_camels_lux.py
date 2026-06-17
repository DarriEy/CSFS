"""Tests for the CAMELS-LUX connector (Q+Qflag CSV + WGS84 gauge shapefile)."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from csfs.connectors.camels_lux import CAMELSLUXConnector

SAMPLE_TS = (
    "Date,Q,Qspec,Qflag\n"
    "2005-01-01,2.273,0.30,0\n"     # original -> raw
    "2005-01-02,2.5,0.33,1\n"        # gap-filled -> estimated
    "2005-01-03,NaN,NaN,0\n"         # NaN -> missing
    "2005-01-04,-1,0,0\n"            # negative -> missing
)


def _ts_dir(tmp_path: Path) -> Path:
    d = tmp_path / "timeseries" / "daily"
    d.mkdir(parents=True)
    (d / "CAMELS_LUX_hydromet_timeseries__daily_ID_01.csv").write_text(
        SAMPLE_TS, encoding="utf-8")
    return tmp_path


@pytest.mark.asyncio
async def test_fetch_observations_q_and_qflag(tmp_path: Path):
    _ts_dir(tmp_path)
    async with CAMELSLUXConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_lux:ID_01",
            start=datetime(2005, 1, 1, tzinfo=UTC),
            end=datetime(2005, 1, 10, tzinfo=UTC),
        )
    assert chunk.provider == "camels_lux"
    assert len(chunk.observations) == 4
    assert chunk.observations[0].discharge_m3s == pytest.approx(2.273)
    assert chunk.observations[0].quality.value == "raw"
    assert chunk.observations[1].discharge_m3s == pytest.approx(2.5)
    assert chunk.observations[1].quality.value == "estimated"  # Qflag=1
    assert chunk.observations[2].discharge_m3s is None  # NaN
    assert chunk.observations[3].discharge_m3s is None  # negative


@pytest.mark.asyncio
async def test_fetch_observations_missing_file_empty(tmp_path: Path):
    async with CAMELSLUXConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_lux:ID_99",
            start=datetime(2005, 1, 1, tzinfo=UTC),
            end=datetime(2005, 1, 10, tzinfo=UTC),
        )
    assert chunk.observations == []


@pytest.mark.asyncio
async def test_fetch_stations_reads_wgs84_gauge_shapefile(tmp_path: Path):
    fiona = pytest.importorskip("fiona")

    schema = {"geometry": "Point", "properties": {"gauge_id": "str", "Station": "str"}}
    shp = tmp_path / "stream-gauges_CAMELS-LUX.shp"
    with fiona.open(
        str(shp), "w", driver="ESRI Shapefile", crs="EPSG:4326", schema=schema,
    ) as dst:
        dst.write({
            "geometry": {"type": "Point", "coordinates": (6.115, 49.526)},
            "properties": {"gauge_id": "ID_01", "Station": "Alzette1_Livange"},
        })

    async with CAMELSLUXConnector(config={"data_dir": str(tmp_path)}) as conn:
        stations = await conn.fetch_stations()
    assert len(stations) == 1
    s = stations[0]
    assert s.id == "camels_lux:ID_01"
    assert s.native_id == "ID_01"
    assert s.country_code == "LU"
    assert s.latitude == pytest.approx(49.526, abs=1e-3)
    assert s.longitude == pytest.approx(6.115, abs=1e-3)
