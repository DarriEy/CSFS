"""Tests for the CAMELS-SE connector (per-catchment CSV + WGS84 station shapefile)."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from csfs.connectors.camels_se import CAMELSSEConnector

# Year,Month,Day,Qobs_m3s,... — Swedish chars in the FILENAME only (id is ASCII).
SAMPLE_TS = (
    "Year,Month,Day,Qobs_m3s,Qobs_mm,Pobs_mm,Tobs_C\n"
    "1990,1,1,4.24,0.35,2.0,-0.5\n"
    "1990,1,2,4.31,0.36,0.3,0.7\n"
    "1990,1,3,,,,\n"
    "1990,1,4,-9999,,,\n"
)


def _ts_dir(tmp_path: Path) -> Path:
    d = tmp_path / "catchment time series"
    d.mkdir(parents=True)
    # Name carries a Swedish character; the id (1069) is ASCII so the glob is exact.
    (d / "catchment_id_1069_MÖCKELN.csv").write_text(SAMPLE_TS, encoding="latin-1")
    return tmp_path


@pytest.mark.asyncio
async def test_fetch_observations_globs_on_ascii_id(tmp_path: Path):
    _ts_dir(tmp_path)
    async with CAMELSSEConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_se:1069",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 10, tzinfo=UTC),
        )
    assert chunk.provider == "camels_se"
    assert len(chunk.observations) == 4
    assert chunk.observations[0].discharge_m3s == pytest.approx(4.24)
    assert chunk.observations[2].discharge_m3s is None  # blank
    assert chunk.observations[2].quality.value == "missing"
    assert chunk.observations[3].discharge_m3s is None  # -9999 sentinel
    assert chunk.observations[3].quality.value == "missing"


@pytest.mark.asyncio
async def test_fetch_observations_missing_file_empty(tmp_path: Path):
    async with CAMELSSEConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_se:99999",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 10, tzinfo=UTC),
        )
    assert chunk.observations == []


@pytest.mark.asyncio
async def test_fetch_stations_reads_wgs84_point_shapefile(tmp_path: Path):
    fiona = pytest.importorskip("fiona")

    schema = {
        "geometry": "Point",
        "properties": {"id": "int", "name": "str", "area": "float"},
    }
    shp = tmp_path / "Sweden_catchments_50_stations_WGS84.shp"
    with fiona.open(
        str(shp), "w", driver="ESRI Shapefile", crs="EPSG:4326", schema=schema,
    ) as dst:
        dst.write({
            "geometry": {"type": "Point", "coordinates": (14.125, 56.662)},
            "properties": {"id": 1069, "name": "MÖCKELN", "area": 1026.0},
        })

    async with CAMELSSEConnector(config={"data_dir": str(tmp_path)}) as conn:
        stations = await conn.fetch_stations()
    assert len(stations) == 1
    s = stations[0]
    assert s.id == "camels_se:1069"
    assert s.native_id == "1069"
    assert s.country_code == "SE"
    assert s.latitude == pytest.approx(56.662, abs=1e-3)
    assert s.longitude == pytest.approx(14.125, abs=1e-3)
