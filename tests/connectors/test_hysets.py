"""Tests for the HYSETS connector (discharge from a multi-variable NetCDF)."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from csfs.connectors.hysets import HYSETSConnector, _country_of

# Two gauges (USGS + HYDAT); Watershed_ID maps into the NetCDF watershed dim.
SAMPLE_PROPS = (
    "Watershed_ID,Source,Name,Official_ID,Centroid_Lat_deg_N,Centroid_Lon_deg_E,"
    "Hydrometric_station_latitude,Hydrometric_station_longitude\n"
    '1,USGS,"POTOMAC",01646500,38.9,-77.1,38.94977778,-77.12763889\n'
    '2,HYDAT,"SAINT JOHN",01AD002,47.2,-68.6,47.25806,-68.59583\n'
)


def _write_props(tmp_path: Path) -> None:
    (tmp_path / "HYSETS_watershed_properties.txt").write_text(SAMPLE_PROPS, encoding="utf-8")


def _write_nc(tmp_path: Path) -> None:
    xr = pytest.importorskip("xarray")
    import numpy as np

    times = np.array(["2000-01-01", "2000-01-02", "2000-01-03", "2000-01-04"], dtype="datetime64[ns]")
    # watershed 0 (WID 1) = Potomac, watershed 1 (WID 2) = Saint John.
    discharge = np.array([[148.66, 142.43, np.nan, -1.0], [10.0, 11.0, 12.0, 13.0]], dtype="float32")
    d = xr.Dataset(
        {
            "discharge": (("watershed", "time"), discharge),
            "watershedID": (("watershed",), np.array([1.0, 2.0])),
        },
        coords={"time": times},
    )
    d.to_netcdf(tmp_path / "HYSETS_2023_update_QC_stations.nc")


def test_country_of_maps_sources():
    assert _country_of("HYDAT") == "CA"
    assert _country_of("USGS") == "US"
    assert _country_of("BANDAS") == "MX"


@pytest.mark.asyncio
async def test_fetch_stations_from_properties(tmp_path: Path):
    _write_props(tmp_path)
    async with HYSETSConnector(config={"data_dir": str(tmp_path)}) as conn:
        stations = await conn.fetch_stations()
    assert len(stations) == 2
    s = next(st for st in stations if st.native_id == "01646500")
    assert s.id == "hysets:01646500"
    assert s.country_code == "US"
    assert s.latitude == pytest.approx(38.9498, abs=1e-3)
    assert s.longitude == pytest.approx(-77.1276, abs=1e-3)
    assert next(st for st in stations if st.native_id == "01AD002").country_code == "CA"


@pytest.mark.asyncio
async def test_fetch_observations_reads_netcdf_discharge(tmp_path: Path):
    pytest.importorskip("xarray")
    _write_props(tmp_path)
    _write_nc(tmp_path)
    async with HYSETSConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "hysets:01646500",
            start=datetime(2000, 1, 1, tzinfo=UTC),
            end=datetime(2000, 1, 10, tzinfo=UTC),
        )
    assert chunk.provider == "hysets"
    assert len(chunk.observations) == 4
    assert chunk.observations[0].discharge_m3s == pytest.approx(148.66, abs=1e-2)
    assert chunk.observations[2].discharge_m3s is None  # NaN -> missing
    assert chunk.observations[2].quality.value == "missing"
    assert chunk.observations[3].discharge_m3s is None  # negative -> missing


@pytest.mark.asyncio
async def test_fetch_observations_unknown_station_empty(tmp_path: Path):
    _write_props(tmp_path)
    async with HYSETSConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "hysets:99999999",
            start=datetime(2000, 1, 1, tzinfo=UTC),
            end=datetime(2000, 1, 10, tzinfo=UTC),
        )
    assert chunk.observations == []
