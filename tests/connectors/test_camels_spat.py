"""Tests for the CAMELS-SPAT connector (manual / Globus-only, best-effort parser).

CAMELS-SPAT is distribution-gated (FRDR Globus-only); the conventions exercised
here are the documented-but-unverified ones the connector targets. These tests
prove the parser is internally consistent against that documented layout — not
that the layout matches the real (Globus-only) archive.
"""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from csfs.connectors.camels_spat import CAMELSSPATConnector

SAMPLE_META = (
    "gauge_id,lat,lon,country\n"
    "01646500,38.9498,-77.1276,US\n"
    "05BB001,51.1722,-115.5717,CA\n"
)


def _write_nc(tmp_path: Path, gauge: str) -> None:
    xr = pytest.importorskip("xarray")
    import numpy as np

    times = np.array(["2000-01-01", "2000-01-02", "2000-01-03", "2000-01-04"], dtype="datetime64[ns]")
    flow = np.array([148.66, 142.43, np.nan, -1.0], dtype="float32")
    d = xr.Dataset({"streamflow": (("time",), flow)}, coords={"time": times})
    d.to_netcdf(tmp_path / f"CAMELS_SPAT_{gauge}_daily.nc")


@pytest.mark.asyncio
async def test_fetch_observations_reads_basin_netcdf(tmp_path: Path):
    pytest.importorskip("xarray")
    _write_nc(tmp_path, "01646500")
    async with CAMELSSPATConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_spat:01646500",
            start=datetime(2000, 1, 1, tzinfo=UTC),
            end=datetime(2000, 1, 10, tzinfo=UTC),
        )
    assert chunk.provider == "camels_spat"
    assert len(chunk.observations) == 4
    assert chunk.observations[0].discharge_m3s == pytest.approx(148.66, abs=1e-2)
    assert chunk.observations[2].discharge_m3s is None  # NaN
    assert chunk.observations[3].discharge_m3s is None  # negative


@pytest.mark.asyncio
async def test_fetch_stations_from_metadata(tmp_path: Path):
    (tmp_path / "CAMELS_SPAT_metadata.csv").write_text(SAMPLE_META, encoding="utf-8")
    async with CAMELSSPATConnector(config={"data_dir": str(tmp_path)}) as conn:
        stations = await conn.fetch_stations()
    assert len(stations) == 2
    usgs = next(s for s in stations if s.native_id == "01646500")
    assert usgs.country_code == "US"
    assert usgs.latitude == pytest.approx(38.9498, abs=1e-3)
    # Country comes from the explicit column (WSC/USGS ids aren't prefix-distinct).
    assert next(s for s in stations if s.native_id == "05BB001").country_code == "CA"


@pytest.mark.asyncio
async def test_no_data_dir_returns_empty():
    async with CAMELSSPATConnector(config={"auto_download": False}) as conn:
        chunk = await conn.fetch_observations(
            "camels_spat:01646500",
            start=datetime(2000, 1, 1, tzinfo=UTC),
            end=datetime(2000, 1, 10, tzinfo=UTC),
        )
    assert chunk.observations == []
