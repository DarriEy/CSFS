"""Tests for the CAMELS-FR connector (comment-header CSV + EPSG:27572 gpkg)."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from csfs.connectors.camels_fr import CAMELSFRConnector

# Real-shape: a '#'-comment block, then a ';'-separated header. tsd_q_l is L/s.
SAMPLE_TS = (
    "# Station code                            : A105003001\n"
    "# Hydrological data producer              : DREAL Grand Est\n"
    "# Hydrometeorological model chain producer: Meteo-France\n"
    "tsd_date;tsd_q_l;tsd_q_mm;tsd_prec;tsd_temp\n"
    "19700101;1650;0.599;0.1;-4.8\n"
    "19700102;1730;0.628;0;-4.8\n"
    "19700103;;;0;-3.5\n"      # blank discharge -> missing
    "19700104;-1;0.0;0;-1.5\n"  # negative -> missing
)


def _ts_dir(tmp_path: Path) -> Path:
    d = tmp_path / "CAMELS_FR_time_series" / "daily"
    d.mkdir(parents=True)
    (d / "CAMELS_FR_tsd_A105003001.csv").write_text(SAMPLE_TS, encoding="utf-8")
    return tmp_path


@pytest.mark.asyncio
async def test_fetch_observations_skips_comment_block_and_converts_lps(tmp_path: Path):
    _ts_dir(tmp_path)
    async with CAMELSFRConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_fr:A105003001",
            start=datetime(1970, 1, 1, tzinfo=UTC),
            end=datetime(1970, 1, 10, tzinfo=UTC),
        )
    assert chunk.provider == "camels_fr"
    assert len(chunk.observations) == 4
    # 1650 L/s -> 1.65 m3/s
    assert chunk.observations[0].discharge_m3s == pytest.approx(1.65)
    assert chunk.observations[1].discharge_m3s == pytest.approx(1.73)
    assert chunk.observations[2].discharge_m3s is None  # blank
    assert chunk.observations[2].quality.value == "missing"
    assert chunk.observations[3].discharge_m3s is None  # negative sentinel
    assert chunk.observations[3].quality.value == "missing"


@pytest.mark.asyncio
async def test_fetch_observations_missing_file_empty(tmp_path: Path):
    async with CAMELSFRConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "camels_fr:Z999999999",
            start=datetime(1970, 1, 1, tzinfo=UTC),
            end=datetime(1970, 1, 10, tzinfo=UTC),
        )
    assert chunk.observations == []


@pytest.mark.asyncio
async def test_fetch_stations_reprojects_lambert_ii_to_wgs84(tmp_path: Path):
    fiona = pytest.importorskip("fiona")
    pytest.importorskip("pyproj")

    schema = {"geometry": "Point", "properties": {"sta_code_h3": "str", "sta_code_h2": "str"}}
    gpkg = tmp_path / "CAMELS_FR_gauge_outlet.gpkg"
    # A Lambert-II (EPSG:27572) easting/northing near the Alsace gauge A105003001.
    with fiona.open(
        str(gpkg), "w", driver="GPKG", layer="CAMELS_FR_gauge_outlet",
        crs="EPSG:27572", schema=schema,
    ) as dst:
        dst.write({
            "geometry": {"type": "Point", "coordinates": (998000.0, 2380000.0)},
            "properties": {"sta_code_h3": "A105003001", "sta_code_h2": "A1050310"},
        })

    async with CAMELSFRConnector(config={"data_dir": str(tmp_path)}) as conn:
        stations = await conn.fetch_stations()
    assert len(stations) == 1
    s = stations[0]
    assert s.id == "camels_fr:A105003001"
    assert s.native_id == "A105003001"
    assert s.country_code == "FR"
    # Reprojected into France's WGS84 envelope (not the raw projected metres).
    assert 41.0 < s.latitude < 52.0
    assert -5.0 < s.longitude < 10.0
