"""Tests for the Iceland LamaH-Ice connector (local file-based)."""

from datetime import UTC, datetime
from pathlib import Path

import pytest

from csfs.connectors.iceland_lamahice import IcelandLamahIceConnector, _safe_float

# Real Gauge_attributes.csv schema: semicolon-delimited, with lat/lon as ISN93
# (EPSG:3057) northing/easting in metres — converted to WGS84 by the connector.
SAMPLE_ATTRIBUTES = """\
id;V_no;name;river;elevation;lat;lon;geometry
1;V503;Olfusa gauge;Olfusa;8;451317;370531;POINT (370531 451317)
7;V510;Jokulsa gauge;Jokulsa;120;489682.742;516630.196;POINT (516630.196 489682.742)
"""

# LamaH-style daily discharge: separate YYYY;MM;DD columns + qobs.
SAMPLE_DAILY_YMD = """\
YYYY;MM;DD;qobs
1990;01;01;120.5
1990;01;02;118.2
1990;01;03;-999
1990;01;04;125.0
"""

# Alternate layout: a single date column + discharge.
SAMPLE_DAILY_DATE = """\
date,discharge
1990-01-01,120.5
1990-01-02,118.2
"""


def _make_dataset(root: Path) -> None:
    """Create a minimal LamaH-Ice-style extracted tree under root."""
    attr_dir = root / "lamah_ice" / "D_gauges" / "1_attributes"
    attr_dir.mkdir(parents=True)
    (attr_dir / "Gauge_attributes.csv").write_text(SAMPLE_ATTRIBUTES, encoding="utf-8")

    daily_dir = root / "lamah_ice" / "D_gauges" / "2_timeseries" / "daily"
    daily_dir.mkdir(parents=True)
    (daily_dir / "ID_1.csv").write_text(SAMPLE_DAILY_YMD, encoding="utf-8")
    (daily_dir / "ID_7.csv").write_text(SAMPLE_DAILY_DATE, encoding="utf-8")


# ---------------------------------------------------------------------------
# Station listing
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_stations_from_attributes(tmp_path: Path):
    """Stations are parsed from the gauge-attributes table (real IDs/coords)."""
    _make_dataset(tmp_path)
    async with IcelandLamahIceConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    s = next(s for s in stations if s.native_id == "1")
    assert s.id == "iceland_lamahice:1"
    assert s.country_code == "IS"
    assert s.river == "Olfusa"
    # ISN93 (370531 E, 451317 N) -> WGS84, within Iceland and at the known point.
    assert s.latitude == pytest.approx(64.5385, abs=1e-3)
    assert s.longitude == pytest.approx(-21.6989, abs=1e-3)
    assert s.elevation_m == pytest.approx(8.0)


@pytest.mark.asyncio
async def test_fetch_stations_no_data_returns_empty():
    """With auto-download disabled and no data_dir, no stations (no fabrication)."""
    async with IcelandLamahIceConnector(
        config={"auto_download": False},
    ) as conn:
        stations = await conn.fetch_stations()
    assert stations == []


# ---------------------------------------------------------------------------
# Observations
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_ymd_columns(tmp_path: Path):
    """Daily CSV with separate YYYY/MM/DD columns parses, -999 -> missing."""
    _make_dataset(tmp_path)
    async with IcelandLamahIceConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "iceland_lamahice:1",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 5, tzinfo=UTC),
        )

    assert chunk.provider == "iceland_lamahice"
    assert len(chunk.observations) == 4
    assert chunk.observations[0].discharge_m3s == pytest.approx(120.5)
    assert chunk.observations[0].quality.value == "raw"
    # -999 sentinel -> missing
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
async def test_fetch_observations_single_date_column(tmp_path: Path):
    """Daily CSV with a single date column + discharge parses too."""
    _make_dataset(tmp_path)
    async with IcelandLamahIceConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "iceland_lamahice:7",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[1].discharge_m3s == pytest.approx(118.2)


@pytest.mark.asyncio
async def test_fetch_observations_date_filtering(tmp_path: Path):
    """Only observations within [start, end] are returned."""
    _make_dataset(tmp_path)
    async with IcelandLamahIceConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "iceland_lamahice:1",
            start=datetime(1990, 1, 2, tzinfo=UTC),
            end=datetime(1990, 1, 3, tzinfo=UTC),
        )

    assert [o.timestamp.day for o in chunk.observations] == [2, 3]


@pytest.mark.asyncio
async def test_fetch_observations_file_not_found(tmp_path: Path):
    """A gauge with no CSV returns an empty chunk."""
    _make_dataset(tmp_path)
    async with IcelandLamahIceConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "iceland_lamahice:9999",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 5, tzinfo=UTC),
        )
    assert chunk.observations == []


@pytest.mark.asyncio
async def test_fetch_observations_no_data_returns_empty():
    """Auto-download disabled + no data_dir -> empty chunk."""
    async with IcelandLamahIceConnector(
        config={"auto_download": False},
    ) as conn:
        chunk = await conn.fetch_observations(
            "iceland_lamahice:1",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 5, tzinfo=UTC),
        )
    assert chunk.observations == []


def test_connector_registration():
    """Connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    assert get_connector("iceland_lamahice") is IcelandLamahIceConnector


def test_safe_float_edge_cases():
    """Module-level _safe_float handles edge cases and NA sentinels."""
    assert _safe_float(None) is None
    assert _safe_float("abc") is None
    assert _safe_float("na") is None
    assert _safe_float("-") is None
    assert _safe_float("123.4") == pytest.approx(123.4)
