"""Tests for GRDC connector with file-based parsing and respx mocks."""

from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from csfs.connectors.grdc import (
    _SEED_STATIONS,
    GRDCConnector,
)

# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SAMPLE_GRDC_FILE = """\
# GRDC STATION DATA FILE
# File generated: 2024-01-15
#
# Station:   6340110
# River:     RHINE
# Country:   DE
# Latitude:  51.84
# Longitude: 6.11
# Catchment area (km2): 160800.0
# Unit: m3/s
# Time series: 1901 - 2022
#
# Missing value: -999.000
#
# This is the GRDC station data file.
# Contact: grdc@bafg.de
# Website: https://grdc.bafg.de
#
# Data source: Federal Waterways and Shipping Administration
#
# Flags:
# 0 = original value
# 1 = estimated
# 2 = suspect
# 3 = missing
#
#
#
#
#
#
#
#
#
#
#
YYYY-MM-DD;hh:mm; Original; Flag
1950-01-01; 00:00;    620.000;0
1950-01-02; 00:00;    635.500;0
1950-01-03; 00:00;   -999.000;
1950-01-04; 00:00;    610.200;1
1950-01-05; 00:00;    598.000;2
"""

SAMPLE_GRDC_FILE_NO_HEADER_ROW = """\
# GRDC STATION DATA FILE
# Station: 6340110
1950-01-01; 00:00;    620.000;0
1950-01-02; 00:00;    635.500;0
"""

MOCK_WFS_RESPONSE = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [6.11, 51.84],
            },
            "properties": {
                "grdc_no": "6340110",
                "station": "RHINE AT LOBITH",
                "country_code": "DE",
                "river": "RHINE",
                "area": 160800.0,
            },
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [28.27, 45.45],
            },
            "properties": {
                "grdc_no": "6935051",
                "station": "DANUBE AT RENI",
                "country_code": "UA",
                "river": "DANUBE",
                "area": 805700.0,
            },
        },
    ],
}

MOCK_WFS_EMPTY = {
    "type": "FeatureCollection",
    "features": [],
}


# ---------------------------------------------------------------------------
# Station listing tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_stations_seed_default():
    """Default mode returns the curated seed catalogue (no network)."""
    async with GRDCConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    first = stations[0]
    assert first.provider == "grdc"
    assert first.id.startswith("grdc:")
    assert first.country_code in (
        "DE", "UA", "LV", "RU", "MK", "BY", "MD", "RS",
        "TR", "EG", "NG", "CD", "ZM", "SD", "GH", "KH",
        "BD", "PK", "MM", "TM",
    )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_wfs_success():
    """When seed_only=False, connector fetches from WFS endpoint."""
    respx.get("https://grdc.bafg.de/GRDC/ows").mock(
        return_value=httpx.Response(200, json=MOCK_WFS_RESPONSE),
    )

    async with GRDCConnector(config={"seed_only": False}) as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    rhine = stations[0]
    assert rhine.native_id == "6340110"
    assert rhine.name == "RHINE AT LOBITH"
    assert rhine.latitude == pytest.approx(51.84)
    assert rhine.longitude == pytest.approx(6.11)
    assert rhine.catchment_area_km2 == pytest.approx(160800.0)
    assert rhine.river == "RHINE"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_wfs_fallback_to_seed():
    """If WFS fails, connector falls back to seed list."""
    respx.get("https://grdc.bafg.de/GRDC/ows").mock(
        return_value=httpx.Response(500),
    )

    async with GRDCConnector(config={"seed_only": False}) as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)


# ---------------------------------------------------------------------------
# Observation / file-parsing tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_no_data_dir():
    """Without data_dir configured, returns empty chunk with guidance."""
    async with GRDCConnector() as conn:
        chunk = await conn.fetch_observations(
            "grdc:6340110",
            start=datetime(1950, 1, 1, tzinfo=UTC),
            end=datetime(1950, 1, 5, tzinfo=UTC),
        )

    assert chunk.station_id == "grdc:6340110"
    assert chunk.provider == "grdc"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_file_not_found(tmp_path: Path):
    """When data_dir exists but file is missing, returns empty chunk."""
    async with GRDCConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "grdc:9999999",
            start=datetime(1950, 1, 1, tzinfo=UTC),
            end=datetime(1950, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_parses_file(tmp_path: Path):
    """Full parse of a GRDC text file with header, data, and flags."""
    grdc_file = tmp_path / "6340110_Q_Day.Cmd.txt"
    grdc_file.write_text(SAMPLE_GRDC_FILE, encoding="utf-8")

    async with GRDCConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "grdc:6340110",
            start=datetime(1950, 1, 1, tzinfo=UTC),
            end=datetime(1950, 1, 5, tzinfo=UTC),
        )

    assert chunk.station_id == "grdc:6340110"
    assert chunk.provider == "grdc"
    assert len(chunk.observations) == 5

    # First obs: original value, good quality
    assert chunk.observations[0].discharge_m3s == pytest.approx(620.0)
    assert chunk.observations[0].quality.value == "good"

    # Second obs: original value
    assert chunk.observations[1].discharge_m3s == pytest.approx(635.5)
    assert chunk.observations[1].quality.value == "good"

    # Third obs: -999.0 -> missing
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"

    # Fourth obs: estimated flag
    assert chunk.observations[3].discharge_m3s == pytest.approx(610.2)
    assert chunk.observations[3].quality.value == "estimated"

    # Fifth obs: suspect flag
    assert chunk.observations[4].discharge_m3s == pytest.approx(598.0)
    assert chunk.observations[4].quality.value == "suspect"


@pytest.mark.asyncio
async def test_fetch_observations_date_filtering(tmp_path: Path):
    """Only observations within [start, end] are returned."""
    grdc_file = tmp_path / "6340110_Q_Day.Cmd.txt"
    grdc_file.write_text(SAMPLE_GRDC_FILE, encoding="utf-8")

    async with GRDCConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "grdc:6340110",
            start=datetime(1950, 1, 2, tzinfo=UTC),
            end=datetime(1950, 1, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    dates = [obs.timestamp.day for obs in chunk.observations]
    assert dates == [2, 3]


@pytest.mark.asyncio
async def test_fetch_observations_alt_filename(tmp_path: Path):
    """Connector finds files with the plain {grdc_no}.txt naming."""
    grdc_file = tmp_path / "6340110.txt"
    grdc_file.write_text(
        SAMPLE_GRDC_FILE_NO_HEADER_ROW, encoding="utf-8",
    )

    async with GRDCConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "grdc:6340110",
            start=datetime(1950, 1, 1, tzinfo=UTC),
            end=datetime(1950, 1, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(620.0)


@pytest.mark.asyncio
async def test_seed_station_ids_are_canonical():
    """Every seed station has a properly formatted CSFS station ID."""
    async with GRDCConnector() as conn:
        stations = await conn.fetch_stations()

    for station in stations:
        assert station.id == f"grdc:{station.native_id}"
        assert station.provider == "grdc"
        assert station.latitude != 0.0 or station.longitude != 0.0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_wfs_empty():
    """WFS returning no features produces an empty list."""
    respx.get("https://grdc.bafg.de/GRDC/ows").mock(
        return_value=httpx.Response(200, json=MOCK_WFS_EMPTY),
    )

    async with GRDCConnector(config={"seed_only": False}) as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


# ---------------------------------------------------------------------------
# Coverage gap tests — WFS feature parsing edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_wfs_missing_grdc_no():
    """WFS features without grdc_no or short coords are skipped."""
    response = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [6.11, 51.84]},
                "properties": {
                    "grdc_no": "",  # empty grdc_no
                    "station": "No ID",
                    "country_code": "DE",
                },
            },
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [6.11]},  # short coords
                "properties": {
                    "grdc_no": "1234567",
                    "station": "Short Coords",
                    "country_code": "DE",
                },
            },
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [6.11, 51.84]},
                "properties": {
                    "grdc_no": "6340110",
                    "station": "RHINE AT LOBITH",
                    "country_code": "DE",
                    "river": "RHINE",
                    "area": 160800.0,
                },
            },
        ],
    }
    respx.get("https://grdc.bafg.de/GRDC/ows").mock(
        return_value=httpx.Response(200, json=response),
    )

    async with GRDCConnector(config={"seed_only": False}) as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "6340110"


# ---------------------------------------------------------------------------
# Coverage gap tests — file read error
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_file_read_error(tmp_path: Path):
    """OSError when reading a GRDC file raises ConnectorError."""
    from csfs.core.exceptions import ConnectorError

    grdc_file = tmp_path / "6340110_Q_Day.Cmd.txt"
    grdc_file.write_text(SAMPLE_GRDC_FILE, encoding="utf-8")
    grdc_file.chmod(0o000)

    try:
        async with GRDCConnector(
            config={"data_dir": str(tmp_path)},
        ) as conn:
            with pytest.raises(ConnectorError, match="Cannot read GRDC file"):
                await conn.fetch_observations(
                    "grdc:6340110",
                    start=datetime(1950, 1, 1, tzinfo=UTC),
                    end=datetime(1950, 1, 5, tzinfo=UTC),
                )
    finally:
        grdc_file.chmod(0o644)


# ---------------------------------------------------------------------------
# Coverage gap tests — empty data lines
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_only_comments(tmp_path: Path):
    """A file with only comment lines returns empty observations."""
    grdc_file = tmp_path / "6340110_Q_Day.Cmd.txt"
    grdc_file.write_text(
        "# Only comments\n# No data\n\n",
        encoding="utf-8",
    )

    async with GRDCConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "grdc:6340110",
            start=datetime(1950, 1, 1, tzinfo=UTC),
            end=datetime(1950, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


# ---------------------------------------------------------------------------
# Coverage gap tests — short data line (< 3 parts)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_short_lines_skipped(tmp_path: Path):
    """Data lines with fewer than 3 semicolons are skipped."""
    grdc_file = tmp_path / "6340110_Q_Day.Cmd.txt"
    grdc_file.write_text(
        "# Header\n"
        "1950-01-01;00:00\n"  # only 2 parts
        "1950-01-02; 00:00;    620.000;0\n",
        encoding="utf-8",
    )

    async with GRDCConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "grdc:6340110",
            start=datetime(1950, 1, 1, tzinfo=UTC),
            end=datetime(1950, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(620.0)


# ---------------------------------------------------------------------------
# Coverage gap tests — invalid date in data line
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_bad_date_skipped(tmp_path: Path):
    """Data lines with unparseable dates are skipped."""
    grdc_file = tmp_path / "6340110_Q_Day.Cmd.txt"
    grdc_file.write_text(
        "# Header\n"
        "bad-date; 00:00;    620.000;0\n"
        "1950-01-02; 00:00;    635.500;0\n",
        encoding="utf-8",
    )

    async with GRDCConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "grdc:6340110",
            start=datetime(1950, 1, 1, tzinfo=UTC),
            end=datetime(1950, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(635.5)


# ---------------------------------------------------------------------------
# Coverage gap tests — non-numeric value string
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_non_numeric_value(tmp_path: Path):
    """Non-numeric value strings result in MISSING quality."""
    grdc_file = tmp_path / "6340110_Q_Day.Cmd.txt"
    grdc_file.write_text(
        "# Header\n"
        "1950-01-01; 00:00;    abc;0\n",
        encoding="utf-8",
    )

    async with GRDCConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "grdc:6340110",
            start=datetime(1950, 1, 1, tzinfo=UTC),
            end=datetime(1950, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[0].quality.value == "missing"
