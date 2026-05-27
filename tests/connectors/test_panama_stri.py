"""Tests for the Panama STRI (ACP discharge) connector."""

from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from csfs.connectors.panama_stri import (
    _SEED_STATIONS,
    PanamaSTRIConnector,
)

# ------------------------------------------------------------------
# Mock data
# ------------------------------------------------------------------

SAMPLE_CSV = (
    "datetime,station_id,discharge\n"
    "2023-01-01 00:00:00,CHA,45.3\n"
    "2023-01-01 00:15:00,CHA,46.1\n"
    "2023-01-01 00:30:00,CHA,44.8\n"
    "2023-01-01 00:00:00,GAT,120.5\n"
    "2023-01-02 00:00:00,CHA,48.2\n"
)

SAMPLE_CSV_NO_STATION = (
    "datetime,discharge\n"
    "2023-01-01 00:00:00,45.3\n"
    "2023-01-01 00:15:00,46.1\n"
    "2023-01-02 00:00:00,48.2\n"
)


def _build_zip(csv_content: str) -> bytes:
    """Build a minimal ZIP archive containing a CSV file."""
    import io
    import zipfile

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("acp_discharge_15min.csv", csv_content)
    return buf.getvalue()


# ------------------------------------------------------------------
# Station listing tests
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_stations_seed_default():
    """Default mode returns the curated seed catalogue (no network)."""
    async with PanamaSTRIConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    first = stations[0]
    assert first.provider == "panama_stri"
    assert first.id.startswith("panama_stri:")
    assert first.country_code == "PA"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_remote_success():
    """When seed_only=False, connector verifies ZIP availability."""
    zip_bytes = _build_zip(SAMPLE_CSV)
    url = (
        "https://biogeodb.stri.si.edu/physical_monitoring"
        "/downloads/acp_discharge_15min.zip"
    )
    respx.get(url).mock(
        return_value=httpx.Response(200, content=zip_bytes),
    )

    async with PanamaSTRIConnector(
        config={"seed_only": False},
    ) as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_remote_error_falls_back():
    """If remote fails, connector falls back to seed list."""
    url = (
        "https://biogeodb.stri.si.edu/physical_monitoring"
        "/downloads/acp_discharge_15min.zip"
    )
    respx.get(url).mock(
        return_value=httpx.Response(500),
    )

    async with PanamaSTRIConnector(
        config={"seed_only": False},
    ) as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)


# ------------------------------------------------------------------
# Observation tests -- local CSV files
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_from_local_csv(tmp_path: Path):
    """Parse a local CSV with station_id column filtering."""
    csv_file = tmp_path / "CHA.csv"
    csv_file.write_text(SAMPLE_CSV, encoding="utf-8")

    async with PanamaSTRIConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "panama_stri:CHA",
            start=datetime(2023, 1, 1, tzinfo=UTC),
            end=datetime(2023, 1, 2, tzinfo=UTC),
        )

    assert chunk.station_id == "panama_stri:CHA"
    assert chunk.provider == "panama_stri"
    # 3 CHA rows on Jan 1 + 1 CHA row on Jan 2 = 4
    assert len(chunk.observations) == 4
    assert chunk.observations[0].discharge_m3s == pytest.approx(45.3)


@pytest.mark.asyncio
async def test_fetch_observations_no_data_dir_no_network():
    """Without data_dir and no network, returns empty chunk."""
    async with PanamaSTRIConnector() as conn:
        chunk = await conn.fetch_observations(
            "panama_stri:CHA",
            start=datetime(2023, 1, 1, tzinfo=UTC),
            end=datetime(2023, 1, 2, tzinfo=UTC),
        )

    assert chunk.station_id == "panama_stri:CHA"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_from_zip_download():
    """When no local files, connector downloads ZIP from STRI."""
    zip_bytes = _build_zip(SAMPLE_CSV_NO_STATION)
    url = (
        "https://biogeodb.stri.si.edu/physical_monitoring"
        "/downloads/acp_discharge_15min.zip"
    )
    respx.get(url).mock(
        return_value=httpx.Response(200, content=zip_bytes),
    )

    async with PanamaSTRIConnector() as conn:
        chunk = await conn.fetch_observations(
            "panama_stri:CHA",
            start=datetime(2023, 1, 1, tzinfo=UTC),
            end=datetime(2023, 1, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(45.3)


@pytest.mark.asyncio
async def test_fetch_observations_date_filtering(tmp_path: Path):
    """Only observations within [start, end] are returned."""
    csv_file = tmp_path / "CHA.csv"
    csv_file.write_text(SAMPLE_CSV_NO_STATION, encoding="utf-8")

    async with PanamaSTRIConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "panama_stri:CHA",
            start=datetime(2023, 1, 1, tzinfo=UTC),
            end=datetime(2023, 1, 1, 0, 20, tzinfo=UTC),
        )

    # Only rows at 00:00 and 00:15 are within range
    assert len(chunk.observations) == 2


@pytest.mark.asyncio
async def test_seed_station_ids_are_canonical():
    """Every seed station has a properly formatted CSFS station ID."""
    async with PanamaSTRIConnector() as conn:
        stations = await conn.fetch_stations()

    for station in stations:
        assert station.id == f"panama_stri:{station.native_id}"
        assert station.provider == "panama_stri"
        assert station.country_code == "PA"
        assert 9.0 <= station.latitude <= 9.5
        assert -80.1 <= station.longitude <= -79.5


@pytest.mark.asyncio
async def test_connector_registration():
    """The connector is registered under the 'panama_stri' slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("panama_stri")
    assert cls is PanamaSTRIConnector


# ------------------------------------------------------------------
# Additional coverage tests — error branches, edge cases
# ------------------------------------------------------------------


def test_safe_float_non_numeric_returns_none():
    """_safe_float returns None for non-numeric values (lines 153, 156-157)."""
    from csfs.connectors.panama_stri import _safe_float

    assert _safe_float(None) is None
    assert _safe_float("not_a_number") is None
    assert _safe_float("") is None


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_download_fails_returns_empty():
    """When ZIP download fails, returns empty chunk (lines 240-252)."""
    url = (
        "https://biogeodb.stri.si.edu/physical_monitoring"
        "/downloads/acp_discharge_15min.zip"
    )
    respx.get(url).mock(
        side_effect=httpx.ConnectError("connection refused"),
    )

    async with PanamaSTRIConnector() as conn:
        chunk = await conn.fetch_observations(
            "panama_stri:CHA",
            start=datetime(2023, 1, 1, tzinfo=UTC),
            end=datetime(2023, 1, 2, tzinfo=UTC),
        )

    assert chunk.station_id == "panama_stri:CHA"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_remote_non_200_raises():
    """When remote ZIP returns non-200, falls back to seed (line 267)."""
    url = (
        "https://biogeodb.stri.si.edu/physical_monitoring"
        "/downloads/acp_discharge_15min.zip"
    )
    respx.get(url).mock(
        return_value=httpx.Response(404),
    )

    async with PanamaSTRIConnector(
        config={"seed_only": False},
    ) as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_from_zip_bad_zip_raises():
    """Invalid ZIP content raises ConnectorError (lines 334-335)."""
    from csfs.core.exceptions import ConnectorError

    url = (
        "https://biogeodb.stri.si.edu/physical_monitoring"
        "/downloads/acp_discharge_15min.zip"
    )
    respx.get(url).mock(
        return_value=httpx.Response(200, content=b"not a zip file"),
    )

    async with PanamaSTRIConnector() as conn:
        with pytest.raises(ConnectorError, match="Invalid ZIP"):
            await conn._fetch_from_zip(
                "panama_stri:CHA", "CHA",
                datetime(2023, 1, 1, tzinfo=UTC),
                datetime(2023, 1, 2, tzinfo=UTC),
            )


def test_find_data_file_not_found(tmp_path: Path):
    """_find_data_file returns None when no matching file (line 364)."""
    conn = PanamaSTRIConnector()
    result = conn._find_data_file(tmp_path, "NONEXISTENT")
    assert result is None


def test_find_data_file_acp_prefix(tmp_path: Path):
    """_find_data_file finds acp_discharge_{id}.csv pattern."""
    csv_file = tmp_path / "acp_discharge_CHA.csv"
    csv_file.write_text("datetime,discharge\n")
    conn = PanamaSTRIConnector()
    result = conn._find_data_file(tmp_path, "CHA")
    assert result == csv_file


@pytest.mark.asyncio
async def test_load_csv_file_oserror_raises(tmp_path: Path):
    """_load_csv_file raises ConnectorError on OSError (lines 377-378)."""
    import os

    from csfs.core.exceptions import ConnectorError

    csv_file = tmp_path / "CHA.csv"
    csv_file.write_text("datetime,discharge\n")
    os.chmod(csv_file, 0o000)

    try:
        async with PanamaSTRIConnector() as conn:
            with pytest.raises(ConnectorError, match="Cannot read file"):
                conn._load_csv_file(
                    csv_file, "panama_stri:CHA", "CHA",
                    datetime(2023, 1, 1, tzinfo=UTC),
                    datetime(2023, 1, 2, tzinfo=UTC),
                )
    finally:
        os.chmod(csv_file, 0o644)


def test_parse_csv_text_no_header_returns_empty():
    """CSV with no header returns empty list (line 430)."""
    conn = PanamaSTRIConnector()
    result = conn._parse_csv_text(
        "", "panama_stri:CHA", "CHA",
        datetime(2023, 1, 1, tzinfo=UTC),
        datetime(2023, 1, 2, tzinfo=UTC),
    )
    assert result == []


def test_parse_csv_text_missing_columns_returns_empty():
    """CSV without datetime or value columns returns empty (line 486)."""
    csv_text = "station_id,other_field\nCHA,something\n"
    conn = PanamaSTRIConnector()
    result = conn._parse_csv_text(
        csv_text, "panama_stri:CHA", "CHA",
        datetime(2023, 1, 1, tzinfo=UTC),
        datetime(2023, 1, 2, tzinfo=UTC),
    )
    assert result == []


def test_parse_row_empty_date_returns_none():
    """Row with empty date returns None (line 490)."""
    conn = PanamaSTRIConnector()
    row = {"datetime": "", "discharge": "45.0"}
    result = conn._parse_row(
        row, "datetime", None, "discharge",
        "panama_stri:CHA", "CHA",
        datetime(2023, 1, 1, tzinfo=UTC),
        datetime(2023, 1, 2, tzinfo=UTC),
    )
    assert result is None


def test_parse_row_unparseable_date_returns_none():
    """Row with unparseable date returns None (line 503)."""
    conn = PanamaSTRIConnector()
    row = {"datetime": "not-a-date", "discharge": "45.0"}
    result = conn._parse_row(
        row, "datetime", None, "discharge",
        "panama_stri:CHA", "CHA",
        datetime(2023, 1, 1, tzinfo=UTC),
        datetime(2023, 1, 2, tzinfo=UTC),
    )
    assert result is None


def test_parse_row_non_numeric_discharge_is_missing():
    """Non-numeric discharge value sets MISSING quality (line 503)."""
    from csfs.core.models import QualityFlag

    conn = PanamaSTRIConnector()
    row = {"datetime": "2023-01-01 00:00:00", "discharge": "bad_value"}
    result = conn._parse_row(
        row, "datetime", None, "discharge",
        "panama_stri:CHA", "CHA",
        datetime(2023, 1, 1, tzinfo=UTC),
        datetime(2023, 1, 2, tzinfo=UTC),
    )
    assert result is not None
    assert result.discharge_m3s is None
    assert result.quality == QualityFlag.MISSING


def test_parse_timestamp_multiple_formats():
    """_parse_timestamp handles all supported formats (lines 527-529)."""
    # Format: YYYY-MM-DD HH:MM:SS
    result = PanamaSTRIConnector._parse_timestamp("2023-01-01 00:00:00")
    assert result is not None

    # Format: YYYY-MM-DD HH:MM
    result = PanamaSTRIConnector._parse_timestamp("2023-01-01 00:00")
    assert result is not None

    # Format: YYYY-MM-DD
    result = PanamaSTRIConnector._parse_timestamp("2023-01-01")
    assert result is not None

    # Format: YYYY/MM/DD HH:MM:SS
    result = PanamaSTRIConnector._parse_timestamp("2023/01/01 00:00:00")
    assert result is not None

    # Format: YYYY/MM/DD HH:MM
    result = PanamaSTRIConnector._parse_timestamp("2023/01/01 00:00")
    assert result is not None

    # None for unrecognized format
    result = PanamaSTRIConnector._parse_timestamp("01-Jan-2023")
    assert result is None


def test_empty_chunk():
    """_empty_chunk returns an empty TimeSeriesChunk (line 564)."""
    conn = PanamaSTRIConnector()
    chunk = conn._empty_chunk("panama_stri:CHA")
    assert chunk.station_id == "panama_stri:CHA"
    assert chunk.provider == "panama_stri"
    assert len(chunk.observations) == 0
