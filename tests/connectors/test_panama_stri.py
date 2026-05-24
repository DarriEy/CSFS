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
