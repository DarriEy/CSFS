"""Tests for the CA-discharge (Central Asian Discharge) connector."""

from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from csfs.connectors.ca_discharge import (
    _SEED_STATIONS,
    CADischargeConnector,
)

# ------------------------------------------------------------------
# Mock data
# ------------------------------------------------------------------

MOCK_ZENODO_RECORD = {
    "id": 7743778,
    "metadata": {
        "title": "Central Asian discharge dataset",
    },
    "files": [
        {
            "key": "ca_discharge.gpkg",
            "links": {
                "self": "https://zenodo.org/api/records/7743778/files/ca_discharge.gpkg/content",
            },
        },
    ],
}

SAMPLE_CSV = (
    "date,discharge_m3s\n"
    "1960-01-01,120.5\n"
    "1960-01-02,135.2\n"
    "1960-01-03,-999.0\n"
    "1960-01-04,110.8\n"
    "1960-01-05,98.3\n"
)

SAMPLE_CSV_ALT_COLUMNS = (
    "date,value\n"
    "1970-06-01,250.0\n"
    "1970-06-02,270.5\n"
)


# ------------------------------------------------------------------
# Station listing tests
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_stations_seed_default():
    """Default mode returns the curated seed catalogue (no network)."""
    async with CADischargeConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    first = stations[0]
    assert first.provider == "ca_discharge"
    assert first.id.startswith("ca_discharge:")
    assert first.country_code in ("KG", "TJ", "KZ", "UZ", "AF")


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_zenodo_verification():
    """When seed_only=False, connector verifies Zenodo record."""
    respx.get("https://zenodo.org/api/records/7743778").mock(
        return_value=httpx.Response(
            200, json=MOCK_ZENODO_RECORD,
        ),
    )

    async with CADischargeConnector(
        config={"seed_only": False},
    ) as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_zenodo_unreachable_falls_back():
    """If Zenodo is unreachable, connector falls back to seed list."""
    respx.get("https://zenodo.org/api/records/7743778").mock(
        return_value=httpx.Response(500),
    )

    async with CADischargeConnector(
        config={"seed_only": False},
    ) as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)


# ------------------------------------------------------------------
# Observation / file-parsing tests
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_no_data_dir():
    """Without data_dir, returns empty chunk with guidance."""
    async with CADischargeConnector() as conn:
        chunk = await conn.fetch_observations(
            "ca_discharge:CA001",
            start=datetime(1960, 1, 1, tzinfo=UTC),
            end=datetime(1960, 1, 5, tzinfo=UTC),
        )

    assert chunk.station_id == "ca_discharge:CA001"
    assert chunk.provider == "ca_discharge"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_file_not_found(tmp_path: Path):
    """When data_dir exists but file is missing, returns empty."""
    async with CADischargeConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "ca_discharge:CA999",
            start=datetime(1960, 1, 1, tzinfo=UTC),
            end=datetime(1960, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_parses_csv(tmp_path: Path):
    """Full parse of a CSV file with date and discharge."""
    csv_file = tmp_path / "CA001.csv"
    csv_file.write_text(SAMPLE_CSV, encoding="utf-8")

    async with CADischargeConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "ca_discharge:CA001",
            start=datetime(1960, 1, 1, tzinfo=UTC),
            end=datetime(1960, 1, 5, tzinfo=UTC),
        )

    assert chunk.station_id == "ca_discharge:CA001"
    assert chunk.provider == "ca_discharge"
    assert len(chunk.observations) == 5

    # Normal value
    assert chunk.observations[0].discharge_m3s == pytest.approx(
        120.5,
    )
    assert chunk.observations[0].quality.value == "raw"

    # Missing value (-999.0)
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
async def test_fetch_observations_date_filtering(tmp_path: Path):
    """Only observations within [start, end] are returned."""
    csv_file = tmp_path / "CA001.csv"
    csv_file.write_text(SAMPLE_CSV, encoding="utf-8")

    async with CADischargeConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "ca_discharge:CA001",
            start=datetime(1960, 1, 2, tzinfo=UTC),
            end=datetime(1960, 1, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    dates = [obs.timestamp.day for obs in chunk.observations]
    assert dates == [2, 3]


@pytest.mark.asyncio
async def test_seed_station_ids_are_canonical():
    """Every seed station has a properly formatted CSFS station ID."""
    async with CADischargeConnector() as conn:
        stations = await conn.fetch_stations()

    for station in stations:
        assert station.id == f"ca_discharge:{station.native_id}"
        assert station.provider == "ca_discharge"
        assert station.latitude != 0.0 or station.longitude != 0.0
        assert station.country_code in (
            "KG", "TJ", "KZ", "UZ", "AF",
        )


# ------------------------------------------------------------------
# Coverage gap tests — _safe_float edge cases
# ------------------------------------------------------------------


def test_safe_float_none():
    """_safe_float returns None for None input."""
    from csfs.connectors.ca_discharge import _safe_float

    assert _safe_float(None) is None


def test_safe_float_invalid_string():
    """_safe_float returns None for non-numeric strings."""
    from csfs.connectors.ca_discharge import _safe_float

    assert _safe_float("abc") is None
    assert _safe_float("") is None


# ------------------------------------------------------------------
# Coverage gap tests — CSV read error
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_file_read_error(tmp_path: Path):
    """OSError when reading CSV file raises ConnectorError."""
    from csfs.core.exceptions import ConnectorError

    csv_file = tmp_path / "CA001.csv"
    csv_file.write_text(SAMPLE_CSV, encoding="utf-8")
    csv_file.chmod(0o000)

    try:
        async with CADischargeConnector(
            config={"data_dir": str(tmp_path)},
        ) as conn:
            with pytest.raises(ConnectorError, match="Cannot read file"):
                await conn.fetch_observations(
                    "ca_discharge:CA001",
                    start=datetime(1960, 1, 1, tzinfo=UTC),
                    end=datetime(1960, 1, 5, tzinfo=UTC),
                )
    finally:
        csv_file.chmod(0o644)


# ------------------------------------------------------------------
# Coverage gap tests — CSV with no fieldnames
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_empty_csv(tmp_path: Path):
    """Empty CSV file (no fieldnames) returns zero observations."""
    csv_file = tmp_path / "CA001.csv"
    csv_file.write_text("", encoding="utf-8")

    async with CADischargeConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "ca_discharge:CA001",
            start=datetime(1960, 1, 1, tzinfo=UTC),
            end=datetime(1960, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


# ------------------------------------------------------------------
# Coverage gap tests — CSV with unrecognized columns
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_unrecognized_columns(tmp_path: Path):
    """CSV with unrecognized column names returns zero observations."""
    csv_file = tmp_path / "CA001.csv"
    csv_file.write_text(
        "timestamp,flow_rate\n1960-01-01,120.5\n",
        encoding="utf-8",
    )

    async with CADischargeConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "ca_discharge:CA001",
            start=datetime(1960, 1, 1, tzinfo=UTC),
            end=datetime(1960, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


# ------------------------------------------------------------------
# Coverage gap tests — CSV row with empty date
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_empty_date_row_skipped(tmp_path: Path):
    """CSV rows with empty date are skipped."""
    csv_file = tmp_path / "CA001.csv"
    csv_file.write_text(
        "date,discharge_m3s\n,120.5\n1960-01-01,135.2\n",
        encoding="utf-8",
    )

    async with CADischargeConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "ca_discharge:CA001",
            start=datetime(1960, 1, 1, tzinfo=UTC),
            end=datetime(1960, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(135.2)


# ------------------------------------------------------------------
# Coverage gap tests — CSV row with bad date format
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_bad_date_format_skipped(tmp_path: Path):
    """CSV rows with unparseable dates are skipped."""
    csv_file = tmp_path / "CA001.csv"
    csv_file.write_text(
        "date,discharge_m3s\nnot-a-date,120.5\n1960-01-01,135.2\n",
        encoding="utf-8",
    )

    async with CADischargeConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "ca_discharge:CA001",
            start=datetime(1960, 1, 1, tzinfo=UTC),
            end=datetime(1960, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1


# ------------------------------------------------------------------
# Coverage gap tests — CSV row with non-numeric value
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_non_numeric_value_missing(tmp_path: Path):
    """Non-numeric discharge values result in MISSING quality."""
    csv_file = tmp_path / "CA001.csv"
    csv_file.write_text(
        "date,discharge_m3s\n1960-01-01,abc\n",
        encoding="utf-8",
    )

    async with CADischargeConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "ca_discharge:CA001",
            start=datetime(1960, 1, 1, tzinfo=UTC),
            end=datetime(1960, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].quality.value == "missing"
