"""Tests for the Caravan (global large-sample hydrology) connector."""

from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from csfs.connectors.caravan import (
    _SEED_STATIONS,
    CaravanConnector,
)

# ------------------------------------------------------------------
# Mock data
# ------------------------------------------------------------------

MOCK_ZENODO_RECORD = {
    "id": 7540792,
    "metadata": {
        "title": "Caravan - A global community dataset",
    },
    "files": [
        {
            "key": "caravan_us.zip",
            "links": {
                "self": "https://zenodo.org/api/records/7540792/files/caravan_us.zip/content",
            },
        },
        {
            "key": "caravan_gb.zip",
            "links": {
                "self": "https://zenodo.org/api/records/7540792/files/caravan_gb.zip/content",
            },
        },
    ],
}

MOCK_ZENODO_EMPTY = {
    "id": 7540792,
    "metadata": {"title": "Empty"},
    "files": [],
}

SAMPLE_CARAVAN_CSV = (
    "date,streamflow\n"
    "1990-01-01,15.3\n"
    "1990-01-02,14.8\n"
    "1990-01-03,16.1\n"
    "1990-01-04,18.5\n"
    "1990-01-05,20.2\n"
)

SAMPLE_CARAVAN_CSV_ALT = (
    "date,discharge\n"
    "2000-06-01,250.0\n"
    "2000-06-02,270.5\n"
)


# ------------------------------------------------------------------
# Station listing tests
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_stations_seed_default():
    """Default mode returns the curated seed catalogue."""
    async with CaravanConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    first = stations[0]
    assert first.provider == "caravan"
    assert first.id.startswith("caravan:")


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_zenodo_discovery():
    """When seed_only=False, connector discovers from Zenodo."""
    respx.get("https://zenodo.org/api/records/7540792").mock(
        return_value=httpx.Response(
            200, json=MOCK_ZENODO_RECORD,
        ),
    )

    async with CaravanConnector(
        config={"seed_only": False},
    ) as conn:
        stations = await conn.fetch_stations()

    # Falls back to seed after verifying Zenodo
    assert len(stations) == len(_SEED_STATIONS)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_zenodo_error_falls_back():
    """If Zenodo returns error, connector falls back to seed."""
    respx.get("https://zenodo.org/api/records/7540792").mock(
        return_value=httpx.Response(500),
    )

    async with CaravanConnector(
        config={"seed_only": False},
    ) as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_zenodo_empty_files_raises():
    """DataFormatError when Zenodo record has no files."""
    respx.get("https://zenodo.org/api/records/7540792").mock(
        return_value=httpx.Response(
            200, json=MOCK_ZENODO_EMPTY,
        ),
    )

    async with CaravanConnector(
        config={"seed_only": False},
    ) as conn:
        # Empty files raises DataFormatError, which triggers
        # fallback to seed in the outer handler
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)


# ------------------------------------------------------------------
# Observation / file-parsing tests
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_no_data_dir():
    """Without data_dir and auto-download disabled, returns empty chunk."""
    async with CaravanConnector(config={"auto_download": False}) as conn:
        chunk = await conn.fetch_observations(
            "caravan:camels_us_01013500",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 5, tzinfo=UTC),
        )

    assert chunk.station_id == "caravan:camels_us_01013500"
    assert chunk.provider == "caravan"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_parses_csv(tmp_path: Path):
    """Parse Caravan CSV with date and streamflow columns."""
    csv_file = tmp_path / "camels_us_01013500.csv"
    csv_file.write_text(SAMPLE_CARAVAN_CSV, encoding="utf-8")

    async with CaravanConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "caravan:camels_us_01013500",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 5
    assert chunk.observations[0].discharge_m3s == pytest.approx(
        15.3,
    )
    assert chunk.observations[4].discharge_m3s == pytest.approx(
        20.2,
    )


@pytest.mark.asyncio
async def test_fetch_observations_nested_path(tmp_path: Path):
    """Connector finds CSV in timeseries/csv/ subdirectory."""
    nested_dir = tmp_path / "timeseries" / "csv"
    nested_dir.mkdir(parents=True)
    csv_file = nested_dir / "camels_gb_15006.csv"
    csv_file.write_text(SAMPLE_CARAVAN_CSV, encoding="utf-8")

    async with CaravanConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "caravan:camels_gb_15006",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 5


@pytest.mark.asyncio
async def test_fetch_observations_recursive_fallback(tmp_path: Path):
    """CSV under an unexpected deep extract path is found via recursive search."""
    # A layout matched by none of the fixed candidates — only rglob resolves it.
    deep = tmp_path / "Caravan-1.5" / "timeseries" / "csv" / "camels"
    deep.mkdir(parents=True)
    (deep / "camels_01013500.csv").write_text(SAMPLE_CARAVAN_CSV, encoding="utf-8")

    async with CaravanConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "caravan:camels_us_01013500",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 5


@pytest.mark.asyncio
async def test_seed_station_ids_are_canonical():
    """Every seed station has a properly formatted CSFS station ID."""
    async with CaravanConnector() as conn:
        stations = await conn.fetch_stations()

    for station in stations:
        assert station.id == f"caravan:{station.native_id}"
        assert station.provider == "caravan"
        assert station.latitude != 0.0 or station.longitude != 0.0


@pytest.mark.asyncio
async def test_connector_registration():
    """The connector is registered under the 'caravan' slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("caravan")
    assert cls is CaravanConnector


@pytest.mark.asyncio
async def test_fetch_observations_file_not_found(tmp_path: Path):
    """When data_dir exists but file is missing, returns empty chunk."""
    async with CaravanConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "caravan:nonexistent_basin",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_alt_column_name(tmp_path: Path):
    """CSV with 'discharge' column is parsed correctly."""
    csv_file = tmp_path / "test_basin.csv"
    csv_file.write_text(SAMPLE_CARAVAN_CSV_ALT, encoding="utf-8")

    async with CaravanConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "caravan:test_basin",
            start=datetime(2000, 6, 1, tzinfo=UTC),
            end=datetime(2000, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(250.0)


@pytest.mark.asyncio
async def test_fetch_observations_missing_headers(tmp_path: Path):
    """CSV with no recognized date/value columns returns empty."""
    csv_content = "col_a,col_b\nfoo,bar\n"
    csv_file = tmp_path / "test_basin.csv"
    csv_file.write_text(csv_content, encoding="utf-8")

    async with CaravanConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "caravan:test_basin",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_unparseable_date_skipped(
    tmp_path: Path,
):
    """Rows with unparseable dates are skipped."""
    csv_content = (
        "date,streamflow\n"
        "not-a-date,15.3\n"
        "1990-01-01,14.8\n"
    )
    csv_file = tmp_path / "test_basin.csv"
    csv_file.write_text(csv_content, encoding="utf-8")

    async with CaravanConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "caravan:test_basin",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1


@pytest.mark.asyncio
async def test_fetch_observations_unparseable_value_missing(
    tmp_path: Path,
):
    """Non-numeric streamflow values result in MISSING quality."""
    csv_content = (
        "date,streamflow\n"
        "1990-01-01,abc\n"
        "1990-01-02,14.8\n"
    )
    csv_file = tmp_path / "test_basin.csv"
    csv_file.write_text(csv_content, encoding="utf-8")

    async with CaravanConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "caravan:test_basin",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[0].quality.value == "missing"


@pytest.mark.asyncio
async def test_fetch_observations_empty_date_skipped(tmp_path: Path):
    """Rows with empty date string are skipped."""
    csv_content = (
        "date,streamflow\n"
        ",15.3\n"
        "1990-01-01,14.8\n"
    )
    csv_file = tmp_path / "test_basin.csv"
    csv_file.write_text(csv_content, encoding="utf-8")

    async with CaravanConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "caravan:test_basin",
            start=datetime(1990, 1, 1, tzinfo=UTC),
            end=datetime(1990, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1


@pytest.mark.asyncio
async def test_fetch_observations_date_filtering_out_of_range(
    tmp_path: Path,
):
    """Observations outside [start, end] are filtered out."""
    csv_file = tmp_path / "test_basin.csv"
    csv_file.write_text(SAMPLE_CARAVAN_CSV, encoding="utf-8")

    async with CaravanConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "caravan:test_basin",
            start=datetime(1990, 1, 2, tzinfo=UTC),
            end=datetime(1990, 1, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2


@pytest.mark.asyncio
async def test_fetch_observations_naive_datetimes(tmp_path: Path):
    """Naive start/end datetimes are treated as UTC."""
    csv_file = tmp_path / "test_basin.csv"
    csv_file.write_text(SAMPLE_CARAVAN_CSV, encoding="utf-8")

    async with CaravanConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "caravan:test_basin",
            start=datetime(1990, 1, 1),  # naive
            end=datetime(1990, 1, 5),  # naive
        )

    assert len(chunk.observations) == 5


def test_safe_float_none():
    """_safe_float returns None for None input."""
    from csfs.connectors.caravan import _safe_float

    assert _safe_float(None) is None


def test_safe_float_invalid():
    """_safe_float returns None for non-numeric string."""
    from csfs.connectors.caravan import _safe_float

    assert _safe_float("abc") is None
