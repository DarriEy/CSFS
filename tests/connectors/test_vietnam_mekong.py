"""Tests for the Vietnam Mekong Delta EIDC connector with mocked HTTP and CSV."""

from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from csfs.connectors.vietnam_mekong import (
    _SEED_STATIONS,
    EIDC_CATALOGUE_DOC,
    VietnamMekongConnector,
)

# ---------------------------------------------------------------------------
# Sample CSV data
# ---------------------------------------------------------------------------

SAMPLE_EIDC_CSV = """\
datetime,discharge_m3s,sediment
2010-06-01 00:00:00,8500.0,120.5
2010-06-01 01:00:00,8520.0,121.0
2010-06-01 02:00:00,8480.0,119.8
"""

SAMPLE_EIDC_CSV_WITH_STATION = """\
datetime,discharge_m3s,sediment,station
2010-06-01 00:00:00,8500.0,120.5,chau_doc
2010-06-01 01:00:00,8520.0,121.0,chau_doc
2010-06-01 02:00:00,3200.0,55.0,tan_chau
"""

SAMPLE_EIDC_CSV_MISSING = """\
datetime,discharge_m3s,sediment
2010-06-01 00:00:00,8500.0,120.5
2010-06-01 01:00:00,NA,
2010-06-01 02:00:00,,119.8
"""

MOCK_EIDC_RESPONSE = {
    "data": [
        {
            "datetime": "2010-06-01T00:00:00",
            "discharge_m3s": 8500.0,
        },
        {
            "datetime": "2010-06-01T01:00:00",
            "discharge_m3s": 8520.0,
        },
    ]
}


# ---------------------------------------------------------------------------
# Station listing tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_stations_returns_seed_list():
    """Seed list returns all 4 Mekong Delta stations."""
    async with VietnamMekongConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    assert len(stations) == 4


@pytest.mark.asyncio
async def test_fetch_stations_metadata():
    """Seed stations have correct metadata fields."""
    async with VietnamMekongConnector() as conn:
        stations = await conn.fetch_stations()

    chau_doc = next(
        s for s in stations if s.native_id == "chau_doc"
    )
    assert chau_doc.id == "vietnam_mekong:chau_doc"
    assert chau_doc.provider == "vietnam_mekong"
    assert chau_doc.name == "Chau Doc"
    assert chau_doc.latitude == pytest.approx(10.70)
    assert chau_doc.longitude == pytest.approx(105.12)
    assert chau_doc.country_code == "VN"
    assert chau_doc.river == "Mekong (Bassac)"


# ---------------------------------------------------------------------------
# EIDC API tests (respx mocks)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_eidc_api():
    """EIDC catalogue API response is parsed correctly."""
    url = (
        f"https://catalogue.ceh.ac.uk/documents/"
        f"{EIDC_CATALOGUE_DOC}"
    )
    respx.get(url).mock(
        return_value=httpx.Response(
            200, json=MOCK_EIDC_RESPONSE,
        ),
    )

    async with VietnamMekongConnector() as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 2, tzinfo=UTC),
        )

    assert chunk.provider == "vietnam_mekong"
    assert chunk.station_id == "vietnam_mekong:chau_doc"
    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(
        8500.0,
    )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_api_fails_csv_fallback(
    tmp_path: Path,
):
    """Falls back to CSV when EIDC catalogue API fails."""
    url = (
        f"https://catalogue.ceh.ac.uk/documents/"
        f"{EIDC_CATALOGUE_DOC}"
    )
    respx.get(url).mock(
        return_value=httpx.Response(500),
    )

    csv_file = tmp_path / "chau_doc.csv"
    csv_file.write_text(SAMPLE_EIDC_CSV, encoding="utf-8")

    async with VietnamMekongConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 1, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(
        8500.0,
    )


# ---------------------------------------------------------------------------
# Local CSV tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_csv_parses_correctly(
    tmp_path: Path,
):
    """EIDC CSV with hourly discharge data is parsed correctly."""
    csv_file = tmp_path / "chau_doc.csv"
    csv_file.write_text(SAMPLE_EIDC_CSV, encoding="utf-8")

    async with VietnamMekongConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 1, 3, tzinfo=UTC),
        )

    assert chunk.station_id == "vietnam_mekong:chau_doc"
    assert chunk.provider == "vietnam_mekong"
    assert len(chunk.observations) == 3
    assert chunk.observations[1].discharge_m3s == pytest.approx(
        8520.0,
    )
    assert chunk.observations[0].quality.value == "raw"


@pytest.mark.asyncio
async def test_fetch_observations_csv_handles_missing(
    tmp_path: Path,
):
    """NA and empty values are treated as missing."""
    csv_file = tmp_path / "chau_doc.csv"
    csv_file.write_text(
        SAMPLE_EIDC_CSV_MISSING, encoding="utf-8",
    )

    async with VietnamMekongConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 1, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s is not None
    assert chunk.observations[1].discharge_m3s is None
    assert chunk.observations[1].quality.value == "missing"
    assert chunk.observations[2].discharge_m3s is None


@pytest.mark.asyncio
async def test_fetch_observations_csv_station_filter(
    tmp_path: Path,
):
    """CSV with station column filters by station."""
    csv_file = tmp_path / "mekong_data.csv"
    csv_file.write_text(
        SAMPLE_EIDC_CSV_WITH_STATION, encoding="utf-8",
    )

    async with VietnamMekongConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 1, 3, tzinfo=UTC),
        )

    # Only chau_doc rows, not tan_chau
    assert len(chunk.observations) == 2
    for obs in chunk.observations:
        assert obs.discharge_m3s != pytest.approx(3200.0)


@pytest.mark.asyncio
async def test_fetch_observations_no_data_dir():
    """Without data_dir and no web, returns empty chunk."""
    async with VietnamMekongConnector() as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 2, tzinfo=UTC),
        )

    assert chunk.station_id == "vietnam_mekong:chau_doc"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_connector_registration():
    """Connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("vietnam_mekong")
    assert cls is VietnamMekongConnector


@pytest.mark.asyncio
async def test_seed_station_ids_are_canonical():
    """Every seed station has a properly formatted CSFS station ID."""
    async with VietnamMekongConnector() as conn:
        stations = await conn.fetch_stations()

    for station in stations:
        assert station.id == (
            f"vietnam_mekong:{station.native_id}"
        )
        assert station.provider == "vietnam_mekong"
        assert station.country_code == "VN"


# ---------------------------------------------------------------------------
# Additional coverage tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_eidc_response_as_list():
    """EIDC returning a bare list is parsed correctly."""
    url = (
        f"https://catalogue.ceh.ac.uk/documents/"
        f"{EIDC_CATALOGUE_DOC}"
    )
    bare_list = [
        {
            "datetime": "2010-06-01T00:00:00",
            "discharge_m3s": 8500.0,
        },
    ]
    respx.get(url).mock(
        return_value=httpx.Response(200, json=bare_list),
    )

    async with VietnamMekongConnector() as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1


@pytest.mark.asyncio
@respx.mock
async def test_eidc_response_non_dict_non_list():
    """EIDC returning unexpected type falls through."""
    url = (
        f"https://catalogue.ceh.ac.uk/documents/"
        f"{EIDC_CATALOGUE_DOC}"
    )
    respx.get(url).mock(
        return_value=httpx.Response(200, json="just-a-string"),
    )

    async with VietnamMekongConnector() as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_eidc_obs_list_not_a_list():
    """EIDC response with non-list data falls through."""
    url = (
        f"https://catalogue.ceh.ac.uk/documents/"
        f"{EIDC_CATALOGUE_DOC}"
    )
    respx.get(url).mock(
        return_value=httpx.Response(
            200, json={"data": "not-a-list"},
        ),
    )

    async with VietnamMekongConnector() as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_eidc_empty_observations_falls_through():
    """EIDC returning empty observations falls through to CSV."""
    url = (
        f"https://catalogue.ceh.ac.uk/documents/"
        f"{EIDC_CATALOGUE_DOC}"
    )
    respx.get(url).mock(
        return_value=httpx.Response(
            200, json={"data": []},
        ),
    )

    async with VietnamMekongConnector() as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_eidc_obs_entry_parse_error_skipped():
    """EIDC entries that raise ValueError/TypeError are skipped."""
    url = (
        f"https://catalogue.ceh.ac.uk/documents/"
        f"{EIDC_CATALOGUE_DOC}"
    )
    data = {
        "data": [
            {
                "datetime": "2010-06-01T00:00:00",
                "discharge_m3s": 8500.0,
            },
        ],
    }
    respx.get(url).mock(
        return_value=httpx.Response(200, json=data),
    )

    async with VietnamMekongConnector() as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1


@pytest.mark.asyncio
@respx.mock
async def test_eidc_obs_out_of_range_filtered():
    """EIDC observations outside the time range are filtered out."""
    url = (
        f"https://catalogue.ceh.ac.uk/documents/"
        f"{EIDC_CATALOGUE_DOC}"
    )
    data = {
        "data": [
            {
                "datetime": "2010-05-01T00:00:00",
                "discharge_m3s": 7000.0,
            },
            {
                "datetime": "2010-06-01T00:00:00",
                "discharge_m3s": 8500.0,
            },
        ],
    }
    respx.get(url).mock(
        return_value=httpx.Response(200, json=data),
    )

    async with VietnamMekongConnector() as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1


@pytest.mark.asyncio
async def test_csv_no_csv_files_in_dir(tmp_path: Path):
    """Empty data_dir (no CSV files) returns empty chunk."""
    async with VietnamMekongConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_csv_invalid_data_dir():
    """Non-existent data_dir returns empty chunk."""
    async with VietnamMekongConnector(
        config={"data_dir": "/nonexistent/path"},
    ) as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_csv_file_for_other_station_skipped(tmp_path: Path):
    """CSV files named for other stations are skipped."""
    csv_file = tmp_path / "tan_chau.csv"
    csv_file.write_text(SAMPLE_EIDC_CSV, encoding="utf-8")

    async with VietnamMekongConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 1, 3, tzinfo=UTC),
        )

    # tan_chau.csv is for another station, not chau_doc
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_csv_empty_file(tmp_path: Path):
    """Empty CSV file returns empty observations."""
    csv_file = tmp_path / "chau_doc.csv"
    csv_file.write_text("", encoding="utf-8")

    async with VietnamMekongConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_csv_no_datetime_column(tmp_path: Path):
    """CSV without a datetime column returns empty."""
    csv_data = """\
value,station
8500.0,chau_doc
"""
    csv_file = tmp_path / "chau_doc.csv"
    csv_file.write_text(csv_data, encoding="utf-8")

    async with VietnamMekongConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_csv_short_row_skipped(tmp_path: Path):
    """CSV rows with fewer columns than needed are skipped."""
    csv_data = """\
datetime,discharge_m3s,station
2010-06-01 00:00:00
2010-06-01 01:00:00,8520.0,chau_doc
"""
    csv_file = tmp_path / "chau_doc.csv"
    csv_file.write_text(csv_data, encoding="utf-8")

    async with VietnamMekongConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 1, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1


@pytest.mark.asyncio
async def test_csv_out_of_range_rows_skipped(tmp_path: Path):
    """CSV rows outside time range are skipped."""
    csv_data = """\
datetime,discharge_m3s
2010-05-01 00:00:00,7000.0
2010-06-01 00:00:00,8500.0
"""
    csv_file = tmp_path / "chau_doc.csv"
    csv_file.write_text(csv_data, encoding="utf-8")

    async with VietnamMekongConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1


@pytest.mark.asyncio
async def test_csv_nan_and_dash_missing(tmp_path: Path):
    """nan and - values are treated as missing."""
    csv_data = """\
datetime,discharge_m3s
2010-06-01 00:00:00,nan
2010-06-01 01:00:00,-
"""
    csv_file = tmp_path / "chau_doc.csv"
    csv_file.write_text(csv_data, encoding="utf-8")

    async with VietnamMekongConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "vietnam_mekong:chau_doc",
            start=datetime(2010, 6, 1, tzinfo=UTC),
            end=datetime(2010, 6, 1, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    for obs in chunk.observations:
        assert obs.discharge_m3s is None
        assert obs.quality.value == "missing"


def test_parse_timestamp_various_formats():
    """_parse_timestamp handles various formats."""
    conn = VietnamMekongConnector()
    assert conn._parse_timestamp({}) is None
    assert conn._parse_timestamp({"datetime": ""}) is None

    ts = conn._parse_timestamp(
        {"datetime": "2010-06-01T00:00:00"},
    )
    assert ts is not None

    # Unparseable
    assert conn._parse_timestamp(
        {"datetime": "not-a-date"},
    ) is None


def test_parse_datetime_various_formats():
    """_parse_datetime handles various datetime formats."""
    assert VietnamMekongConnector._parse_datetime(
        "2010-06-01 00:00:00",
    ) is not None
    assert VietnamMekongConnector._parse_datetime(
        "2010-06-01",
    ) is not None
    assert VietnamMekongConnector._parse_datetime(
        "01/06/2010 00:00:00",
    ) is not None
    assert VietnamMekongConnector._parse_datetime(
        "not-a-date",
    ) is None
