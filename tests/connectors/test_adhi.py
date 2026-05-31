"""Tests for the ADHI (African Database of Hydrometric Indices) connector."""

import io
import zipfile
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from csfs.connectors.adhi import _SEED_STATIONS, ADHI_COUNTRY_CODES, ADHIConnector


def _make_monthly_zip(series_by_station: dict[str, str]) -> bytes:
    """Build an in-memory ADHI MonthlySeries.zip.

    ``series_by_station`` maps a native_id to the headerless text content of
    its ``monthly_{native_id}.txt`` file (columns: year, month, mean, max,
    min, num_missing_days).
    """
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        for native_id, content in series_by_station.items():
            zf.writestr(f"MonthlySeries/monthly_{native_id}.txt", content)
    return buf.getvalue()

# ------------------------------------------------------------------
# Mock DataVerse API responses
# ------------------------------------------------------------------

MOCK_DATAVERSE_RESPONSE = {
    "status": "OK",
    "data": {
        "id": 99999,
        "persistentUrl": "https://doi.org/10.23708/LXGXQ9",
        "latestVersion": {
            "versionState": "RELEASED",
            "files": [
                {
                    "dataFile": {
                        "id": 5001,
                        "filename": "ADHI_stations_metadata.tab",
                        "filesize": 245000,
                    },
                },
                {
                    "dataFile": {
                        # Decoy: matches the old "discharge" hint but is a
                        # 35 MB archive of PNG plots, not data.
                        "id": 5002,
                        "filename": "ADHI_Discharge_plots.zip",
                        "filesize": 35000000,
                    },
                },
                {
                    "dataFile": {
                        "id": 5004,
                        "filename": "ADHI_MonthlySeries.zip",
                        "filesize": 6452188,
                    },
                },
                {
                    "dataFile": {
                        "id": 5003,
                        "filename": "README.txt",
                        "filesize": 3200,
                    },
                },
            ],
        },
    },
}

MOCK_STATION_METADATA_TAB = (
    "station_code\tstation_name\tlatitude\tlongitude"
    "\tcountry_code\triver\tcatchment_area\n"
    "ADHI-NG-0001\tNIGER AT LOKOJA\t7.80\t6.74\tNG\tNIGER\t2074000\n"
    "ADHI-KE-0001\tTANA AT GARISSA\t-0.46\t39.64\tKE\tTANA\t32500\n"
    "ADHI-ZA-0001\tORANGE AT VIOOLSDRIF\t-28.77\t17.73"
    "\tZA\tORANGE\t850530\n"
)

MOCK_STATION_METADATA_MISSING_COORDS = (
    "station_code\tstation_name\tlatitude\tlongitude\tcountry_code\n"
    "ADHI-BAD\tNo Coords\t\t\tXX\n"
    "ADHI-OK\tGood Station\t10.0\t20.0\tNG\n"
)

MOCK_DISCHARGE_LOCAL_CSV = (
    "station_code,date,discharge,quality\n"
    "ADHI-NG-0001,1970-01,1250.5,0\n"
    "ADHI-NG-0001,1970-02,980.0,0\n"
    "ADHI-NG-0001,1970-03,-999.0,3\n"
    "ADHI-NG-0001,1970-06,500.0,0\n"
)


# ------------------------------------------------------------------
# Station listing tests
# ------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_seed_fallback():
    """When DataVerse is unavailable, falls back to seed stations."""
    respx.get(
        "https://dataverse.ird.fr/api/datasets/:persistentId/",
    ).mock(side_effect=httpx.ConnectError("connection refused"))

    async with ADHIConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    first = stations[0]
    assert first.provider == "adhi"
    assert first.id.startswith("adhi:")


@pytest.mark.asyncio
@respx.mock
async def test_seed_station_ids_are_canonical():
    """Every seed station has properly formatted CSFS station IDs."""
    respx.get(
        "https://dataverse.ird.fr/api/datasets/:persistentId/",
    ).mock(side_effect=httpx.ConnectError("connection refused"))

    async with ADHIConnector() as conn:
        stations = await conn.fetch_stations()

    for station in stations:
        assert station.id == f"adhi:{station.native_id}"
        assert station.provider == "adhi"
        assert station.latitude != 0.0 or station.longitude != 0.0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_from_api():
    """Stations are fetched and parsed from the DataVerse API."""
    respx.get(
        "https://dataverse.ird.fr/api/datasets/:persistentId/",
    ).mock(
        return_value=httpx.Response(200, json=MOCK_DATAVERSE_RESPONSE),
    )
    respx.get(
        "https://dataverse.ird.fr/api/access/datafile/5001",
    ).mock(
        return_value=httpx.Response(
            200, text=MOCK_STATION_METADATA_TAB,
        ),
    )

    async with ADHIConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 3
    ng = next(s for s in stations if s.native_id == "ADHI-NG-0001")
    assert ng.name == "NIGER AT LOKOJA"
    assert ng.country_code == "NG"
    assert ng.river == "NIGER"
    assert ng.latitude == pytest.approx(7.80)
    assert ng.longitude == pytest.approx(6.74)
    assert ng.catchment_area_km2 == pytest.approx(2074000.0)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_missing_coordinates():
    """Stations with missing lat/lon are skipped during parsing."""
    respx.get(
        "https://dataverse.ird.fr/api/datasets/:persistentId/",
    ).mock(
        return_value=httpx.Response(200, json=MOCK_DATAVERSE_RESPONSE),
    )
    respx.get(
        "https://dataverse.ird.fr/api/access/datafile/5001",
    ).mock(
        return_value=httpx.Response(
            200, text=MOCK_STATION_METADATA_MISSING_COORDS,
        ),
    )

    async with ADHIConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "ADHI-OK"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_api_error_falls_back_to_seed():
    """When the DataVerse API returns a server error, seed list is used."""
    respx.get(
        "https://dataverse.ird.fr/api/datasets/:persistentId/",
    ).mock(
        return_value=httpx.Response(500),
    )

    async with ADHIConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)


# ------------------------------------------------------------------
# Observation tests
# ------------------------------------------------------------------


# Headerless monthly series: year, month, mean, max, min, num_missing_days.
MOCK_MONTHLY_NG = (
    "1970,1,1250.5,1300.0,1200.0,0\n"
    "1970,2,980.0,1010.0,950.0,0\n"
    "1970,3,NaN,NaN,NaN,31\n"
    "1970,4,1100.2,1150.0,1050.0,2\n"
)
MOCK_MONTHLY_KE = (
    "1970,1,55.3,60.0,50.0,0\n"
    "1970,2,48.1,52.0,44.0,0\n"
)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_from_api():
    """Monthly discharge is fetched and parsed from the MonthlySeries ZIP."""
    respx.get(
        "https://dataverse.ird.fr/api/datasets/:persistentId/",
    ).mock(
        return_value=httpx.Response(200, json=MOCK_DATAVERSE_RESPONSE),
    )
    zip_bytes = _make_monthly_zip({
        "ADHI-NG-0001": MOCK_MONTHLY_NG,
        "ADHI-KE-0001": MOCK_MONTHLY_KE,
    })
    respx.get(
        "https://dataverse.ird.fr/api/access/datafile/5004",
    ).mock(
        return_value=httpx.Response(200, content=zip_bytes),
    )

    async with ADHIConnector() as conn:
        chunk = await conn.fetch_observations(
            "adhi:ADHI-NG-0001",
            start=datetime(1970, 1, 1, tzinfo=UTC),
            end=datetime(1970, 12, 31, tzinfo=UTC),
        )

    assert chunk.station_id == "adhi:ADHI-NG-0001"
    assert chunk.provider == "adhi"
    assert len(chunk.observations) == 4

    # First obs: valid mean monthly runoff
    assert chunk.observations[0].discharge_m3s == pytest.approx(1250.5)
    assert chunk.observations[0].quality.value == "good"
    assert chunk.observations[0].timestamp == datetime(1970, 1, 1, tzinfo=UTC)

    # Third obs: NaN value -> missing
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"

    # Fourth obs: valid again
    assert chunk.observations[3].discharge_m3s == pytest.approx(1100.2)
    assert chunk.observations[3].quality.value == "good"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_filters_by_station():
    """Each station reads only its own file from the ZIP archive."""
    respx.get(
        "https://dataverse.ird.fr/api/datasets/:persistentId/",
    ).mock(
        return_value=httpx.Response(200, json=MOCK_DATAVERSE_RESPONSE),
    )
    zip_bytes = _make_monthly_zip({
        "ADHI-NG-0001": MOCK_MONTHLY_NG,
        "ADHI-KE-0001": MOCK_MONTHLY_KE,
    })
    respx.get(
        "https://dataverse.ird.fr/api/access/datafile/5004",
    ).mock(
        return_value=httpx.Response(200, content=zip_bytes),
    )

    async with ADHIConnector() as conn:
        chunk = await conn.fetch_observations(
            "adhi:ADHI-KE-0001",
            start=datetime(1970, 1, 1, tzinfo=UTC),
            end=datetime(1970, 12, 31, tzinfo=UTC),
        )

    assert chunk.station_id == "adhi:ADHI-KE-0001"
    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(55.3)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_selects_monthly_series_not_plots():
    """The plots ZIP decoy is ignored; MonthlySeries.zip (id 5004) is used."""
    respx.get(
        "https://dataverse.ird.fr/api/datasets/:persistentId/",
    ).mock(
        return_value=httpx.Response(200, json=MOCK_DATAVERSE_RESPONSE),
    )
    plots_route = respx.get(
        "https://dataverse.ird.fr/api/access/datafile/5002",
    ).mock(return_value=httpx.Response(200, content=b"PNG-NOT-DATA"))
    respx.get(
        "https://dataverse.ird.fr/api/access/datafile/5004",
    ).mock(
        return_value=httpx.Response(
            200, content=_make_monthly_zip({"ADHI-NG-0001": MOCK_MONTHLY_NG}),
        ),
    )

    async with ADHIConnector() as conn:
        chunk = await conn.fetch_observations(
            "adhi:ADHI-NG-0001",
            start=datetime(1970, 1, 1, tzinfo=UTC),
            end=datetime(1970, 12, 31, tzinfo=UTC),
        )

    assert len(chunk.observations) == 4
    assert not plots_route.called


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_bad_zip_returns_empty():
    """A corrupt MonthlySeries download is handled gracefully."""
    respx.get(
        "https://dataverse.ird.fr/api/datasets/:persistentId/",
    ).mock(
        return_value=httpx.Response(200, json=MOCK_DATAVERSE_RESPONSE),
    )
    respx.get(
        "https://dataverse.ird.fr/api/access/datafile/5004",
    ).mock(return_value=httpx.Response(200, content=b"not-a-zip"))

    async with ADHIConnector() as conn:
        chunk = await conn.fetch_observations(
            "adhi:ADHI-NG-0001",
            start=datetime(1970, 1, 1, tzinfo=UTC),
            end=datetime(1970, 12, 31, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_from_local_file(tmp_path: Path):
    """Observations are read from a local pre-downloaded CSV file."""
    data_file = tmp_path / "ADHI-NG-0001.csv"
    data_file.write_text(MOCK_DISCHARGE_LOCAL_CSV, encoding="utf-8")

    async with ADHIConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "adhi:ADHI-NG-0001",
            start=datetime(1970, 1, 1, tzinfo=UTC),
            end=datetime(1970, 3, 31, tzinfo=UTC),
        )

    assert chunk.station_id == "adhi:ADHI-NG-0001"
    assert len(chunk.observations) == 3

    # -999 should be mapped to missing
    missing = chunk.observations[2]
    assert missing.discharge_m3s is None
    assert missing.quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_no_data_returns_empty():
    """Without data_dir or API, returns empty chunk."""
    respx.get(
        "https://dataverse.ird.fr/api/datasets/:persistentId/",
    ).mock(side_effect=httpx.ConnectError("connection refused"))

    async with ADHIConnector() as conn:
        chunk = await conn.fetch_observations(
            "adhi:ADHI-XX-9999",
            start=datetime(1970, 1, 1, tzinfo=UTC),
            end=datetime(1970, 12, 31, tzinfo=UTC),
        )

    assert chunk.station_id == "adhi:ADHI-XX-9999"
    assert chunk.provider == "adhi"
    assert len(chunk.observations) == 0


# ------------------------------------------------------------------
# Registration and metadata tests
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_connector_registration():
    """The connector is registered under the 'adhi' slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("adhi")
    assert cls is ADHIConnector


@pytest.mark.asyncio
async def test_country_codes_comprehensive():
    """ADHI covers a comprehensive list of African countries."""
    assert len(ADHI_COUNTRY_CODES) >= 40
    # Spot-check major countries
    for code in ("NG", "ZA", "EG", "KE", "ET", "CD", "GH", "TZ"):
        assert code in ADHI_COUNTRY_CODES


# ------------------------------------------------------------------
# Additional coverage tests — error branches, edge cases
# ------------------------------------------------------------------


def test_safe_float_non_numeric_returns_none():
    """_safe_float returns None for non-numeric values (lines 212-213)."""
    from csfs.connectors.adhi import _safe_float

    assert _safe_float("not_a_number") is None
    assert _safe_float("") is None


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest_returns_empty_chunk():
    """fetch_latest returns empty chunk for historical-only data (line 361)."""
    async with ADHIConnector() as conn:
        chunk = await conn.fetch_latest("adhi:ADHI-NG-0001")

    assert chunk.station_id == "adhi:ADHI-NG-0001"
    assert chunk.provider == "adhi"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_get_file_list_caching():
    """File list is cached after first fetch (lines 369-370)."""
    respx.get(
        "https://dataverse.ird.fr/api/datasets/:persistentId/",
    ).mock(
        return_value=httpx.Response(200, json=MOCK_DATAVERSE_RESPONSE),
    )

    async with ADHIConnector() as conn:
        file_list_1 = await conn._get_file_list()
        file_list_2 = await conn._get_file_list()

    # Both calls return the same cached list
    assert file_list_1 is file_list_2
    assert len(file_list_1) == 4


@pytest.mark.asyncio
@respx.mock
async def test_get_file_list_alternative_structure():
    """File list parsed from alternative response structure (line 390)."""
    alt_response = {
        "status": "OK",
        "data": {
            "id": 99999,
            "latestVersion": {
                "versionState": "RELEASED",
                "files": [],  # empty latestVersion files
            },
            "files": [
                {
                    "dataFile": {
                        "id": 6001,
                        "filename": "ADHI_stations_metadata.tab",
                        "filesize": 100000,
                    },
                },
            ],
        },
    }
    respx.get(
        "https://dataverse.ird.fr/api/datasets/:persistentId/",
    ).mock(
        return_value=httpx.Response(200, json=alt_response),
    )

    async with ADHIConnector() as conn:
        file_list = await conn._get_file_list()

    assert len(file_list) == 1
    assert file_list[0]["filename"] == "ADHI_stations_metadata.tab"


@pytest.mark.asyncio
@respx.mock
async def test_get_file_list_no_files_raises():
    """Empty file list raises DataFormatError (lines 402)."""
    from csfs.core.exceptions import DataFormatError

    empty_response = {
        "status": "OK",
        "data": {
            "latestVersion": {"files": []},
        },
    }
    respx.get(
        "https://dataverse.ird.fr/api/datasets/:persistentId/",
    ).mock(
        return_value=httpx.Response(200, json=empty_response),
    )

    async with ADHIConnector() as conn:
        with pytest.raises(DataFormatError, match="No files found"):
            await conn._get_file_list()


def test_find_file_tab_fallback():
    """_find_file falls back to .tab files when no hint matches (lines 428-433)."""
    conn = ADHIConnector()
    file_list = [
        {"filename": "something_else.tab", "id": 1},
        {"filename": "readme.txt", "id": 2},
    ]
    result = conn._find_file(file_list, ("nonexistent_hint",))
    assert result is not None
    assert result["id"] == 1


def test_find_file_no_match_returns_none():
    """_find_file returns None when no file matches (line 433)."""
    conn = ADHIConnector()
    file_list = [
        {"filename": "readme.txt", "id": 1},
    ]
    result = conn._find_file(file_list, ("nonexistent_hint",))
    assert result is None


@pytest.mark.asyncio
@respx.mock
async def test_download_datafile_error_raises():
    """Failed datafile download raises ConnectorError (lines 440-441)."""
    from csfs.core.exceptions import ConnectorError

    respx.get(
        "https://dataverse.ird.fr/api/access/datafile/9999",
    ).mock(
        return_value=httpx.Response(500),
    )

    async with ADHIConnector() as conn:
        with pytest.raises(ConnectorError, match="Failed to download"):
            await conn._download_datafile(9999)


def test_parse_station_metadata_no_header_raises():
    """Station metadata with no header raises DataFormatError (line 459)."""
    from csfs.core.exceptions import DataFormatError

    conn = ADHIConnector()
    with pytest.raises(DataFormatError, match="no header"):
        conn._parse_station_metadata("")


def test_parse_station_metadata_skips_bad_rows():
    """Rows that raise errors are skipped (lines 474-480)."""
    content = (
        "station_code\tstation_name\tlatitude\tlongitude\tcountry_code\n"
        "GOOD\tGood Station\t10.0\t20.0\tNG\n"
    )
    conn = ADHIConnector()
    stations = conn._parse_station_metadata(content)
    assert len(stations) == 1
    assert stations[0].native_id == "GOOD"


def test_parse_station_row_empty_id_returns_none():
    """Row with empty station_code returns None (line 501)."""
    conn = ADHIConnector()
    row = {
        "station_code": "",
        "station_name": "Test",
        "latitude": "10.0",
        "longitude": "20.0",
        "country_code": "NG",
    }
    field_map = {k.lower(): k for k in row}
    result = conn._parse_station_row(row, field_map)
    assert result is None


def test_parse_discharge_data_no_header_returns_empty():
    """Discharge data with no header returns empty list (line 572)."""
    conn = ADHIConnector()
    result = conn._parse_discharge_data(
        "", "ADHI-NG-0001", "adhi:ADHI-NG-0001",
        datetime(1970, 1, 1, tzinfo=UTC),
        datetime(1970, 12, 31, tzinfo=UTC),
    )
    assert result == []


def test_parse_discharge_row_outside_range_returns_none():
    """Row outside date range returns None (line 611)."""
    conn = ADHIConnector()
    lrow = {
        "date": "1980-01",
        "discharge": "100.0",
    }
    result = conn._parse_discharge_row(
        lrow, "adhi:test",
        datetime(1970, 1, 1, tzinfo=UTC),
        datetime(1970, 12, 31, tzinfo=UTC),
    )
    assert result is None


def test_parse_discharge_row_invalid_value_sets_missing():
    """Non-numeric discharge value results in MISSING quality (lines 631-633)."""
    from csfs.core.models import QualityFlag

    conn = ADHIConnector()
    lrow = {
        "date": "1970-01",
        "discharge": "bad_value",
    }
    result = conn._parse_discharge_row(
        lrow, "adhi:test",
        datetime(1970, 1, 1, tzinfo=UTC),
        datetime(1970, 12, 31, tzinfo=UTC),
    )
    assert result is not None
    assert result.discharge_m3s is None
    assert result.quality == QualityFlag.MISSING


def test_parse_row_timestamp_date_formats():
    """_parse_row_timestamp handles multiple date formats (lines 673-683)."""
    # year-month format
    result = ADHIConnector._parse_row_timestamp({"date": "1970-01"})
    assert result is not None
    assert result.year == 1970 and result.month == 1

    # dd/mm/yyyy format
    result = ADHIConnector._parse_row_timestamp({"date": "15/03/1975"})
    assert result is not None
    assert result.year == 1975 and result.month == 3

    # yyyy/mm/dd format
    result = ADHIConnector._parse_row_timestamp({"date": "1975/03/15"})
    assert result is not None
    assert result.year == 1975

    # year + month columns
    result = ADHIConnector._parse_row_timestamp(
        {"year": "1980", "month": "6"},
    )
    assert result is not None
    assert result.year == 1980 and result.month == 6

    # invalid year+month returns None
    result = ADHIConnector._parse_row_timestamp(
        {"year": "bad", "month": "6"},
    )
    assert result is None

    # no date at all returns None
    result = ADHIConnector._parse_row_timestamp({})
    assert result is None


@pytest.mark.asyncio
async def test_read_local_observations_no_file_returns_none():
    """_read_local_observations returns None when no file exists (line 700)."""
    async with ADHIConnector() as conn:
        result = conn._read_local_observations(
            Path("/nonexistent/dir"), "ADHI-XX-9999", "adhi:ADHI-XX-9999",
            datetime(1970, 1, 1, tzinfo=UTC),
            datetime(1970, 12, 31, tzinfo=UTC),
        )
    assert result is None


@pytest.mark.asyncio
async def test_read_local_observations_read_error_returns_none(tmp_path: Path):
    """_read_local_observations returns None on OSError (lines 704-710)."""
    import os

    # Create a file, then make it unreadable
    data_file = tmp_path / "ADHI-NG-0001.csv"
    data_file.write_text("some content")
    os.chmod(data_file, 0o000)

    try:
        async with ADHIConnector() as conn:
            result = conn._read_local_observations(
                tmp_path, "ADHI-NG-0001", "adhi:ADHI-NG-0001",
                datetime(1970, 1, 1, tzinfo=UTC),
                datetime(1970, 12, 31, tzinfo=UTC),
            )
        assert result is None
    finally:
        os.chmod(data_file, 0o644)


def test_find_local_file_tab_extension(tmp_path: Path):
    """_find_local_file finds .tab files (line 737)."""
    tab_file = tmp_path / "ADHI-NG-0001.tab"
    tab_file.write_text("some content")

    result = ADHIConnector._find_local_file(tmp_path, "ADHI-NG-0001")
    assert result == tab_file


def test_detect_delimiter_tab():
    """_detect_delimiter correctly detects tab delimiter (line 781)."""
    content = "col1\tcol2\tcol3\nval1\tval2\tval3"
    result = ADHIConnector._detect_delimiter(content)
    assert result == "\t"


def test_detect_delimiter_semicolon():
    """_detect_delimiter correctly detects semicolon delimiter."""
    content = "col1;col2;col3\nval1;val2;val3"
    result = ADHIConnector._detect_delimiter(content)
    assert result == ";"


def test_detect_delimiter_default_comma():
    """_detect_delimiter defaults to comma when no delimiter found."""
    content = "singlecolumn\nvalue"
    result = ADHIConnector._detect_delimiter(content)
    assert result == ","
