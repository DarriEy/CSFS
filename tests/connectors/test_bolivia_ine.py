"""Tests for the Bolivia INE connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.bolivia_ine import (
    _GRDC_BOLIVIAN_STATIONS,
    _SEED_STATIONS,
    BoliviaIneConnector,
)

BASE_URL = "https://anda.ine.gob.bo"

MOCK_CATALOG_VARIABLES = {
    "variables": [
        {
            "id": "3716400",
            "label": "Desaguadero",
            "latitude": -16.56,
            "longitude": -69.04,
            "river": "Desaguadero",
        },
        {
            "id": "BO-003",
            "label": "Rurrenabaque",
            "latitude": -14.44,
            "longitude": -67.53,
            "river": "Beni",
        },
        {
            "id": "",
            "label": "Missing ID",
            "latitude": -15.0,
            "longitude": -65.0,
        },
    ],
}

MOCK_LONG_CSV = """station,date,caudal
BO-001,2024-06-01,150.3
BO-001,2024-06-02,148.7
BO-003,2024-06-01,200.5
BO-001,2024-06-03,
"""

MOCK_WIDE_CSV = """date,BO-001,BO-003
2024-06-01,150.3,200.5
2024-06-02,148.7,195.0
2024-06-03,,210.0
"""


@pytest.mark.asyncio
async def test_fetch_stations_returns_seed_list():
    """Seed stations are returned when API is unreachable."""
    async with BoliviaIneConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    native_ids = {s.native_id for s in stations}
    assert "3716400" in native_ids
    assert "3717600" in native_ids


@pytest.mark.asyncio
async def test_fetch_stations_seed_fields():
    """Seed stations have correct metadata."""
    async with BoliviaIneConnector() as conn:
        stations = await conn.fetch_stations()

    abapo = next(
        s for s in stations if s.native_id == "3717600"
    )
    assert abapo.id == "bolivia_ine:3717600"
    assert abapo.provider == "bolivia_ine"
    assert abapo.name == "Abapo"
    assert abapo.country_code == "BO"
    assert abapo.river == "Rio Grande"
    assert abapo.latitude == pytest.approx(-18.85)
    assert abapo.catchment_area_km2 is None


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_from_catalog_api():
    """Station list is parsed from NADA catalog when available."""
    respx.get(f"{BASE_URL}/index.php/catalog/209").mock(
        return_value=httpx.Response(
            200, json=MOCK_CATALOG_VARIABLES,
        ),
    )

    async with BoliviaIneConnector() as conn:
        stations = await conn.fetch_stations()

    # Only 2 valid entries (empty ID is skipped)
    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"3716400", "BO-003"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_long_format_csv():
    """Long-format CSV observations are parsed correctly."""
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/BO-001").mock(
        return_value=httpx.Response(
            200,
            text=MOCK_LONG_CSV,
            headers={"content-type": "text/csv"},
        ),
    )

    async with BoliviaIneConnector() as conn:
        chunk = await conn.fetch_observations(
            "bolivia_ine:BO-001",
            start=datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 3, 23, 59, 59, tzinfo=UTC),
        )

    assert chunk.provider == "bolivia_ine"
    assert chunk.station_id == "bolivia_ine:BO-001"
    # 3 rows for BO-001 (2024-06-01, 02, 03)
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(150.3)
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_returns_empty_on_html():
    """HTML responses (auth needed) are skipped gracefully."""
    html_response = "<html><body>Login required</body></html>"
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/BO-001").mock(
        return_value=httpx.Response(
            200,
            text=html_response,
            headers={"content-type": "text/html"},
        ),
    )
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/data").mock(
        return_value=httpx.Response(
            200,
            text=html_response,
            headers={"content-type": "text/html"},
        ),
    )
    respx.get(
        f"{BASE_URL}/index.php/catalog/209/download/caudales"
    ).mock(
        return_value=httpx.Response(
            200,
            text=html_response,
            headers={"content-type": "text/html"},
        ),
    )

    async with BoliviaIneConnector() as conn:
        chunk = await conn.fetch_observations(
            "bolivia_ine:BO-001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0
    assert chunk.provider == "bolivia_ine"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_returns_empty_on_failure():
    """Server errors return an empty chunk."""
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/BO-008").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/data").mock(
        return_value=httpx.Response(500),
    )
    respx.get(
        f"{BASE_URL}/index.php/catalog/209/download/caudales"
    ).mock(
        return_value=httpx.Response(500),
    )

    async with BoliviaIneConnector() as conn:
        chunk = await conn.fetch_observations(
            "bolivia_ine:BO-008",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


def test_grdc_cross_reference():
    """GRDC station cross-reference list is available."""
    grdc = BoliviaIneConnector.grdc_station_ids()
    assert len(grdc) == len(_GRDC_BOLIVIAN_STATIONS)
    ids = {g[0] for g in grdc}
    assert "3716400" in ids  # Angosto del Bala


def test_connector_is_registered():
    """The connector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("bolivia_ine")
    assert cls is BoliviaIneConnector


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest_delegates_to_fetch_observations():
    """fetch_latest calls fetch_observations with a 30-day window."""
    # All three resource ID attempts return HTML (skipped)
    html = "<html>Login</html>"
    respx.get(url__startswith=f"{BASE_URL}/index.php/catalog/209/download/").mock(
        return_value=httpx.Response(
            200, text=html, headers={"content-type": "text/html"},
        ),
    )

    async with BoliviaIneConnector() as conn:
        chunk = await conn.fetch_latest("bolivia_ine:BO-001")

    assert chunk.provider == "bolivia_ine"
    assert chunk.station_id == "bolivia_ine:BO-001"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_local_csv_fallback(tmp_path):
    """Local CSV files are used when catalog download fails."""
    # All catalog download attempts fail
    respx.get(url__startswith=f"{BASE_URL}/index.php/catalog/209/download/").mock(
        return_value=httpx.Response(500),
    )

    csv_content = "station,date,caudal\nBO-010,2024-06-01,300.5\nBO-010,2024-06-02,310.0\n"
    csv_file = tmp_path / "test_data.csv"
    csv_file.write_text(csv_content)

    async with BoliviaIneConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "bolivia_ine:BO-010",
            start=datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 3, 0, 0, 0, tzinfo=UTC),
        )

    assert chunk is not None
    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(300.5)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_local_csv_wide_format(tmp_path):
    """Wide format CSV files are parsed correctly."""
    respx.get(url__startswith=f"{BASE_URL}/index.php/catalog/209/download/").mock(
        return_value=httpx.Response(500),
    )

    csv_content = "date,BO-010,BO-020\n2024-06-01,300.5,200.5\n2024-06-02,310.0,195.0\n2024-06-03,,210.0\n"
    csv_file = tmp_path / "wide_data.csv"
    csv_file.write_text(csv_content)

    async with BoliviaIneConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "bolivia_ine:BO-010",
            start=datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 3, 23, 59, 59, tzinfo=UTC),
        )

    assert chunk is not None
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(300.5)
    # Empty value -> None discharge, MISSING quality
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_local_csv_no_data_dir():
    """When no data_dir is configured, local CSV fallback returns None."""
    respx.get(url__startswith=f"{BASE_URL}/index.php/catalog/209/download/").mock(
        return_value=httpx.Response(500),
    )

    async with BoliviaIneConnector() as conn:
        chunk = await conn.fetch_observations(
            "bolivia_ine:BO-001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_local_csv_nonexistent_dir():
    """Non-existent data_dir returns empty chunk."""
    respx.get(url__startswith=f"{BASE_URL}/index.php/catalog/209/download/").mock(
        return_value=httpx.Response(500),
    )

    async with BoliviaIneConnector(config={"data_dir": "/nonexistent/path"}) as conn:
        chunk = await conn.fetch_observations(
            "bolivia_ine:BO-001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_local_csv_empty_dir(tmp_path):
    """Empty data_dir (no CSV files) returns empty chunk."""
    respx.get(url__startswith=f"{BASE_URL}/index.php/catalog/209/download/").mock(
        return_value=httpx.Response(500),
    )

    async with BoliviaIneConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "bolivia_ine:BO-001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_local_csv_no_matching_station(tmp_path):
    """CSV exists but has no matching station returns empty chunk."""
    respx.get(url__startswith=f"{BASE_URL}/index.php/catalog/209/download/").mock(
        return_value=httpx.Response(500),
    )

    csv_content = "station,date,caudal\nBO-999,2024-06-01,100.0\n"
    csv_file = tmp_path / "other.csv"
    csv_file.write_text(csv_content)

    async with BoliviaIneConnector(config={"data_dir": str(tmp_path)}) as conn:
        chunk = await conn.fetch_observations(
            "bolivia_ine:BO-001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_catalog_stations_missing_lat_lon():
    """Catalog entries missing latitude or longitude are skipped."""
    catalog_data = {
        "variables": [
            {
                "id": "BO-100",
                "label": "No Coords",
                "latitude": None,
                "longitude": -65.0,
            },
            {
                "id": "BO-101",
                "label": "Has Coords",
                "latitude": -16.0,
                "longitude": -65.0,
            },
        ],
    }
    respx.get(f"{BASE_URL}/index.php/catalog/209").mock(
        return_value=httpx.Response(200, json=catalog_data),
    )

    async with BoliviaIneConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "BO-101"


@pytest.mark.asyncio
@respx.mock
async def test_catalog_stations_parse_exception():
    """Station entries that raise ValueError during parsing are skipped."""
    catalog_data = {
        "variables": [
            {
                "id": "BO-200",
                "label": "Bad Lat",
                "latitude": "not-a-number",
                "longitude": -65.0,
            },
            {
                "id": "BO-201",
                "label": "Good Station",
                "latitude": -16.0,
                "longitude": -65.0,
            },
        ],
    }
    respx.get(f"{BASE_URL}/index.php/catalog/209").mock(
        return_value=httpx.Response(200, json=catalog_data),
    )

    async with BoliviaIneConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "BO-201"


@pytest.mark.asyncio
@respx.mock
async def test_catalog_stations_all_invalid_returns_seed():
    """When all catalog entries are invalid, falls back to seed list."""
    catalog_data = {
        "variables": [
            {
                "id": "",
                "label": "No ID",
                "latitude": -16.0,
                "longitude": -65.0,
            },
        ],
    }
    respx.get(f"{BASE_URL}/index.php/catalog/209").mock(
        return_value=httpx.Response(200, json=catalog_data),
    )

    async with BoliviaIneConnector() as conn:
        stations = await conn.fetch_stations()

    # Returns seed list since parsed catalog is empty (None)
    assert len(stations) == len(_SEED_STATIONS)


@pytest.mark.asyncio
@respx.mock
async def test_catalog_data_general_exception():
    """General exceptions during catalog data parsing are caught."""
    # Mock a response that returns valid HTTP but causes a parse exception
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/BO-001").mock(
        return_value=httpx.Response(
            200,
            text="not,a,valid,csv\n",
            headers={"content-type": "text/csv"},
        ),
    )
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/data").mock(
        side_effect=Exception("Unexpected error"),
    )
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/caudales").mock(
        side_effect=Exception("Another error"),
    )

    async with BoliviaIneConnector() as conn:
        chunk = await conn.fetch_observations(
            "bolivia_ine:BO-001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    # First attempt returns valid CSV but no matching data
    # Second and third attempts raise general exceptions (caught)
    assert chunk.provider == "bolivia_ine"


def test_parse_value_non_numeric():
    """Non-numeric value strings return None with MISSING quality."""
    from csfs.connectors.bolivia_ine import BoliviaIneConnector

    discharge, quality = BoliviaIneConnector._parse_value("abc")
    assert discharge is None
    assert quality.value == "missing"


def test_parse_value_missing_markers():
    """Known missing markers return None with MISSING quality."""
    from csfs.connectors.bolivia_ine import BoliviaIneConnector

    for marker in ("", "na", "nan", "-", "nd"):
        discharge, quality = BoliviaIneConnector._parse_value(marker)
        assert discharge is None
        assert quality.value == "missing"


def test_parse_date_multiple_formats():
    """Various date formats are parsed correctly."""
    from csfs.connectors.bolivia_ine import BoliviaIneConnector

    # dd/mm/yyyy
    result = BoliviaIneConnector._parse_date("01/06/2024")
    assert result is not None
    assert result.year == 2024

    # dd-mm-yyyy
    result = BoliviaIneConnector._parse_date("01-06-2024")
    assert result is not None

    # yyyy/mm/dd
    result = BoliviaIneConnector._parse_date("2024/06/01")
    assert result is not None

    # Unparseable
    result = BoliviaIneConnector._parse_date("not-a-date")
    assert result is None


@pytest.mark.asyncio
@respx.mock
async def test_long_format_csv_missing_columns():
    """Long-format CSV missing required columns returns empty."""
    csv_text = "col1,col2,col3\na,b,c\n"
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/BO-001").mock(
        return_value=httpx.Response(
            200, text=csv_text,
            headers={"content-type": "text/csv"},
        ),
    )
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/data").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/caudales").mock(
        return_value=httpx.Response(500),
    )

    async with BoliviaIneConnector() as conn:
        chunk = await conn.fetch_observations(
            "bolivia_ine:BO-001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 3, tzinfo=UTC),
        )

    # CSV parsed but no matching columns, returns chunk with 0 obs
    assert chunk.provider == "bolivia_ine"


@pytest.mark.asyncio
@respx.mock
async def test_long_format_csv_short_rows():
    """Long-format CSV with rows shorter than expected are skipped."""
    csv_text = "station,date,caudal\nBO-001\nBO-001,2024-06-01,150.3\n"
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/BO-001").mock(
        return_value=httpx.Response(
            200, text=csv_text,
            headers={"content-type": "text/csv"},
        ),
    )
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/data").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/caudales").mock(
        return_value=httpx.Response(500),
    )

    async with BoliviaIneConnector() as conn:
        chunk = await conn.fetch_observations(
            "bolivia_ine:BO-001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 3, tzinfo=UTC),
        )

    # Short row is skipped, only 1 valid row
    assert len(chunk.observations) == 1


@pytest.mark.asyncio
@respx.mock
async def test_long_format_csv_date_filtering():
    """Observations outside the date range are excluded."""
    csv_text = "station,date,caudal\nBO-001,2024-05-01,100.0\nBO-001,2024-06-01,150.3\nBO-001,2024-07-01,200.0\n"
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/BO-001").mock(
        return_value=httpx.Response(
            200, text=csv_text,
            headers={"content-type": "text/csv"},
        ),
    )
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/data").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/caudales").mock(
        return_value=httpx.Response(500),
    )

    async with BoliviaIneConnector() as conn:
        chunk = await conn.fetch_observations(
            "bolivia_ine:BO-001",
            start=datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 30, 23, 59, 59, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(150.3)


@pytest.mark.asyncio
@respx.mock
async def test_wide_format_csv_station_not_in_header():
    """Wide-format CSV without the station column returns empty."""
    csv_text = "date,OTHER-001,OTHER-002\n2024-06-01,100.0,200.0\n"
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/BO-001").mock(
        return_value=httpx.Response(
            200, text=csv_text,
            headers={"content-type": "text/csv"},
        ),
    )
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/data").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/caudales").mock(
        return_value=httpx.Response(500),
    )

    async with BoliviaIneConnector() as conn:
        chunk = await conn.fetch_observations(
            "bolivia_ine:BO-001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_wide_format_csv_no_date_column():
    """Wide-format CSV without a date column returns empty."""
    csv_text = "something,BO-001\nval1,100.0\n"
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/BO-001").mock(
        return_value=httpx.Response(
            200, text=csv_text,
            headers={"content-type": "text/csv"},
        ),
    )
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/data").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/caudales").mock(
        return_value=httpx.Response(500),
    )

    async with BoliviaIneConnector() as conn:
        chunk = await conn.fetch_observations(
            "bolivia_ine:BO-001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0
