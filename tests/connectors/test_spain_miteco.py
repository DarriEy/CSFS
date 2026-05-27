"""Tests for Spain MITECO/CEDEX connector with respx mocks."""

import zipfile
from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from csfs.connectors.spain_miteco import (
    _SEED_STATIONS,
    MITECO_DOWNLOAD_PATH,
    MITECO_STATION_KMZ,
    SpainMITECOConnector,
)

# ---------------------------------------------------------------------------
# Sample CSV data (semicolon-delimited, Spanish headers)
# ---------------------------------------------------------------------------

SAMPLE_CSV_SEMICOLON = (
    "estacion;fecha;caudal;calidad\n"
    "9001;01/01/2020;12.34;0\n"
    "9001;02/01/2020;15.50;1\n"
    "9001;03/01/2020;;3\n"
    "9001;04/01/2020;18.20;\n"
    "9002;01/01/2020;99.00;0\n"
)

SAMPLE_CSV_COMMA_DELIMITED = (
    "estacion,fecha,caudal,calidad\n"
    "9001,01/01/2020,12.34,0\n"
    "9001,02/01/2020,15.50,1\n"
)

SAMPLE_CSV_ISO_DATES = (
    "estacion;fecha;caudal\n"
    "9001;2020-01-01;25.00\n"
    "9001;2020-01-02;26.50\n"
)

SAMPLE_CSV_SPANISH_DECIMALS = (
    "estacion;fecha;caudal\n"
    "9001;01/01/2020;12,34\n"
    "9001;02/01/2020;15,50\n"
)


def _make_yearbook_zip(
    csv_content: str,
    csv_name: str = "anuario_2020.csv",
) -> bytes:
    """Create an in-memory ZIP file containing a CSV."""
    buf = __import__("io").BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(csv_name, csv_content)
    return buf.getvalue()


# ---------------------------------------------------------------------------
# Station listing tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_stations_returns_seed_list():
    """Default mode returns the curated seed catalogue (no network)."""
    async with SpainMITECOConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    for station in stations:
        assert station.provider == "spain_miteco"
        assert station.id.startswith("spain_miteco:")
        assert station.country_code == "ES"
        assert station.latitude != 0.0 or station.longitude != 0.0


@pytest.mark.asyncio
async def test_seed_station_fields():
    """Seed stations have correct field values from the seed list."""
    async with SpainMITECOConnector() as conn:
        stations = await conn.fetch_stations()

    ebro = next(s for s in stations if s.native_id == "9001")
    assert ebro.name == "EBRO EN MIRANDA DE EBRO"
    assert ebro.river == "EBRO"
    assert ebro.latitude == pytest.approx(42.68)
    assert ebro.longitude == pytest.approx(-2.95)
    assert ebro.catchment_area_km2 == pytest.approx(3327.0)
    assert ebro.id == "spain_miteco:9001"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_verify_endpoint():
    """When verify_endpoint=True, connector pings the download URL."""
    respx.get(
        f"https://www.mapama.gob.es{MITECO_DOWNLOAD_PATH}",
        params={"f": MITECO_STATION_KMZ},
    ).mock(
        return_value=httpx.Response(200, content=b"fake-kmz"),
    )

    config = {"verify_endpoint": True}
    async with SpainMITECOConnector(config=config) as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)


# ---------------------------------------------------------------------------
# Observation / file-parsing tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_no_data_dir():
    """Without data_dir configured, returns empty chunk with guidance."""
    async with SpainMITECOConnector() as conn:
        chunk = await conn.fetch_observations(
            "spain_miteco:9001",
            start=datetime(2020, 1, 1, tzinfo=UTC),
            end=datetime(2020, 1, 31, tzinfo=UTC),
        )

    assert chunk.station_id == "spain_miteco:9001"
    assert chunk.provider == "spain_miteco"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_from_csv(tmp_path: Path):
    """Parse semicolon-delimited CSV with Spanish headers."""
    csv_file = tmp_path / "anuario_2020.csv"
    csv_file.write_text(SAMPLE_CSV_SEMICOLON, encoding="utf-8")

    config = {"data_dir": str(tmp_path)}
    async with SpainMITECOConnector(config=config) as conn:
        chunk = await conn.fetch_observations(
            "spain_miteco:9001",
            start=datetime(2020, 1, 1, tzinfo=UTC),
            end=datetime(2020, 1, 4, tzinfo=UTC),
        )

    # Station 9001 has 4 rows; station 9002 is filtered out
    assert len(chunk.observations) == 4

    # First obs: good quality, valid discharge
    assert chunk.observations[0].discharge_m3s == pytest.approx(12.34)
    assert chunk.observations[0].quality.value == "good"

    # Third obs: empty discharge -> missing
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"

    # Fourth obs: no quality string -> raw
    assert chunk.observations[3].discharge_m3s == pytest.approx(18.20)
    assert chunk.observations[3].quality.value == "raw"


@pytest.mark.asyncio
async def test_fetch_observations_from_zip(tmp_path: Path):
    """Parse CSV embedded inside a yearbook ZIP archive."""
    zip_data = _make_yearbook_zip(SAMPLE_CSV_SEMICOLON)
    zip_path = tmp_path / "TablaAnuario2020-21.zip"
    zip_path.write_bytes(zip_data)

    config = {"data_dir": str(tmp_path)}
    async with SpainMITECOConnector(config=config) as conn:
        chunk = await conn.fetch_observations(
            "spain_miteco:9001",
            start=datetime(2020, 1, 1, tzinfo=UTC),
            end=datetime(2020, 1, 4, tzinfo=UTC),
        )

    assert len(chunk.observations) == 4
    assert chunk.observations[0].discharge_m3s == pytest.approx(12.34)


@pytest.mark.asyncio
async def test_fetch_observations_date_filtering(tmp_path: Path):
    """Only observations within [start, end] are returned."""
    csv_file = tmp_path / "data.csv"
    csv_file.write_text(SAMPLE_CSV_SEMICOLON, encoding="utf-8")

    config = {"data_dir": str(tmp_path)}
    async with SpainMITECOConnector(config=config) as conn:
        chunk = await conn.fetch_observations(
            "spain_miteco:9001",
            start=datetime(2020, 1, 2, tzinfo=UTC),
            end=datetime(2020, 1, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    dates = [obs.timestamp.day for obs in chunk.observations]
    assert dates == [2, 3]


@pytest.mark.asyncio
async def test_fetch_observations_spanish_decimal_commas(
    tmp_path: Path,
):
    """Discharge values with comma decimal separators are parsed."""
    csv_file = tmp_path / "data.csv"
    csv_file.write_text(
        SAMPLE_CSV_SPANISH_DECIMALS, encoding="utf-8",
    )

    config = {"data_dir": str(tmp_path)}
    async with SpainMITECOConnector(config=config) as conn:
        chunk = await conn.fetch_observations(
            "spain_miteco:9001",
            start=datetime(2020, 1, 1, tzinfo=UTC),
            end=datetime(2020, 1, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(12.34)
    assert chunk.observations[1].discharge_m3s == pytest.approx(15.50)


@pytest.mark.asyncio
async def test_fetch_observations_iso_dates(tmp_path: Path):
    """CSV with ISO date format (yyyy-mm-dd) is handled correctly."""
    csv_file = tmp_path / "data.csv"
    csv_file.write_text(SAMPLE_CSV_ISO_DATES, encoding="utf-8")

    config = {"data_dir": str(tmp_path)}
    async with SpainMITECOConnector(config=config) as conn:
        chunk = await conn.fetch_observations(
            "spain_miteco:9001",
            start=datetime(2020, 1, 1, tzinfo=UTC),
            end=datetime(2020, 1, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(25.00)


@pytest.mark.asyncio
async def test_fetch_observations_bad_zip_raises(tmp_path: Path):
    """Invalid ZIP data raises DataFormatError."""
    bad_zip = tmp_path / "corrupt.zip"
    bad_zip.write_bytes(b"not-a-zip-file")

    from csfs.core.exceptions import DataFormatError

    config = {"data_dir": str(tmp_path)}
    async with SpainMITECOConnector(config=config) as conn:
        with pytest.raises(DataFormatError, match="Failed to read"):
            await conn.fetch_observations(
                "spain_miteco:9001",
                start=datetime(2020, 1, 1, tzinfo=UTC),
                end=datetime(2020, 1, 31, tzinfo=UTC),
            )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_verify_endpoint_failure():
    """When verify_endpoint fails, stations are still returned."""
    respx.get(
        f"https://www.mapama.gob.es{MITECO_DOWNLOAD_PATH}",
        params={"f": MITECO_STATION_KMZ},
    ).mock(
        return_value=httpx.Response(500),
    )

    config = {"verify_endpoint": True}
    async with SpainMITECOConnector(config=config) as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)


@pytest.mark.asyncio
async def test_fetch_observations_comma_delimited(tmp_path: Path):
    """Parse comma-delimited CSV files."""
    csv_file = tmp_path / "anuario_comma.csv"
    csv_file.write_text(SAMPLE_CSV_COMMA_DELIMITED, encoding="utf-8")

    config = {"data_dir": str(tmp_path)}
    async with SpainMITECOConnector(config=config) as conn:
        chunk = await conn.fetch_observations(
            "spain_miteco:9001",
            start=datetime(2020, 1, 1, tzinfo=UTC),
            end=datetime(2020, 1, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2


@pytest.mark.asyncio
async def test_fetch_observations_no_header_returns_empty(tmp_path: Path):
    """CSV with no recognizable headers returns empty observations."""
    csv_file = tmp_path / "bad_header.csv"
    csv_file.write_text(
        "col_a;col_b;col_c\n1;2;3\n",
        encoding="utf-8",
    )

    config = {"data_dir": str(tmp_path)}
    async with SpainMITECOConnector(config=config) as conn:
        chunk = await conn.fetch_observations(
            "spain_miteco:9001",
            start=datetime(2020, 1, 1, tzinfo=UTC),
            end=datetime(2020, 12, 31, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_no_matching_station(tmp_path: Path):
    """Station column filtering returns only matching station rows."""
    csv_file = tmp_path / "anuario.csv"
    csv_file.write_text(SAMPLE_CSV_SEMICOLON, encoding="utf-8")

    config = {"data_dir": str(tmp_path)}
    async with SpainMITECOConnector(config=config) as conn:
        chunk = await conn.fetch_observations(
            "spain_miteco:9999",  # station not in file
            start=datetime(2020, 1, 1, tzinfo=UTC),
            end=datetime(2020, 1, 31, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_negative_discharge_is_missing(
    tmp_path: Path,
):
    """Negative discharge values (sentinel) are treated as missing."""
    csv_content = (
        "estacion;fecha;caudal\n"
        "9001;01/01/2020;-999.0\n"
        "9001;02/01/2020;12.34\n"
    )
    csv_file = tmp_path / "data.csv"
    csv_file.write_text(csv_content, encoding="utf-8")

    config = {"data_dir": str(tmp_path)}
    async with SpainMITECOConnector(config=config) as conn:
        chunk = await conn.fetch_observations(
            "spain_miteco:9001",
            start=datetime(2020, 1, 1, tzinfo=UTC),
            end=datetime(2020, 1, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    # -999.0 should be treated as None (negative sentinel)
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[0].quality.value == "missing"
    assert chunk.observations[1].discharge_m3s == pytest.approx(12.34)


@pytest.mark.asyncio
async def test_fetch_observations_unparseable_date(tmp_path: Path):
    """Rows with unparseable dates are skipped."""
    csv_content = (
        "estacion;fecha;caudal\n"
        "9001;not-a-date;12.34\n"
        "9001;01/01/2020;15.50\n"
    )
    csv_file = tmp_path / "data.csv"
    csv_file.write_text(csv_content, encoding="utf-8")

    config = {"data_dir": str(tmp_path)}
    async with SpainMITECOConnector(config=config) as conn:
        chunk = await conn.fetch_observations(
            "spain_miteco:9001",
            start=datetime(2020, 1, 1, tzinfo=UTC),
            end=datetime(2020, 1, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1


@pytest.mark.asyncio
async def test_fetch_observations_unparseable_discharge(tmp_path: Path):
    """Rows with unparseable discharge values have None discharge."""
    csv_content = (
        "estacion;fecha;caudal\n"
        "9001;01/01/2020;abc\n"
    )
    csv_file = tmp_path / "data.csv"
    csv_file.write_text(csv_content, encoding="utf-8")

    config = {"data_dir": str(tmp_path)}
    async with SpainMITECOConnector(config=config) as conn:
        chunk = await conn.fetch_observations(
            "spain_miteco:9001",
            start=datetime(2020, 1, 1, tzinfo=UTC),
            end=datetime(2020, 1, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[0].quality.value == "missing"


@pytest.mark.asyncio
async def test_fetch_observations_empty_date(tmp_path: Path):
    """Rows with empty date string are skipped."""
    csv_content = (
        "estacion;fecha;caudal\n"
        "9001;;12.34\n"
        "9001;01/01/2020;15.50\n"
    )
    csv_file = tmp_path / "data.csv"
    csv_file.write_text(csv_content, encoding="utf-8")

    config = {"data_dir": str(tmp_path)}
    async with SpainMITECOConnector(config=config) as conn:
        chunk = await conn.fetch_observations(
            "spain_miteco:9001",
            start=datetime(2020, 1, 1, tzinfo=UTC),
            end=datetime(2020, 1, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1


@pytest.mark.asyncio
async def test_fetch_observations_quality_column_mapping(
    tmp_path: Path,
):
    """Quality values 2 and 3 map to ESTIMATED and SUSPECT."""
    csv_content = (
        "estacion;fecha;caudal;calidad\n"
        "9001;01/01/2020;12.34;2\n"
        "9001;02/01/2020;15.50;3\n"
    )
    csv_file = tmp_path / "data.csv"
    csv_file.write_text(csv_content, encoding="utf-8")

    config = {"data_dir": str(tmp_path)}
    async with SpainMITECOConnector(config=config) as conn:
        chunk = await conn.fetch_observations(
            "spain_miteco:9001",
            start=datetime(2020, 1, 1, tzinfo=UTC),
            end=datetime(2020, 1, 2, tzinfo=UTC),
        )

    assert chunk.observations[0].quality.value == "estimated"
    assert chunk.observations[1].quality.value == "suspect"


@pytest.mark.asyncio
async def test_fetch_observations_csv_no_station_column(tmp_path: Path):
    """CSV without station column includes all rows."""
    csv_content = (
        "fecha;caudal\n"
        "01/01/2020;12.34\n"
        "02/01/2020;15.50\n"
    )
    csv_file = tmp_path / "data.csv"
    csv_file.write_text(csv_content, encoding="utf-8")

    config = {"data_dir": str(tmp_path)}
    async with SpainMITECOConnector(config=config) as conn:
        chunk = await conn.fetch_observations(
            "spain_miteco:9001",
            start=datetime(2020, 1, 1, tzinfo=UTC),
            end=datetime(2020, 1, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2


@pytest.mark.asyncio
async def test_fetch_observations_zip_non_csv_skipped(tmp_path: Path):
    """Non-CSV files inside ZIP archives are skipped."""
    buf = __import__("io").BytesIO()
    with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
        zf.writestr("readme.txt", "Not a CSV file")
        zf.writestr("data.csv", SAMPLE_CSV_SEMICOLON)
    zip_path = tmp_path / "yearbook.zip"
    zip_path.write_bytes(buf.getvalue())

    config = {"data_dir": str(tmp_path)}
    async with SpainMITECOConnector(config=config) as conn:
        chunk = await conn.fetch_observations(
            "spain_miteco:9001",
            start=datetime(2020, 1, 1, tzinfo=UTC),
            end=datetime(2020, 1, 4, tzinfo=UTC),
        )

    assert len(chunk.observations) == 4


@pytest.mark.asyncio
async def test_find_column_returns_none_for_no_match():
    """_find_column returns None when no candidates match."""
    result = SpainMITECOConnector._find_column(
        {"foo": "Foo", "bar": "Bar"},
        ("baz", "qux"),
    )
    assert result is None


@pytest.mark.asyncio
async def test_parse_date_returns_none_for_bad_input():
    """_parse_date returns None for unrecognized formats."""
    result = SpainMITECOConnector._parse_date("not-a-date")
    assert result is None


@pytest.mark.asyncio
async def test_parse_discharge_empty_string():
    """_parse_discharge returns None for empty string."""
    result = SpainMITECOConnector._parse_discharge("")
    assert result is None
