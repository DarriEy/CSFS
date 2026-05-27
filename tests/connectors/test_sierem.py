"""Tests for SIEREM connector with file-based parsing and respx mocks."""

from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from csfs.connectors.sierem import (
    _SEED_STATIONS,
    SIEREMConnector,
)

# ---------------------------------------------------------------------------
# Sample data files
# ---------------------------------------------------------------------------

SAMPLE_SIEREM_CSV_SEMICOLON = """\
# SIEREM discharge data
# Station: SIEREM-1270700103
# River: NIGER
# Country: ML
# Unit: m3/s
date;discharge;flag
1965-01-01;1250.0;0
1965-01-02;1180.5;0
1965-01-03;-999.0;
1965-01-04;1100.2;1
1965-01-05;980.0;2
"""

SAMPLE_SIEREM_CSV_COMMA = """\
# SIEREM data
1965-01-01,1250.0,0
1965-01-02,1180.5,0
"""

SAMPLE_SIEREM_CSV_SLASH_DATE = """\
# SIEREM data with French date format
01/01/1965;1250.0;0
02/01/1965;1180.5;0
"""

MOCK_DATAVERSE_RESPONSE = {
    "status": "OK",
    "data": {
        "id": 12345,
        "persistentUrl": "https://doi.org/10.23708/L4XD4B",
        "latestVersion": {
            "versionState": "RELEASED",
        },
    },
}

# ---------------------------------------------------------------------------
# Station listing tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_stations_seed_default():
    """Default mode returns the curated seed catalogue (no network)."""
    async with SIEREMConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    first = stations[0]
    assert first.provider == "sierem"
    assert first.id.startswith("sierem:")
    assert first.country_code in (
        "BF", "BJ", "CF", "CG", "CI", "CM", "GA", "GN",
        "ML", "MR", "NE", "SN", "TD", "TG",
    )


@pytest.mark.asyncio
async def test_seed_station_ids_are_canonical():
    """Every seed station has a properly formatted CSFS station ID."""
    async with SIEREMConnector() as conn:
        stations = await conn.fetch_stations()

    for station in stations:
        assert station.id == f"sierem:{station.native_id}"
        assert station.provider == "sierem"
        assert station.latitude != 0.0 or station.longitude != 0.0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_with_doi_verification():
    """When verify_doi=True, connector queries DataVerse API."""
    route = respx.get("https://dataverse.ird.fr/api/datasets/:persistentId/").mock(
        return_value=httpx.Response(200, json=MOCK_DATAVERSE_RESPONSE),
    )

    async with SIEREMConnector(config={"verify_doi": True}) as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    assert route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_doi_failure_still_returns_seed():
    """If DataVerse verification fails, seed list is still returned."""
    respx.get("https://dataverse.ird.fr/api/datasets/:persistentId/").mock(
        return_value=httpx.Response(500),
    )

    async with SIEREMConnector(config={"verify_doi": True}) as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)


# ---------------------------------------------------------------------------
# Observation / file-parsing tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_no_data_dir():
    """Without data_dir configured, returns empty chunk with guidance."""
    async with SIEREMConnector() as conn:
        chunk = await conn.fetch_observations(
            "sierem:SIEREM-1270700103",
            start=datetime(1965, 1, 1, tzinfo=UTC),
            end=datetime(1965, 1, 5, tzinfo=UTC),
        )

    assert chunk.station_id == "sierem:SIEREM-1270700103"
    assert chunk.provider == "sierem"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_file_not_found(tmp_path: Path):
    """When data_dir exists but file is missing, returns empty chunk."""
    async with SIEREMConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "sierem:SIEREM-9999999",
            start=datetime(1965, 1, 1, tzinfo=UTC),
            end=datetime(1965, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_parses_semicolon_csv(tmp_path: Path):
    """Full parse of a semicolon-delimited SIEREM file."""
    data_file = tmp_path / "SIEREM-1270700103.csv"
    data_file.write_text(SAMPLE_SIEREM_CSV_SEMICOLON, encoding="utf-8")

    async with SIEREMConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "sierem:SIEREM-1270700103",
            start=datetime(1965, 1, 1, tzinfo=UTC),
            end=datetime(1965, 1, 5, tzinfo=UTC),
        )

    assert chunk.station_id == "sierem:SIEREM-1270700103"
    assert chunk.provider == "sierem"
    assert len(chunk.observations) == 5

    # First obs: original value, good quality
    assert chunk.observations[0].discharge_m3s == pytest.approx(1250.0)
    assert chunk.observations[0].quality.value == "good"

    # Third obs: -999.0 -> missing
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"

    # Fourth obs: estimated flag
    assert chunk.observations[3].discharge_m3s == pytest.approx(1100.2)
    assert chunk.observations[3].quality.value == "estimated"

    # Fifth obs: suspect flag
    assert chunk.observations[4].discharge_m3s == pytest.approx(980.0)
    assert chunk.observations[4].quality.value == "suspect"


@pytest.mark.asyncio
async def test_fetch_observations_date_filtering(tmp_path: Path):
    """Only observations within [start, end] are returned."""
    data_file = tmp_path / "SIEREM-1270700103.csv"
    data_file.write_text(SAMPLE_SIEREM_CSV_SEMICOLON, encoding="utf-8")

    async with SIEREMConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "sierem:SIEREM-1270700103",
            start=datetime(1965, 1, 2, tzinfo=UTC),
            end=datetime(1965, 1, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    dates = [obs.timestamp.day for obs in chunk.observations]
    assert dates == [2, 3]


@pytest.mark.asyncio
async def test_fetch_observations_comma_delimited(tmp_path: Path):
    """Comma-delimited files are auto-detected and parsed."""
    data_file = tmp_path / "SIEREM-1270700103.csv"
    data_file.write_text(SAMPLE_SIEREM_CSV_COMMA, encoding="utf-8")

    async with SIEREMConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "sierem:SIEREM-1270700103",
            start=datetime(1965, 1, 1, tzinfo=UTC),
            end=datetime(1965, 1, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(1250.0)


@pytest.mark.asyncio
async def test_fetch_observations_french_date_format(tmp_path: Path):
    """dd/mm/yyyy date format is supported."""
    data_file = tmp_path / "SIEREM-1270700103.csv"
    data_file.write_text(SAMPLE_SIEREM_CSV_SLASH_DATE, encoding="utf-8")

    async with SIEREMConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "sierem:SIEREM-1270700103",
            start=datetime(1965, 1, 1, tzinfo=UTC),
            end=datetime(1965, 1, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[0].timestamp.day == 1
    assert chunk.observations[1].timestamp.day == 2


# ---------------------------------------------------------------------------
# Coverage gap tests — file read error
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_file_read_error(tmp_path: Path):
    """OSError when reading a SIEREM file raises ConnectorError."""
    from csfs.core.exceptions import ConnectorError

    data_file = tmp_path / "SIEREM-1270700103.csv"
    data_file.write_text(SAMPLE_SIEREM_CSV_SEMICOLON, encoding="utf-8")
    data_file.chmod(0o000)

    try:
        async with SIEREMConnector(
            config={"data_dir": str(tmp_path)},
        ) as conn:
            with pytest.raises(ConnectorError, match="Cannot read SIEREM file"):
                await conn.fetch_observations(
                    "sierem:SIEREM-1270700103",
                    start=datetime(1965, 1, 1, tzinfo=UTC),
                    end=datetime(1965, 1, 5, tzinfo=UTC),
                )
    finally:
        data_file.chmod(0o644)


# ---------------------------------------------------------------------------
# Coverage gap tests — short data line (< 2 parts)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_short_line_skipped(tmp_path: Path):
    """Data lines with fewer than 2 parts after splitting are skipped."""
    data_file = tmp_path / "SIEREM-1270700103.csv"
    # Delimiter detected as semicolon from first line; second line has no
    # semicolons, so after split it has < 2 parts and is skipped.
    data_file.write_text(
        "# Header\n"
        "1965-01-02;1180.5;0\n"
        "1965-01-03\n",  # no semicolons -> 1 part -> skipped
        encoding="utf-8",
    )

    async with SIEREMConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "sierem:SIEREM-1270700103",
            start=datetime(1965, 1, 1, tzinfo=UTC),
            end=datetime(1965, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(1180.5)


# ---------------------------------------------------------------------------
# Coverage gap tests — bad date in data line
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_bad_date_skipped(tmp_path: Path):
    """Data lines with unparseable dates are skipped."""
    data_file = tmp_path / "SIEREM-1270700103.csv"
    data_file.write_text(
        "# Header\n"
        "bad-date;1250.0;0\n"
        "1965-01-02;1180.5;0\n",
        encoding="utf-8",
    )

    async with SIEREMConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "sierem:SIEREM-1270700103",
            start=datetime(1965, 1, 1, tzinfo=UTC),
            end=datetime(1965, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(1180.5)


# ---------------------------------------------------------------------------
# Coverage gap tests — non-numeric value string
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_non_numeric_value(tmp_path: Path):
    """Non-numeric value strings result in MISSING quality."""
    data_file = tmp_path / "SIEREM-1270700103.csv"
    data_file.write_text(
        "# Header\n"
        "1965-01-01;abc;0\n",
        encoding="utf-8",
    )

    async with SIEREMConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "sierem:SIEREM-1270700103",
            start=datetime(1965, 1, 1, tzinfo=UTC),
            end=datetime(1965, 1, 5, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[0].quality.value == "missing"


# ---------------------------------------------------------------------------
# Coverage gap tests — _parse_date returns None for all formats
# ---------------------------------------------------------------------------


def test_parse_date_returns_none_for_invalid():
    """_parse_date returns None for strings that match no format."""
    assert SIEREMConnector._parse_date("not-a-date") is None
    assert SIEREMConnector._parse_date("") is None


# ---------------------------------------------------------------------------
# Coverage gap tests — _detect_delimiter fallback
# ---------------------------------------------------------------------------


def test_detect_delimiter_no_known_delimiter():
    """_detect_delimiter returns comma when no known delimiter found."""
    assert SIEREMConnector._detect_delimiter("1965-01-01 1250.0 0") == ","
