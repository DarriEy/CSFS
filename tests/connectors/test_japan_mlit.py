"""Tests for the Japan MLIT Water Information System connector."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.japan_mlit import _SEED_STATIONS, JapanMlitConnector

# ---------------------------------------------------------------------------
# Mock responses
# ---------------------------------------------------------------------------

MOCK_CSV_RESPONSE = """\
2024-06-01T00:00:00,123.4
2024-06-01T01:00:00,130.2
2024-06-01T02:00:00,--
2024-06-01T03:00:00,128.7
"""

MOCK_WHITESPACE_RESPONSE = """\
# Station data
00:00  45.6
01:00  47.2
02:00  ***
03:00  50.1
"""

MOCK_STATION_HTML = """\
<html><body><table>
<tr><td>305011283018070</td><td>Kurihashi</td></tr>
<tr><td>399999999999999</td><td>New Station</td></tr>
</table></body></html>
"""

MOCK_EMPTY_RESPONSE = ""

BASE_URL = "http://www1.river.go.jp"


# ---------------------------------------------------------------------------
# Station tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_returns_seed_list():
    """Seed stations are always returned even when live discovery fails."""
    # Live discovery returns 500 — should not raise
    respx.get(f"{BASE_URL}/cgi-bin/SiteInfo.exe").mock(
        return_value=httpx.Response(500, text="Server Error"),
    )

    async with JapanMlitConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) >= len(_SEED_STATIONS)
    native_ids = {s.native_id for s in stations}
    assert "305011283018070" in native_ids  # Kurihashi
    assert "303051283015040" in native_ids  # Ojiya


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_seed_fields():
    """Seed stations have correct metadata."""
    respx.get(f"{BASE_URL}/cgi-bin/SiteInfo.exe").mock(
        return_value=httpx.Response(500, text="Error"),
    )

    async with JapanMlitConnector() as conn:
        stations = await conn.fetch_stations()

    kurihashi = next(s for s in stations if s.native_id == "305011283018070")
    assert kurihashi.id == "japan_mlit:305011283018070"
    assert kurihashi.provider == "japan_mlit"
    assert kurihashi.name == "Kurihashi"
    assert kurihashi.country_code == "JP"
    assert kurihashi.river == "Tone River"
    assert kurihashi.latitude == pytest.approx(36.1314)
    assert kurihashi.longitude == pytest.approx(139.7006)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_augments_with_live():
    """Live-discovered stations are merged with the seed list."""
    respx.get(f"{BASE_URL}/cgi-bin/SiteInfo.exe").mock(
        return_value=httpx.Response(200, text=MOCK_STATION_HTML),
    )

    async with JapanMlitConnector() as conn:
        stations = await conn.fetch_stations()

    native_ids = {s.native_id for s in stations}
    # Seed station should be present
    assert "305011283018070" in native_ids
    # New station from live discovery should be added
    assert "399999999999999" in native_ids
    # Total should be seed count + 1 new (Kurihashi is in both, so not duplicated)
    assert len(stations) == len(_SEED_STATIONS) + 1


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_populates_cache():
    """fetch_stations populates the internal station cache."""
    respx.get(f"{BASE_URL}/cgi-bin/SiteInfo.exe").mock(
        return_value=httpx.Response(500, text="Error"),
    )

    async with JapanMlitConnector() as conn:
        await conn.fetch_stations()

    assert "305011283018070" in conn._station_cache
    assert conn._station_cache["305011283018070"].name == "Kurihashi"


# ---------------------------------------------------------------------------
# Observation tests — CSV format
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_csv_format():
    """CSV response is parsed into observations correctly."""
    respx.get(f"{BASE_URL}/cgi-bin/DspFlowData.exe").mock(
        return_value=httpx.Response(200, text=MOCK_CSV_RESPONSE),
    )

    async with JapanMlitConnector() as conn:
        chunk = await conn.fetch_observations(
            "japan_mlit:305011283018070",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert chunk.provider == "japan_mlit"
    assert chunk.station_id == "japan_mlit:305011283018070"
    assert len(chunk.observations) == 4

    # First observation
    assert chunk.observations[0].discharge_m3s == pytest.approx(123.4)
    assert chunk.observations[0].quality.value == "raw"

    # Third observation — missing marker "--"
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_whitespace_format():
    """Whitespace-delimited response is parsed correctly."""
    respx.get(f"{BASE_URL}/cgi-bin/DspFlowData.exe").mock(
        return_value=httpx.Response(200, text=MOCK_WHITESPACE_RESPONSE),
    )

    async with JapanMlitConnector() as conn:
        chunk = await conn.fetch_observations(
            "japan_mlit:305011283018070",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 4

    # First observation — time-only timestamp anchored to query day
    assert chunk.observations[0].discharge_m3s == pytest.approx(45.6)
    assert chunk.observations[0].timestamp.hour == 0
    assert chunk.observations[0].timestamp.minute == 0

    # Third observation — "***" missing marker
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty_response():
    """Empty response returns zero observations."""
    respx.get(f"{BASE_URL}/cgi-bin/DspFlowData.exe").mock(
        return_value=httpx.Response(200, text=MOCK_EMPTY_RESPONSE),
    )

    async with JapanMlitConnector() as conn:
        chunk = await conn.fetch_observations(
            "japan_mlit:305011283018070",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_filters_by_date_range():
    """Only observations within the requested range are included."""
    csv_data = """\
2024-06-01T06:00:00,100.0
2024-06-01T12:00:00,110.0
2024-06-01T18:00:00,120.0
"""
    respx.get(f"{BASE_URL}/cgi-bin/DspFlowData.exe").mock(
        return_value=httpx.Response(200, text=csv_data),
    )

    async with JapanMlitConnector() as conn:
        chunk = await conn.fetch_observations(
            "japan_mlit:305011283018070",
            start=datetime(2024, 6, 1, 10, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 1, 23, 0, 0, tzinfo=UTC),
        )

    # Only 12:00 and 18:00 should be in range
    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(110.0)
    assert chunk.observations[1].discharge_m3s == pytest.approx(120.0)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_server_error_gracefully():
    """Server errors for individual days are logged but don't crash."""
    respx.get(f"{BASE_URL}/cgi-bin/DspFlowData.exe").mock(
        return_value=httpx.Response(500, text="Internal Server Error"),
    )

    async with JapanMlitConnector() as conn:
        chunk = await conn.fetch_observations(
            "japan_mlit:305011283018070",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    # Should return empty, not raise
    assert len(chunk.observations) == 0


# ---------------------------------------------------------------------------
# fetch_latest
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest_delegates_to_observations():
    """fetch_latest fetches last 24 hours of data."""
    respx.get(f"{BASE_URL}/cgi-bin/DspFlowData.exe").mock(
        return_value=httpx.Response(200, text=MOCK_EMPTY_RESPONSE),
    )

    async with JapanMlitConnector() as conn:
        chunk = await conn.fetch_latest("japan_mlit:305011283018070")

    assert chunk.provider == "japan_mlit"
    assert chunk.station_id == "japan_mlit:305011283018070"


# ---------------------------------------------------------------------------
# Parsing edge cases
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_parse_japanese_missing_marker():
    """Japanese missing-data marker '欠測' is handled."""
    csv_data = "2024-06-01T10:00:00,欠測\n"
    respx.get(f"{BASE_URL}/cgi-bin/DspFlowData.exe").mock(
        return_value=httpx.Response(200, text=csv_data),
    )

    async with JapanMlitConnector() as conn:
        chunk = await conn.fetch_observations(
            "japan_mlit:305011283018070",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[0].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_station_html_parse_no_matches():
    """HTML without station patterns returns empty list."""
    respx.get(f"{BASE_URL}/cgi-bin/SiteInfo.exe").mock(
        return_value=httpx.Response(200, text="<html><body>No data</body></html>"),
    )

    async with JapanMlitConnector() as conn:
        stations = await conn.fetch_stations()

    # Should still have the seed stations
    assert len(stations) == len(_SEED_STATIONS)


# ---------------------------------------------------------------------------
# Registration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_station_html_empty_name_skipped():
    """HTML rows with empty station names are skipped."""
    html_with_empty_name = (
        '<html><body><table>'
        '<tr><td>399999999999999</td><td>   </td></tr>'
        '</table></body></html>'
    )
    respx.get(f"{BASE_URL}/cgi-bin/SiteInfo.exe").mock(
        return_value=httpx.Response(200, text=html_with_empty_name),
    )

    async with JapanMlitConnector() as conn:
        stations = await conn.fetch_stations()

    # Only seed stations, no extra from live (empty name skipped)
    assert len(stations) == len(_SEED_STATIONS)


def test_parse_csv_response_unparseable_raises():
    """Text that fails both CSV and whitespace parsing raises DataFormatError."""
    from csfs.core.exceptions import DataFormatError

    conn = JapanMlitConnector()
    with pytest.raises(DataFormatError, match="Unable to parse"):
        conn._parse_csv_response(
            "@@@ garbage data with no timestamps @@@",
            "japan_mlit:305011283018070",
            datetime(2024, 6, 1, tzinfo=UTC),
        )


def test_parse_timestamp_returns_none_for_invalid():
    """_parse_timestamp returns None for unparseable strings."""
    result = JapanMlitConnector._parse_timestamp(
        "not-a-timestamp", datetime(2024, 6, 1, tzinfo=UTC),
    )
    assert result is None


def test_parse_discharge_returns_none_for_non_numeric():
    """_parse_discharge returns None for non-numeric strings."""
    assert JapanMlitConnector._parse_discharge("abc") is None
    assert JapanMlitConnector._parse_discharge("N/A") is None
    assert JapanMlitConnector._parse_discharge("") is None


def test_try_parse_csv_no_valid_observations_returns_none():
    """CSV with no valid timestamps returns None (fallback to whitespace)."""
    conn = JapanMlitConnector()
    result = conn._try_parse_csv(
        "header1,header2\nno-date,100\n",
        "japan_mlit:305011283018070",
        datetime(2024, 6, 1, tzinfo=UTC),
    )
    assert result is None


def test_try_parse_whitespace_no_valid_observations_returns_none():
    """Whitespace text with no valid timestamps returns None."""
    conn = JapanMlitConnector()
    result = conn._try_parse_whitespace(
        "no-time 100\nbad-time 200\n",
        "japan_mlit:305011283018070",
        datetime(2024, 6, 1, tzinfo=UTC),
    )
    assert result is None


@pytest.mark.asyncio
@respx.mock
async def test_whitespace_format_skips_comment_and_blank_lines():
    """Whitespace parser correctly skips comments and blank lines."""
    ws_data = "# Comment\n\n   \n00:00  100.5\n"
    respx.get(f"{BASE_URL}/cgi-bin/DspFlowData.exe").mock(
        return_value=httpx.Response(200, text=ws_data),
    )

    async with JapanMlitConnector() as conn:
        chunk = await conn.fetch_observations(
            "japan_mlit:305011283018070",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(100.5)


def test_connector_is_registered():
    """The connector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("japan_mlit")
    assert cls is JapanMlitConnector
