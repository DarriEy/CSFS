"""Tests for the South Africa DWS connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.south_africa_dws import (
    SouthAfricaDWSConnector,
    _parse_timestamp,
    _safe_float,
)
from csfs.core.exceptions import DataFormatError
from csfs.core.models import QualityFlag

# ── Mock responses ─────────────────────────────────────────────────────

MOCK_STATION_JSON = [
    {
        "Station": "A2H012",
        "StationName": "Hartbeespoort Dam",
        "Latitude": -25.748,
        "Longitude": 27.879,
        "River": "Crocodile",
    },
    {
        "Station": "C2H007",
        "StationName": "Vaal River at Orkney",
        "Latitude": -26.988,
        "Longitude": 26.667,
        "River": "Vaal",
    },
]

MOCK_STATION_HTML = """
<html><body><table>
<tr><th>Station</th><th>Name</th><th>Lat</th><th>Lon</th><th>River</th></tr>
<tr><td>A2H012</td><td>Hartbeespoort Dam</td><td>-25.748</td><td>27.879</td><td>Crocodile</td></tr>
<tr><td>C2H007</td><td>Vaal River at Orkney</td><td>-26.988</td><td>26.667</td><td>Vaal</td></tr>
</table></body></html>
"""

MOCK_OBSERVATIONS_JSON = [
    {"Date": "2024-06-01T12:00:00", "Value": 45.3},
    {"Date": "2024-06-01T12:15:00", "Value": 46.1},
    {"Date": "2024-06-01T12:30:00", "Value": None},
]

MOCK_OBSERVATIONS_WRAPPED_JSON = '{"Data": ' + (
    '[{"Date": "2024-06-01", "Value": 50.0}]}'
)

MOCK_OBSERVATIONS_CSV = """\
Date,Value
2024-06-01,45.3
2024-06-02,46.1
"""

BASE = "https://www.dws.gov.za/Hydrology/Verified"


# ── Station tests ──────────────────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_from_json_catalog():
    """Live JSON catalog is parsed correctly."""
    # Wrap JSON inside a minimal page so the regex finds it
    body = str(MOCK_STATION_JSON).replace("'", '"')
    respx.get(f"{BASE}/HyDataSets.aspx").mock(
        return_value=httpx.Response(200, text=body),
    )

    async with SouthAfricaDWSConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"A2H012", "C2H007"}

    s = next(s for s in stations if s.native_id == "A2H012")
    assert s.id == "south_africa_dws:A2H012"
    assert s.provider == "south_africa_dws"
    assert s.country_code == "ZA"
    assert s.river == "Crocodile"
    assert s.latitude == pytest.approx(-25.748)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_from_html_catalog():
    """Live HTML catalog is parsed correctly."""
    respx.get(f"{BASE}/HyDataSets.aspx").mock(
        return_value=httpx.Response(200, text=MOCK_STATION_HTML),
    )

    async with SouthAfricaDWSConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    s = next(s for s in stations if s.native_id == "C2H007")
    assert s.name == "Vaal River at Orkney"
    assert s.river == "Vaal"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_falls_back_to_seed_list():
    """When the live catalog is unreachable, the seed list is returned."""
    respx.get(f"{BASE}/HyDataSets.aspx").mock(
        return_value=httpx.Response(500, text="Server Error"),
    )

    async with SouthAfricaDWSConnector() as conn:
        stations = await conn.fetch_stations()

    # Seed list has 8 stations
    assert len(stations) == 8
    native_ids = {s.native_id for s in stations}
    assert "A2H012" in native_ids
    assert "D1H009" in native_ids
    for s in stations:
        assert s.provider == "south_africa_dws"
        assert s.country_code == "ZA"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_seed_on_bad_gateway():
    """Seed list is used when the catalog endpoint returns a bad gateway."""
    respx.get(f"{BASE}/HyDataSets.aspx").mock(
        return_value=httpx.Response(502, text="Bad Gateway"),
    )

    async with SouthAfricaDWSConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 8


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_empty_catalog_returns_seed():
    """Empty response body causes fallback to seed list."""
    respx.get(f"{BASE}/HyDataSets.aspx").mock(
        return_value=httpx.Response(200, text=""),
    )

    async with SouthAfricaDWSConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 8


# ── Observation tests ──────────────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_json():
    """JSON observations are parsed correctly."""
    import json

    respx.get(f"{BASE}/HyDataValues.aspx").mock(
        return_value=httpx.Response(200, text=json.dumps(MOCK_OBSERVATIONS_JSON)),
    )

    async with SouthAfricaDWSConnector() as conn:
        chunk = await conn.fetch_observations(
            "south_africa_dws:A2H012",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "south_africa_dws"
    assert chunk.station_id == "south_africa_dws:A2H012"
    assert len(chunk.observations) == 3

    assert chunk.observations[0].discharge_m3s == pytest.approx(45.3)
    assert chunk.observations[0].quality == QualityFlag.RAW

    # None value -> MISSING
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality == QualityFlag.MISSING


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_wrapped_json():
    """JSON wrapped in {"Data": [...]} is parsed correctly."""
    respx.get(f"{BASE}/HyDataValues.aspx").mock(
        return_value=httpx.Response(200, text=MOCK_OBSERVATIONS_WRAPPED_JSON),
    )

    async with SouthAfricaDWSConnector() as conn:
        chunk = await conn.fetch_observations(
            "south_africa_dws:A2H012",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(50.0)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_csv():
    """CSV observations are parsed correctly."""
    respx.get(f"{BASE}/HyDataValues.aspx").mock(
        return_value=httpx.Response(200, text=MOCK_OBSERVATIONS_CSV),
    )

    async with SouthAfricaDWSConnector() as conn:
        chunk = await conn.fetch_observations(
            "south_africa_dws:A2H012",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 3),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(45.3)
    assert chunk.observations[1].discharge_m3s == pytest.approx(46.1)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty_response():
    """Empty response body returns zero observations."""
    respx.get(f"{BASE}/HyDataValues.aspx").mock(
        return_value=httpx.Response(200, text=""),
    )

    async with SouthAfricaDWSConnector() as conn:
        chunk = await conn.fetch_observations(
            "south_africa_dws:A2H012",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_server_error_returns_empty():
    """Server error returns an empty chunk rather than raising."""
    respx.get(f"{BASE}/HyDataValues.aspx").mock(
        return_value=httpx.Response(500, text="Internal Server Error"),
    )

    async with SouthAfricaDWSConnector() as conn:
        chunk = await conn.fetch_observations(
            "south_africa_dws:A2H012",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_bad_gateway_returns_empty():
    """Bad gateway returns an empty chunk rather than raising."""
    respx.get(f"{BASE}/HyDataValues.aspx").mock(
        return_value=httpx.Response(502, text="Bad Gateway"),
    )

    async with SouthAfricaDWSConnector() as conn:
        chunk = await conn.fetch_observations(
            "south_africa_dws:A2H012",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest():
    """fetch_latest delegates to fetch_observations for the last 24h."""
    import json

    respx.get(f"{BASE}/HyDataValues.aspx").mock(
        return_value=httpx.Response(200, text=json.dumps(MOCK_OBSERVATIONS_JSON)),
    )

    async with SouthAfricaDWSConnector() as conn:
        chunk = await conn.fetch_latest("south_africa_dws:A2H012")

    assert chunk.provider == "south_africa_dws"
    assert len(chunk.observations) == 3


# ── Registration test ──────────────────────────────────────────────────


def test_connector_registered():
    """The connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("south_africa_dws")
    assert cls is SouthAfricaDWSConnector


def test_connector_metadata():
    """Verify class-level attributes."""
    assert SouthAfricaDWSConnector.slug == "south_africa_dws"
    assert SouthAfricaDWSConnector.country_codes == ["ZA"]
    assert "dws.gov.za" in SouthAfricaDWSConnector.base_url


# ── Helper function tests ─────────────────────────────────────────────


class TestParseTimestamp:
    def test_iso_format(self):
        ts = _parse_timestamp("2024-06-01T12:00:00")
        assert ts.year == 2024
        assert ts.month == 6
        assert ts.hour == 12

    def test_date_only(self):
        ts = _parse_timestamp("2024-06-01")
        assert ts.year == 2024
        assert ts.month == 6
        assert ts.day == 1

    def test_slash_format(self):
        ts = _parse_timestamp("2024/06/01")
        assert ts.year == 2024

    def test_dd_mm_yyyy(self):
        ts = _parse_timestamp("01/06/2024")
        assert ts.day == 1
        assert ts.month == 6

    def test_with_timezone(self):
        ts = _parse_timestamp("2024-06-01T12:00:00+02:00")
        assert ts.year == 2024

    def test_invalid_raises(self):
        with pytest.raises(DataFormatError, match="Unparseable timestamp"):
            _parse_timestamp("not-a-date")


class TestSafeFloat:
    def test_valid_float(self):
        assert _safe_float(45.3) == pytest.approx(45.3)

    def test_string_number(self):
        assert _safe_float("123.4") == pytest.approx(123.4)

    def test_none(self):
        assert _safe_float(None) is None

    def test_empty_string(self):
        assert _safe_float("") is None

    def test_non_numeric_string(self):
        assert _safe_float("N/A") is None


# ── Additional coverage tests ────────────────────────────────────────


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest_delegates():
    """fetch_latest calls fetch_observations for last 24h."""
    import json

    respx.get(f"{BASE}/HyDataValues.aspx").mock(
        return_value=httpx.Response(200, text=json.dumps(MOCK_OBSERVATIONS_JSON)),
    )

    async with SouthAfricaDWSConnector() as conn:
        chunk = await conn.fetch_latest("south_africa_dws:A2H012")

    assert chunk.provider == "south_africa_dws"


@pytest.mark.asyncio
@respx.mock
async def test_stations_from_json_missing_station_id_skipped():
    """JSON station entries with empty Station field are skipped."""
    body = '[{"Station": "", "StationName": "Empty", "Latitude": -25.0, "Longitude": 27.0}]'
    respx.get(f"{BASE}/HyDataSets.aspx").mock(
        return_value=httpx.Response(200, text=body),
    )

    async with SouthAfricaDWSConnector() as conn:
        stations = await conn.fetch_stations()

    # Empty station ID skipped, falls to seed
    assert len(stations) == 8


@pytest.mark.asyncio
@respx.mock
async def test_stations_from_json_invalid_lat_lon():
    """Stations with invalid lat/lon default to 0.0."""
    body = '[{"Station": "X1H001", "Latitude": "bad", "Longitude": "bad"}]'
    respx.get(f"{BASE}/HyDataSets.aspx").mock(
        return_value=httpx.Response(200, text=body),
    )

    async with SouthAfricaDWSConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].latitude == 0.0
    assert stations[0].longitude == 0.0


@pytest.mark.asyncio
@respx.mock
async def test_stations_from_html_invalid_coords():
    """HTML station entries with invalid coordinates use defaults."""
    html = """
    <html><body><table>
    <tr><td>A2H012</td><td>Test</td><td>bad</td><td>bad</td><td>River</td></tr>
    </table></body></html>
    """
    respx.get(f"{BASE}/HyDataSets.aspx").mock(
        return_value=httpx.Response(200, text=html),
    )

    async with SouthAfricaDWSConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].latitude == 0.0


@pytest.mark.asyncio
@respx.mock
async def test_observations_json_invalid_timestamp_skipped():
    """JSON observations with invalid timestamps are skipped."""
    import json

    data = [
        {"Date": "not-a-date", "Value": 45.0},
        {"Date": "2024-06-01", "Value": 50.0},
    ]
    respx.get(f"{BASE}/HyDataValues.aspx").mock(
        return_value=httpx.Response(200, text=json.dumps(data)),
    )

    async with SouthAfricaDWSConnector() as conn:
        chunk = await conn.fetch_observations(
            "south_africa_dws:A2H012",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(50.0)


@pytest.mark.asyncio
@respx.mock
async def test_observations_json_missing_date_skipped():
    """JSON observations missing Date field are skipped."""
    import json

    data = [
        {"Value": 45.0},
        {"Date": "2024-06-01", "Value": 50.0},
    ]
    respx.get(f"{BASE}/HyDataValues.aspx").mock(
        return_value=httpx.Response(200, text=json.dumps(data)),
    )

    async with SouthAfricaDWSConnector() as conn:
        chunk = await conn.fetch_observations(
            "south_africa_dws:A2H012",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 1


@pytest.mark.asyncio
@respx.mock
async def test_observations_csv_tab_separated():
    """Tab-separated observations are parsed."""
    body = "Date\tValue\n2024-06-01\t45.3\n2024-06-02\t0.0\n"
    respx.get(f"{BASE}/HyDataValues.aspx").mock(
        return_value=httpx.Response(200, text=body),
    )

    async with SouthAfricaDWSConnector() as conn:
        chunk = await conn.fetch_observations(
            "south_africa_dws:A2H012",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 3),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(45.3)
    assert chunk.observations[1].discharge_m3s == pytest.approx(0.0)
    assert chunk.observations[1].quality == QualityFlag.RAW


@pytest.mark.asyncio
@respx.mock
async def test_observations_csv_invalid_date_skipped():
    """CSV lines with invalid dates are skipped."""
    body = "Date,Value\nnot-a-date,45.3\n2024-06-01,50.0\n"
    respx.get(f"{BASE}/HyDataValues.aspx").mock(
        return_value=httpx.Response(200, text=body),
    )

    async with SouthAfricaDWSConnector() as conn:
        chunk = await conn.fetch_observations(
            "south_africa_dws:A2H012",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 1


@pytest.mark.asyncio
@respx.mock
async def test_observations_csv_single_column_skipped():
    """CSV lines with only one column are skipped."""
    body = "Date,Value\n2024-06-01\n2024-06-02,50.0\n"
    respx.get(f"{BASE}/HyDataValues.aspx").mock(
        return_value=httpx.Response(200, text=body),
    )

    async with SouthAfricaDWSConnector() as conn:
        chunk = await conn.fetch_observations(
            "south_africa_dws:A2H012",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 3),
        )

    assert len(chunk.observations) == 1


class TestParseTimestampAdditional:
    def test_datetime_with_space(self):
        ts = _parse_timestamp("2024-06-01 12:00:00")
        assert ts.year == 2024
        assert ts.hour == 12

    def test_datetime_with_minute_only(self):
        ts = _parse_timestamp("2024-06-01 12:00")
        assert ts.year == 2024

    def test_slash_with_time(self):
        ts = _parse_timestamp("2024/06/01 12:00:00")
        assert ts.year == 2024

    def test_dd_mm_yyyy_with_time(self):
        ts = _parse_timestamp("01/06/2024 12:00:00")
        assert ts.day == 1
