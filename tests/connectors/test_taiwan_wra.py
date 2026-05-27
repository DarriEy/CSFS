"""Tests for the WRA (Taiwan) connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.taiwan_wra import TaiwanWRAConnector
from csfs.core.exceptions import DataFormatError

BASE = "https://opendata.wra.gov.tw/api/v1"

MOCK_STATIONS_ENGLISH = {
    "data": [
        {
            "StationIdentifier": "1140H055",
            "StationName": "Taipei Bridge",
            "Latitude": 25.0330,
            "Longitude": 121.5654,
            "RiverName": "Tamsui",
            "BasinName": "Tamsui Basin",
            "CatchmentArea": 2726.0,
            "ObservationStartDate": "1965-01-01",
        },
        {
            "StationIdentifier": "1510H067",
            "StationName": "Xizhi",
            "Latitude": 25.0631,
            "Longitude": 121.6437,
            "RiverName": "Keelung",
            "BasinName": "Keelung Basin",
            "CatchmentArea": None,
            "ObservationStartDate": None,
        },
    ]
}

MOCK_STATIONS_CHINESE = {
    "data": [
        {
            "測站代碼": "1140H055",
            "測站名稱": "台北橋",
            "緯度": 25.0330,
            "經度": 121.5654,
            "河川名稱": "淡水河",
        },
    ]
}

MOCK_STATION_MISSING_COORDS = {
    "data": [
        {
            "StationIdentifier": "9999999",
            "StationName": "Bad Station",
        },
    ]
}

MOCK_OBS_ENGLISH = {
    "data": [
        {
            "RecordDate": "2024-06-01",
            "Discharge": 123.4,
            "WaterLevel": 5.2,
        },
        {
            "RecordDate": "2024-06-02",
            "Discharge": 130.0,
            "WaterLevel": 5.5,
        },
        {
            "RecordDate": "2024-06-03",
            "Discharge": None,
            "WaterLevel": None,
        },
    ]
}

MOCK_OBS_CHINESE = {
    "records": [
        {
            "日期": "2024-06-01",
            "流量": 200.5,
            "水位": 3.1,
        },
    ]
}

MOCK_EMPTY = {"data": []}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_english_fields():
    """Stations with English field names are parsed correctly."""
    respx.get(f"{BASE}/RiverFlowStation").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_ENGLISH),
    )

    async with TaiwanWRAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2

    stn = next(s for s in stations if s.native_id == "1140H055")
    assert stn.id == "taiwan_wra:1140H055"
    assert stn.provider == "taiwan_wra"
    assert stn.country_code == "TW"
    assert stn.name == "Taipei Bridge"
    assert stn.river == "Tamsui"
    assert stn.latitude == pytest.approx(25.0330)
    assert stn.longitude == pytest.approx(121.5654)
    assert stn.catchment_area_km2 == pytest.approx(2726.0)
    assert stn.record_start is not None


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_chinese_fields():
    """Stations with Chinese field names are parsed correctly."""
    respx.get(f"{BASE}/RiverFlowStation").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_CHINESE),
    )

    async with TaiwanWRAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    stn = stations[0]
    assert stn.native_id == "1140H055"
    assert stn.name == "台北橋"
    assert stn.river == "淡水河"
    assert stn.latitude == pytest.approx(25.0330)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_missing_coords():
    """Stations without lat/lon are skipped."""
    respx.get(f"{BASE}/RiverFlowStation").mock(
        return_value=httpx.Response(200, json=MOCK_STATION_MISSING_COORDS),
    )

    async with TaiwanWRAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_empty():
    """An empty data list returns no stations."""
    respx.get(f"{BASE}/RiverFlowStation").mock(
        return_value=httpx.Response(200, json=MOCK_EMPTY),
    )

    async with TaiwanWRAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_english():
    """Observations with English field names are parsed correctly."""
    respx.get(f"{BASE}/RiverFlowData").mock(
        return_value=httpx.Response(200, json=MOCK_OBS_ENGLISH),
    )

    async with TaiwanWRAConnector() as conn:
        chunk = await conn.fetch_observations(
            "taiwan_wra:1140H055",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 4),
        )

    assert chunk.provider == "taiwan_wra"
    assert chunk.station_id == "taiwan_wra:1140H055"
    assert len(chunk.observations) == 3

    obs0 = chunk.observations[0]
    assert obs0.discharge_m3s == pytest.approx(123.4)
    assert obs0.quality.value == "raw"

    obs2 = chunk.observations[2]
    assert obs2.discharge_m3s is None
    assert obs2.quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_chinese():
    """Observations with Chinese field names are parsed correctly."""
    respx.get(f"{BASE}/RiverFlowData").mock(
        return_value=httpx.Response(200, json=MOCK_OBS_CHINESE),
    )

    async with TaiwanWRAConnector() as conn:
        chunk = await conn.fetch_observations(
            "taiwan_wra:1140H055",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(200.5)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_falls_back_to_water_level():
    """When Discharge is absent, WaterLevel is used as the value."""
    payload = {
        "data": [
            {
                "RecordDate": "2024-06-01",
                "Discharge": None,
                "WaterLevel": 3.7,
            },
        ]
    }
    respx.get(f"{BASE}/RiverFlowData").mock(
        return_value=httpx.Response(200, json=payload),
    )

    async with TaiwanWRAConnector() as conn:
        chunk = await conn.fetch_observations(
            "taiwan_wra:1140H055",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(3.7)
    assert chunk.observations[0].quality.value == "raw"


@pytest.mark.asyncio
@respx.mock
async def test_registry_slug():
    """The connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("taiwan_wra")
    assert cls is TaiwanWRAConnector


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest():
    """fetch_latest fetches the most recent 24 hours."""
    respx.get(f"{BASE}/RiverFlowData").mock(
        return_value=httpx.Response(200, json=MOCK_OBS_ENGLISH),
    )

    async with TaiwanWRAConnector() as conn:
        chunk = await conn.fetch_latest("taiwan_wra:1140H055")

    assert chunk.provider == "taiwan_wra"
    assert len(chunk.observations) == 3


@pytest.mark.asyncio
@respx.mock
async def test_safe_json_error():
    """Non-JSON response raises DataFormatError."""
    respx.get(f"{BASE}/RiverFlowStation").mock(
        return_value=httpx.Response(
            200, text="not json", headers={"content-type": "text/plain"},
        ),
    )

    async with TaiwanWRAConnector() as conn:
        with pytest.raises(DataFormatError, match="not valid JSON"):
            await conn.fetch_stations()


@pytest.mark.asyncio
@respx.mock
async def test_extract_items_bare_list():
    """Payload that is a bare list is extracted directly."""
    payload = [
        {
            "StationIdentifier": "1140H055",
            "StationName": "Taipei Bridge",
            "Latitude": 25.0330,
            "Longitude": 121.5654,
            "RiverName": "Tamsui",
        },
    ]
    respx.get(f"{BASE}/RiverFlowStation").mock(
        return_value=httpx.Response(200, json=payload),
    )

    async with TaiwanWRAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "1140H055"


@pytest.mark.asyncio
@respx.mock
async def test_extract_items_response_data_nested():
    """Payload with responseData.data nesting is extracted correctly."""
    payload = {
        "responseData": {
            "data": [
                {
                    "StationIdentifier": "1140H055",
                    "StationName": "Taipei Bridge",
                    "Latitude": 25.0330,
                    "Longitude": 121.5654,
                },
            ],
        },
    }
    respx.get(f"{BASE}/RiverFlowStation").mock(
        return_value=httpx.Response(200, json=payload),
    )

    async with TaiwanWRAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1


@pytest.mark.asyncio
@respx.mock
async def test_extract_items_unknown_shape_logs_warning():
    """Unknown payload shape returns empty list with warning."""
    payload = {
        "unknownKey": "unknownValue",
    }
    respx.get(f"{BASE}/RiverFlowStation").mock(
        return_value=httpx.Response(200, json=payload),
    )

    async with TaiwanWRAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_station_skip_no_native_id():
    """Stations with empty native_id are skipped."""
    payload = {
        "data": [
            {
                "StationIdentifier": "",
                "StationName": "No ID",
                "Latitude": 25.0,
                "Longitude": 121.0,
            },
            {
                "StationIdentifier": None,
                "StationName": "Null ID",
                "Latitude": 25.0,
                "Longitude": 121.0,
            },
        ],
    }
    respx.get(f"{BASE}/RiverFlowStation").mock(
        return_value=httpx.Response(200, json=payload),
    )

    async with TaiwanWRAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_station_parse_error_skipped():
    """Station entries that raise ValueError during parsing are skipped."""
    payload = {
        "data": [
            {
                "StationIdentifier": "BAD001",
                "StationName": "Bad",
                "Latitude": "not-a-number",
                "Longitude": 121.0,
            },
            {
                "StationIdentifier": "1140H055",
                "StationName": "Good",
                "Latitude": 25.0,
                "Longitude": 121.0,
            },
        ],
    }
    respx.get(f"{BASE}/RiverFlowStation").mock(
        return_value=httpx.Response(200, json=payload),
    )

    async with TaiwanWRAConnector() as conn:
        stations = await conn.fetch_stations()

    # BAD001 has bad latitude -> skipped at coord check
    # Good station passes
    assert len(stations) == 1
    assert stations[0].native_id == "1140H055"


@pytest.mark.asyncio
@respx.mock
async def test_observation_no_date_skipped():
    """Observation entries without a date are skipped."""
    payload = {
        "data": [
            {
                "RecordDate": "",
                "Discharge": 100.0,
            },
            {
                "RecordDate": "2024-06-01",
                "Discharge": 200.0,
            },
        ],
    }
    respx.get(f"{BASE}/RiverFlowData").mock(
        return_value=httpx.Response(200, json=payload),
    )

    async with TaiwanWRAConnector() as conn:
        chunk = await conn.fetch_observations(
            "taiwan_wra:1140H055",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(200.0)


@pytest.mark.asyncio
@respx.mock
async def test_observation_compact_date_format():
    """Observation with compact date format (YYYYMMDD) is parsed."""
    payload = {
        "data": [
            {
                "RecordDate": "20240601",
                "Discharge": 150.0,
            },
        ],
    }
    respx.get(f"{BASE}/RiverFlowData").mock(
        return_value=httpx.Response(200, json=payload),
    )

    async with TaiwanWRAConnector() as conn:
        chunk = await conn.fetch_observations(
            "taiwan_wra:1140H055",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(150.0)


@pytest.mark.asyncio
@respx.mock
async def test_observation_iso_offset_date():
    """Observation with ISO 8601 offset (e.g., +08:00) is parsed via fromisoformat."""
    payload = {
        "data": [
            {
                "RecordDate": "2024-06-01T12:00:00+08:00",
                "Discharge": 175.0,
            },
        ],
    }
    respx.get(f"{BASE}/RiverFlowData").mock(
        return_value=httpx.Response(200, json=payload),
    )

    async with TaiwanWRAConnector() as conn:
        chunk = await conn.fetch_observations(
            "taiwan_wra:1140H055",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(175.0)


@pytest.mark.asyncio
@respx.mock
async def test_observation_completely_bad_date():
    """Observations with completely unparseable dates are skipped."""
    payload = {
        "data": [
            {
                "RecordDate": "not-a-date-at-all",
                "Discharge": 100.0,
            },
            {
                "RecordDate": "2024-06-01",
                "Discharge": 200.0,
            },
        ],
    }
    respx.get(f"{BASE}/RiverFlowData").mock(
        return_value=httpx.Response(200, json=payload),
    )

    async with TaiwanWRAConnector() as conn:
        chunk = await conn.fetch_observations(
            "taiwan_wra:1140H055",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 1


def test_parse_date_multiple_formats():
    """_parse_date handles various date formats."""
    # yyyy/mm/dd
    result = TaiwanWRAConnector._parse_date("2024/06/01")
    assert result is not None
    assert result.year == 2024

    # yyyymmdd
    result = TaiwanWRAConnector._parse_date("20240601")
    assert result is not None
    assert result.year == 2024

    # None
    result = TaiwanWRAConnector._parse_date(None)
    assert result is None

    # Empty
    result = TaiwanWRAConnector._parse_date("")
    assert result is None

    # Unparseable
    result = TaiwanWRAConnector._parse_date("bad")
    assert result is None


def test_to_float_edge_cases():
    """_to_float handles various edge cases."""
    assert TaiwanWRAConnector._to_float(None) is None
    assert TaiwanWRAConnector._to_float("") is None
    assert TaiwanWRAConnector._to_float("abc") is None
    assert TaiwanWRAConnector._to_float(42) == pytest.approx(42.0)
    assert TaiwanWRAConnector._to_float("3.14") == pytest.approx(3.14)
