"""Tests for the WRA (Taiwan) connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.taiwan_wra import TaiwanWRAConnector

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
