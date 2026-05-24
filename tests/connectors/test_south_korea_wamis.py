"""Tests for the WAMIS (South Korea) connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.south_korea_wamis import SouthKoreaWamisConnector

MOCK_STATIONS_RESPONSE = {
    "header": {"result_code": "00", "result_msg": "OK"},
    "body": {
        "items": [
            {
                "stn_id": "1001650",
                "stn_nm": "Nakdong Bridge",
                "lat": 35.1264,
                "lon": 128.9947,
                "river_nm": "Nakdong",
                "bsn_nm": "Nakdong Basin",
            },
            {
                "stn_id": "2001600",
                "stn_nm": "Han River Main",
                "lat": 37.5172,
                "lon": 126.9784,
                "river_nm": "Han",
                "bsn_nm": "Han Basin",
            },
        ]
    },
}

MOCK_STATION_MISSING_COORDS = {
    "header": {"result_code": "00", "result_msg": "OK"},
    "body": {
        "items": [
            {
                "stn_id": "9999999",
                "stn_nm": "Bad Station",
            },
        ]
    },
}

MOCK_OBSERVATIONS_RESPONSE = {
    "header": {"result_code": "00", "result_msg": "OK"},
    "body": {
        "items": [
            {
                "obs_dt": "2024060112",
                "fw_flux": 123.4,
                "fw_lvl": 5.2,
            },
            {
                "obs_dt": "2024060113",
                "fw_flux": 130.0,
                "fw_lvl": 5.5,
            },
            {
                "obs_dt": "2024060114",
                "fw_flux": None,
                "fw_lvl": None,
            },
        ]
    },
}

MOCK_OBSERVATIONS_DAILY = {
    "header": {"result_code": "00", "result_msg": "OK"},
    "body": {
        "items": [
            {
                "obs_dt": "20240601",
                "fw_flux": 100.0,
                "fw_lvl": 4.0,
            },
        ]
    },
}

MOCK_EMPTY_RESPONSE = {
    "header": {"result_code": "00", "result_msg": "OK"},
    "body": {"items": []},
}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_items():
    """Stations are correctly parsed from the WAMIS envelope."""
    respx.get("http://www.wamis.go.kr/openapi/wkw/rf_dubrfobs").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with SouthKoreaWamisConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2

    stn_a = next(s for s in stations if s.native_id == "1001650")
    assert stn_a.id == "south_korea_wamis:1001650"
    assert stn_a.provider == "south_korea_wamis"
    assert stn_a.country_code == "KR"
    assert stn_a.name == "Nakdong Bridge"
    assert stn_a.river == "Nakdong"
    assert stn_a.latitude == pytest.approx(35.1264)
    assert stn_a.longitude == pytest.approx(128.9947)

    stn_b = next(s for s in stations if s.native_id == "2001600")
    assert stn_b.river == "Han"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_missing_coords():
    """Stations without lat/lon are skipped."""
    respx.get("http://www.wamis.go.kr/openapi/wkw/rf_dubrfobs").mock(
        return_value=httpx.Response(200, json=MOCK_STATION_MISSING_COORDS)
    )

    async with SouthKoreaWamisConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_empty():
    """An empty items list returns no stations."""
    respx.get("http://www.wamis.go.kr/openapi/wkw/rf_dubrfobs").mock(
        return_value=httpx.Response(200, json=MOCK_EMPTY_RESPONSE)
    )

    async with SouthKoreaWamisConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_hourly():
    """Hourly observations (YYYYMMDDHH) are parsed correctly."""
    respx.get("http://www.wamis.go.kr/openapi/wkw/rf_dubrfobs").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE)
    )

    async with SouthKoreaWamisConnector() as conn:
        chunk = await conn.fetch_observations(
            "south_korea_wamis:1001650",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "south_korea_wamis"
    assert chunk.station_id == "south_korea_wamis:1001650"
    assert len(chunk.observations) == 3

    # First observation — discharge available
    obs0 = chunk.observations[0]
    assert obs0.discharge_m3s == pytest.approx(123.4)
    assert obs0.quality.value == "raw"
    assert obs0.timestamp.hour == 12

    # Third observation — both None -> MISSING
    obs2 = chunk.observations[2]
    assert obs2.discharge_m3s is None
    assert obs2.quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_daily():
    """Daily observations (YYYYMMDD) are parsed correctly."""
    respx.get("http://www.wamis.go.kr/openapi/wkw/rf_dubrfobs").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_DAILY)
    )

    async with SouthKoreaWamisConnector() as conn:
        chunk = await conn.fetch_observations(
            "south_korea_wamis:1001650",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(100.0)
    assert chunk.observations[0].timestamp.day == 1


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty():
    """An empty items list returns zero observations."""
    respx.get("http://www.wamis.go.kr/openapi/wkw/rf_dubrfobs").mock(
        return_value=httpx.Response(200, json=MOCK_EMPTY_RESPONSE)
    )

    async with SouthKoreaWamisConnector() as conn:
        chunk = await conn.fetch_observations(
            "south_korea_wamis:1001650",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_with_api_key():
    """When api_key is configured, it is sent as a query parameter."""
    route = respx.get("http://www.wamis.go.kr/openapi/wkw/rf_dubrfobs").mock(
        return_value=httpx.Response(200, json=MOCK_EMPTY_RESPONSE)
    )

    async with SouthKoreaWamisConnector(config={"api_key": "test-key-123"}) as conn:
        await conn.fetch_stations()

    assert route.called
    request = route.calls[0].request
    assert "apikey=test-key-123" in str(request.url)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_falls_back_to_water_level():
    """When fw_flux is absent, fw_lvl is used as the discharge value."""
    payload = {
        "body": {
            "items": [
                {
                    "obs_dt": "2024060112",
                    "fw_flux": None,
                    "fw_lvl": 3.7,
                },
            ]
        }
    }
    respx.get("http://www.wamis.go.kr/openapi/wkw/rf_dubrfobs").mock(
        return_value=httpx.Response(200, json=payload)
    )

    async with SouthKoreaWamisConnector() as conn:
        chunk = await conn.fetch_observations(
            "south_korea_wamis:1001650",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(3.7)
    assert chunk.observations[0].quality.value == "raw"


@pytest.mark.asyncio
@respx.mock
async def test_extract_items_flat_list():
    """If the API returns a flat list instead of the envelope, items are still extracted."""
    flat_payload = [
        {"stn_id": "3001000", "stn_nm": "Flat Station", "lat": 36.0, "lon": 127.0, "river_nm": "Geum"},
    ]
    respx.get("http://www.wamis.go.kr/openapi/wkw/rf_dubrfobs").mock(
        return_value=httpx.Response(200, json=flat_payload)
    )

    async with SouthKoreaWamisConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "3001000"


@pytest.mark.asyncio
@respx.mock
async def test_extract_items_content_fallback():
    """The 'content' key is used as a fallback when 'body.items' is absent."""
    payload = {
        "content": [
            {"stn_id": "4001000", "stn_nm": "Content Station", "lat": 35.0, "lon": 129.0},
        ]
    }
    respx.get("http://www.wamis.go.kr/openapi/wkw/rf_dubrfobs").mock(
        return_value=httpx.Response(200, json=payload)
    )

    async with SouthKoreaWamisConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "4001000"


@pytest.mark.asyncio
@respx.mock
async def test_registry_slug():
    """The connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("south_korea_wamis")
    assert cls is SouthKoreaWamisConnector
