"""Tests for the DSI (Turkey) connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.turkey_dsi import TurkeyDsiConnector

MOCK_STATIONS_RESPONSE = [
    {
        "istasyon_no": 501,
        "istasyon_adi": "Ankara / Sakarya",
        "nehir": "Sakarya",
        "havza_alani_km2": 3200.5,
        "enlem": 39.9208,
        "boylam": 32.8541,
        "havza": "Sakarya",
    },
    {
        "istasyon_no": 502,
        "istasyon_adi": "Istanbul / Riva",
        "nehir": "Riva",
        "havza_alani_km2": None,
        "enlem": 41.2055,
        "boylam": 29.2329,
        "havza": "Marmara",
    },
    {
        "istasyon_no": 503,
        "istasyon_adi": "Antalya / Manavgat",
        "nehir": "Manavgat",
        "havza_alani_km2": 1050.0,
        "enlem": 36.7833,
        "boylam": 31.45,
        "havza": "Antalya",
    },
]

MOCK_OBSERVATIONS_JSON = [
    {"tarih": "2010-06-01T00:00:00", "debi": 45.2},
    {"tarih": "2010-06-02T00:00:00", "debi": 42.8},
    {"tarih": "2010-06-03T00:00:00", "debi": None},
    {"tarih": "2010-06-04T00:00:00", "debi": 50.1},
    {"tarih": "2010-07-01T00:00:00", "debi": 38.0},
]

MOCK_OBSERVATIONS_CSV = """\
tarih;debi
2010-06-01;45,2
2010-06-02;42,8
2010-06-03;eksik
2010-06-04;50,1
2010-07-01;38,0
"""

BASE = "https://akim.faceteknoloji.com.tr"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_returns_all():
    """All stations from the JSON response are parsed."""
    respx.get(f"{BASE}/api/stations").mock(
        return_value=httpx.Response(
            200,
            json=MOCK_STATIONS_RESPONSE,
            headers={"content-type": "application/json"},
        ),
    )

    async with TurkeyDsiConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 3
    native_ids = {s.native_id for s in stations}
    assert native_ids == {"501", "502", "503"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fields():
    """Station fields are correctly mapped from DSI JSON."""
    respx.get(f"{BASE}/api/stations").mock(
        return_value=httpx.Response(
            200,
            json=MOCK_STATIONS_RESPONSE,
            headers={"content-type": "application/json"},
        ),
    )

    async with TurkeyDsiConnector() as conn:
        stations = await conn.fetch_stations()

    ankara = next(s for s in stations if s.native_id == "501")
    assert ankara.id == "turkey_dsi:501"
    assert ankara.provider == "turkey_dsi"
    assert ankara.name == "Ankara / Sakarya"
    assert ankara.country_code == "TR"
    assert ankara.river == "Sakarya"
    assert ankara.latitude == pytest.approx(39.9208)
    assert ankara.longitude == pytest.approx(32.8541)
    assert ankara.catchment_area_km2 == pytest.approx(3200.5)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_null_catchment():
    """Stations with null catchment area have catchment_area_km2 = None."""
    respx.get(f"{BASE}/api/stations").mock(
        return_value=httpx.Response(
            200,
            json=MOCK_STATIONS_RESPONSE,
            headers={"content-type": "application/json"},
        ),
    )

    async with TurkeyDsiConnector() as conn:
        stations = await conn.fetch_stations()

    istanbul = next(
        s for s in stations if s.native_id == "502"
    )
    assert istanbul.catchment_area_km2 is None


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fallback_endpoint():
    """Falls back to alternative endpoint when primary fails."""
    respx.get(f"{BASE}/api/stations").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE}/istasyonlar").mock(
        return_value=httpx.Response(
            200,
            json=MOCK_STATIONS_RESPONSE,
            headers={"content-type": "application/json"},
        ),
    )

    async with TurkeyDsiConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 3


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_csv():
    """Observations are parsed from a CSV response."""
    respx.get(f"{BASE}/api/data").mock(
        return_value=httpx.Response(
            200,
            text=MOCK_OBSERVATIONS_CSV,
            headers={"content-type": "text/csv"},
        ),
    )

    async with TurkeyDsiConnector() as conn:
        chunk = await conn.fetch_observations(
            "turkey_dsi:501",
            start=datetime(2010, 6, 1),
            end=datetime(2010, 6, 30),
        )

    assert chunk.provider == "turkey_dsi"
    assert chunk.station_id == "turkey_dsi:501"
    # Only June dates (4 of 5 rows)
    assert len(chunk.observations) == 4
    assert chunk.observations[0].discharge_m3s == pytest.approx(45.2)
    # "eksik" -> MISSING
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_json():
    """Observations are parsed from a JSON response."""
    respx.get(f"{BASE}/api/data").mock(
        return_value=httpx.Response(
            200,
            json=MOCK_OBSERVATIONS_JSON,
            headers={"content-type": "application/json"},
        ),
    )

    async with TurkeyDsiConnector() as conn:
        chunk = await conn.fetch_observations(
            "turkey_dsi:501",
            start=datetime(2010, 6, 1),
            end=datetime(2010, 6, 30),
        )

    assert len(chunk.observations) == 4
    assert chunk.observations[0].discharge_m3s == pytest.approx(45.2)
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_all_endpoints_fail():
    """When all observation endpoints fail, ConnectorError is raised."""
    from csfs.core.exceptions import ConnectorError

    respx.get(f"{BASE}/api/data").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE}/api/observations").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE}/api/akim").mock(
        return_value=httpx.Response(500),
    )

    async with TurkeyDsiConnector() as conn:
        with pytest.raises(ConnectorError):
            await conn.fetch_observations(
                "turkey_dsi:501",
                start=datetime(2010, 6, 1),
                end=datetime(2010, 6, 30),
            )


@pytest.mark.asyncio
@respx.mock
async def test_connector_registration():
    """The connector is registered with the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("turkey_dsi")
    assert cls is TurkeyDsiConnector


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_entries_without_id():
    """Entries missing istasyon_no are silently skipped."""
    data = [
        {"istasyon_adi": "No ID", "enlem": 40.0, "boylam": 30.0},
        {
            "istasyon_no": "",
            "istasyon_adi": "Empty",
            "enlem": 40.0,
            "boylam": 30.0,
        },
        MOCK_STATIONS_RESPONSE[0],
    ]
    respx.get(f"{BASE}/api/stations").mock(
        return_value=httpx.Response(
            200,
            json=data,
            headers={"content-type": "application/json"},
        ),
    )

    async with TurkeyDsiConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "501"
