"""Tests for the China MWR connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.china_mwr import ChinaMWRConnector

MOCK_HTML_WITH_DATA = """<html><body>
<table>
2024-06-01 06:00 15200.5
2024-06-01 12:00 15800.0
</table>
</body></html>"""


@respx.mock
async def test_fetch_stations_returns_seed_list():
    """Always returns the curated seed station list."""
    respx.get("http://xxfb.mwr.cn/sq_djdh.html").mock(
        return_value=httpx.Response(500)
    )

    async with ChinaMWRConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 20
    s = next(s for s in stations if s.native_id == "MWR001")
    assert s.id == "china_mwr:MWR001"
    assert s.name == "Yichang"
    assert s.river == "Yangtze"
    assert s.country_code == "CN"


@respx.mock
async def test_fetch_observations_parses_html():
    """Observations are extracted from HTML response."""
    respx.get("http://xxfb.mwr.cn/sq_djdh.html").mock(
        return_value=httpx.Response(200, text=MOCK_HTML_WITH_DATA)
    )

    async with ChinaMWRConnector() as conn:
        chunk = await conn.fetch_observations(
            "china_mwr:MWR001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert chunk.provider == "china_mwr"
    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(15200.5)


@respx.mock
async def test_fetch_observations_failure_returns_empty():
    """Returns empty chunk on any failure."""
    respx.get("http://xxfb.mwr.cn/sq_djdh.html").mock(
        return_value=httpx.Response(500)
    )

    async with ChinaMWRConnector() as conn:
        chunk = await conn.fetch_observations(
            "china_mwr:MWR001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert chunk.observations == []


@respx.mock
async def test_fetch_observations_empty_response():
    """Empty text body returns zero observations."""
    respx.get("http://xxfb.mwr.cn/sq_djdh.html").mock(
        return_value=httpx.Response(200, text="")
    )

    async with ChinaMWRConnector() as conn:
        chunk = await conn.fetch_observations(
            "china_mwr:MWR001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@respx.mock
async def test_fetch_observations_no_matching_pattern():
    """Non-matching HTML returns zero observations."""
    respx.get("http://xxfb.mwr.cn/sq_djdh.html").mock(
        return_value=httpx.Response(
            200, text="<html><body>No data</body></html>"
        )
    )

    async with ChinaMWRConnector() as conn:
        chunk = await conn.fetch_observations(
            "china_mwr:MWR001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@respx.mock
async def test_connector_registration():
    """Connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("china_mwr")
    assert cls is ChinaMWRConnector


@respx.mock
async def test_fetch_stations_live_augmentation():
    """Live station discovery adds new stations to the seed list."""
    live_html = """<html>
    station_id = 'LIVE001'
    station_id = 'LIVE002'
    </html>"""
    respx.get("http://xxfb.mwr.cn/sq_djdh.html").mock(
        return_value=httpx.Response(200, text=live_html)
    )

    async with ChinaMWRConnector() as conn:
        stations = await conn.fetch_stations()

    # 20 seed + 2 live
    assert len(stations) == 22
    native_ids = {s.native_id for s in stations}
    assert "LIVE001" in native_ids
    assert "LIVE002" in native_ids


@respx.mock
async def test_fetch_stations_live_dedup():
    """Live stations that duplicate seed IDs are not added."""
    live_html = """<html>
    station_id = 'MWR001'
    station_id = 'LIVE001'
    </html>"""
    respx.get("http://xxfb.mwr.cn/sq_djdh.html").mock(
        return_value=httpx.Response(200, text=live_html)
    )

    async with ChinaMWRConnector() as conn:
        stations = await conn.fetch_stations()

    # 20 seed + 1 new live (MWR001 already in seed)
    assert len(stations) == 21


@respx.mock
async def test_fetch_latest_delegates_to_fetch_observations():
    """fetch_latest calls fetch_observations with a 24-hour window."""
    respx.get("http://xxfb.mwr.cn/sq_djdh.html").mock(
        return_value=httpx.Response(200, text=MOCK_HTML_WITH_DATA)
    )

    async with ChinaMWRConnector() as conn:
        chunk = await conn.fetch_latest("china_mwr:MWR001")

    assert chunk.provider == "china_mwr"
    assert len(chunk.observations) == 2


@respx.mock
async def test_discover_stations_live_non_html():
    """Non-HTML response returns empty list from live discovery."""
    respx.get("http://xxfb.mwr.cn/sq_djdh.html").mock(
        return_value=httpx.Response(200, text="plain text response no html")
    )

    async with ChinaMWRConnector() as conn:
        stations = await conn.fetch_stations()

    # Only seed stations since live returned empty
    assert len(stations) == 20


@respx.mock
async def test_parse_station_html_no_matches():
    """HTML with no station_id patterns returns empty from live discovery."""
    html = "<html><body>Nothing useful here</body></html>"
    respx.get("http://xxfb.mwr.cn/sq_djdh.html").mock(
        return_value=httpx.Response(200, text=html)
    )

    async with ChinaMWRConnector() as conn:
        stations = await conn.fetch_stations()

    # Only seed
    assert len(stations) == 20


@respx.mock
async def test_parse_observations_invalid_value():
    """Observation rows with unparseable values are skipped."""
    text_with_bad_value = """<html><body>
2024-06-01 06:00 15200.5
2024-06-01 12:00 BADVAL
2024-06-02 06:00 abc
</body></html>"""
    respx.get("http://xxfb.mwr.cn/sq_djdh.html").mock(
        return_value=httpx.Response(200, text=text_with_bad_value)
    )

    async with ChinaMWRConnector() as conn:
        chunk = await conn.fetch_observations(
            "china_mwr:MWR001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 3, tzinfo=UTC),
        )

    # Only the first valid row is parsed; the other values don't match the regex
    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(15200.5)


@respx.mock
async def test_parse_observations_slash_date_format():
    """Observations with slash date format are parsed correctly."""
    text_with_slashes = """<html><body>
2024/06/01 06:00 15200.5
2024/06/01 12:00 15800.0
</body></html>"""
    respx.get("http://xxfb.mwr.cn/sq_djdh.html").mock(
        return_value=httpx.Response(200, text=text_with_slashes)
    )

    async with ChinaMWRConnector() as conn:
        chunk = await conn.fetch_observations(
            "china_mwr:MWR001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
