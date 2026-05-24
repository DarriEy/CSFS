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
