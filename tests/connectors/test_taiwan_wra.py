"""Tests for the Taiwan WRA (v2 keyless reservoir inflow) connector."""

import json
from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.taiwan_wra import _RESERVOIR_DATASET, TaiwanWRAConnector

DATASET_URL = f"https://opendata.wra.gov.tw/api/v2/{_RESERVOIR_DATASET}"

MOCK_DATA = json.dumps([
    # Shimen (seeded): two valid hourly inflow points + one empty.
    {"reservoiridentifier": "10201", "observationtime": "2026-06-01T06:00:00",
     "inflowdischarge": "20.9", "waterlevel": "245.0"},
    {"reservoiridentifier": "10201", "observationtime": "2026-06-01T07:00:00",
     "inflowdischarge": "18.3", "waterlevel": "245.1"},
    {"reservoiridentifier": "10201", "observationtime": "2026-06-01T08:00:00",
     "inflowdischarge": "", "waterlevel": "245.2"},
    # Jiaxian Weir (seeded).
    {"reservoiridentifier": "31002", "observationtime": "2026-06-01T07:00:00",
     "inflowdischarge": "3.16"},
    # A reservoir not in the seed (no coords) — must not become a station.
    {"reservoiridentifier": "99999", "observationtime": "2026-06-01T07:00:00",
     "inflowdischarge": "5.0"},
])


@respx.mock
@pytest.mark.asyncio
async def test_fetch_stations_returns_seeded_present():
    respx.get(DATASET_URL).mock(return_value=httpx.Response(200, text=MOCK_DATA))

    async with TaiwanWRAConnector() as conn:
        stations = await conn.fetch_stations()

    ids = {s.native_id for s in stations}
    # Only seeded reservoirs present in the feed; 99999 (unseeded) excluded.
    assert ids == {"10201", "31002"}
    shimen = next(s for s in stations if s.native_id == "10201")
    assert shimen.id == "taiwan_wra:10201"
    assert shimen.name == "Shimen Reservoir"
    assert shimen.country_code == "TW"
    assert shimen.river == "Dahan River"
    assert shimen.latitude == pytest.approx(24.812)


@respx.mock
@pytest.mark.asyncio
async def test_fetch_observations_parses_inflow_and_tz():
    respx.get(DATASET_URL).mock(return_value=httpx.Response(200, text=MOCK_DATA))

    async with TaiwanWRAConnector() as conn:
        chunk = await conn.fetch_observations(
            "taiwan_wra:10201",
            start=datetime(2026, 5, 31, tzinfo=UTC),
            end=datetime(2026, 6, 2, tzinfo=UTC),
        )

    # Two non-empty inflow points (the empty one is skipped), sorted by time.
    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(20.9)
    # 06:00 Taipei (UTC+8) -> 2026-05-31 22:00 UTC.
    assert chunk.observations[0].timestamp == datetime(2026, 5, 31, 22, 0, tzinfo=UTC)
    assert chunk.observations[0].quality.value == "raw"


@respx.mock
@pytest.mark.asyncio
async def test_fetch_observations_window_filter():
    respx.get(DATASET_URL).mock(return_value=httpx.Response(200, text=MOCK_DATA))

    async with TaiwanWRAConnector() as conn:
        # Window covering only the 07:00 Taipei point (= 2026-05-31 23:00 UTC).
        chunk = await conn.fetch_observations(
            "taiwan_wra:10201",
            start=datetime(2026, 5, 31, 22, 30, tzinfo=UTC),
            end=datetime(2026, 5, 31, 23, 30, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(18.3)


@respx.mock
@pytest.mark.asyncio
async def test_unknown_station_returns_empty():
    respx.get(DATASET_URL).mock(return_value=httpx.Response(200, text=MOCK_DATA))

    async with TaiwanWRAConnector() as conn:
        chunk = await conn.fetch_observations(
            "taiwan_wra:00000",
            start=datetime(2026, 5, 31, tzinfo=UTC),
            end=datetime(2026, 6, 2, tzinfo=UTC),
        )

    assert chunk.observations == []
    assert chunk.provider == "taiwan_wra"


@respx.mock
@pytest.mark.asyncio
async def test_dataset_fetched_once_and_cached():
    route = respx.get(DATASET_URL).mock(
        return_value=httpx.Response(200, text=MOCK_DATA),
    )

    async with TaiwanWRAConnector() as conn:
        await conn.fetch_stations()
        await conn.fetch_observations(
            "taiwan_wra:10201",
            start=datetime(2026, 5, 31, tzinfo=UTC),
            end=datetime(2026, 6, 2, tzinfo=UTC),
        )

    assert route.call_count == 1


def test_connector_is_registered():
    from csfs.core.registry import get_connector

    assert get_connector("taiwan_wra") is TaiwanWRAConnector
