"""Tests for the Czech ČHMÚ connector with mocked HTTP responses."""

import json
from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx

from csfs.connectors.czechia_chmi import _META_PATH, CzechiaChmiConnector
from csfs.core.exceptions import DataFormatError

BASE_URL = "https://opendata.chmi.cz"
META_URL = f"{BASE_URL}{_META_PATH}"

MOCK_META = json.dumps({
    "data": {"data": {
        "header": "objID,DBC,STATION_NAME,STREAM_NAME,GEOGR1,GEOGR2,SPAQ_UNIT",
        "values": [
            ["0-203-1-001000", "001000", "Špindlerův Mlýn", "Labe",
             50.7232, 15.5980, "M3_S"],
            ["0-203-1-009000", "009000", "No Coords", "Vltava", None, None, "M3_S"],
        ],
    }},
})


def _station_file(day: str) -> str:
    """A station data file with H (ignored) and Q series for one day."""
    return json.dumps({
        "objList": [{
            "objID": "0-203-1-001000",
            "tsList": [
                {"tsConID": "H", "unit": "CM", "tsData": [
                    {"dt": f"{day}T00:00:00Z", "value": 99},
                ]},
                {"tsConID": "Q", "unit": "M3_S", "tsData": [
                    {"dt": f"{day}T00:00:00Z", "value": 1.5},
                    {"dt": f"{day}T00:10:00Z", "value": None},
                    {"dt": f"{day}T00:20:00Z", "value": 1.7},
                ]},
            ],
        }],
    })


def _recent_url(day: str) -> str:
    return f"{BASE_URL}/hydrology/recent/data/{day}_0-203-1-001000.json"


@respx.mock
@pytest.mark.asyncio
async def test_fetch_stations_parses_metadata():
    respx.get(META_URL).mock(return_value=httpx.Response(200, text=MOCK_META))

    async with CzechiaChmiConnector() as conn:
        stations = await conn.fetch_stations()

    # The no-coords row is dropped.
    assert len(stations) == 1
    st = stations[0]
    assert st.id == "czechia_chmi:0-203-1-001000"
    assert st.native_id == "0-203-1-001000"
    assert st.name == "Špindlerův Mlýn"
    assert st.country_code == "CZ"
    assert st.river == "Labe"
    assert st.latitude == pytest.approx(50.7232)
    assert st.longitude == pytest.approx(15.5980)


@respx.mock
@pytest.mark.asyncio
async def test_invalid_metadata_raises():
    respx.get(META_URL).mock(return_value=httpx.Response(200, text="{not json"))

    async with CzechiaChmiConnector() as conn:
        with pytest.raises(DataFormatError):
            await conn.fetch_stations()


@respx.mock
@pytest.mark.asyncio
async def test_fetch_observations_recent_day_filters_q_and_window():
    respx.get(_recent_url("20240601")).mock(
        return_value=httpx.Response(200, text=_station_file("2024-06-01")),
    )

    async with CzechiaChmiConnector() as conn:
        chunk = await conn.fetch_observations(
            "czechia_chmi:0-203-1-001000",
            start=datetime(2024, 6, 1, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 1, 0, 30, tzinfo=UTC),
        )

    # H series ignored, null Q point skipped → two discharge points.
    assert len(chunk.observations) == 2
    assert [o.discharge_m3s for o in chunk.observations] == [
        pytest.approx(1.5), pytest.approx(1.7),
    ]
    assert chunk.observations[0].timestamp == datetime(2024, 6, 1, 0, 0, tzinfo=UTC)
    assert all(o.quality.value == "raw" for o in chunk.observations)


@respx.mock
@pytest.mark.asyncio
async def test_window_excludes_out_of_range_points():
    respx.get(_recent_url("20240601")).mock(
        return_value=httpx.Response(200, text=_station_file("2024-06-01")),
    )

    async with CzechiaChmiConnector() as conn:
        chunk = await conn.fetch_observations(
            "czechia_chmi:0-203-1-001000",
            start=datetime(2024, 6, 1, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 1, 0, 5, tzinfo=UTC),
        )

    # Only the 00:00 point falls inside the 5-minute window.
    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(1.5)


@respx.mock
@pytest.mark.asyncio
async def test_fetch_observations_spans_multiple_days():
    respx.get(_recent_url("20240601")).mock(
        return_value=httpx.Response(200, text=_station_file("2024-06-01")),
    )
    respx.get(_recent_url("20240602")).mock(
        return_value=httpx.Response(200, text=_station_file("2024-06-02")),
    )

    async with CzechiaChmiConnector() as conn:
        chunk = await conn.fetch_observations(
            "czechia_chmi:0-203-1-001000",
            start=datetime(2024, 6, 1, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 2, 23, 59, tzinfo=UTC),
        )

    # Two non-null Q points per day across two days.
    assert len(chunk.observations) == 4


@respx.mock
@pytest.mark.asyncio
async def test_missing_day_404_is_skipped():
    respx.get(_recent_url("20240601")).mock(return_value=httpx.Response(404))
    respx.get(_recent_url("20240602")).mock(
        return_value=httpx.Response(200, text=_station_file("2024-06-02")),
    )

    async with CzechiaChmiConnector() as conn:
        chunk = await conn.fetch_observations(
            "czechia_chmi:0-203-1-001000",
            start=datetime(2024, 6, 1, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 2, 23, 59, tzinfo=UTC),
        )

    # The 404 day is skipped; only day two's points are returned.
    assert len(chunk.observations) == 2


@respx.mock
@pytest.mark.asyncio
async def test_today_uses_now_endpoint():
    today = datetime.now(UTC).date()
    now_url = f"{BASE_URL}/hydrology/now/data/0-203-1-001000.json"
    respx.get(now_url).mock(
        return_value=httpx.Response(200, text=_station_file(f"{today:%Y-%m-%d}")),
    )

    async with CzechiaChmiConnector() as conn:
        chunk = await conn.fetch_observations(
            "czechia_chmi:0-203-1-001000",
            start=datetime(today.year, today.month, today.day, tzinfo=UTC),
            end=datetime.now(UTC) + timedelta(minutes=1),
        )

    # The current-day path is served by now/, not recent/.
    assert len(chunk.observations) >= 1


def test_connector_is_registered():
    from csfs.core.registry import get_connector

    assert get_connector("czechia_chmi") is CzechiaChmiConnector
