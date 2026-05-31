"""Tests for the SEPA (Scotland) KiWIS connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.scotland_sepa import ScotlandSepaConnector, _map_quality
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import QualityFlag

KIWIS_URL = "https://timeseries.sepa.org.uk/KiWIS/KiWIS"

# getStationList: 3rd station has no Flow series and must be dropped.
MOCK_STATION_LIST = [
    ["station_no", "station_name", "station_latitude", "station_longitude", "river_name"],
    ["14969", "Aberuthven", 56.3197, -3.6587, "Ruthven Water"],
    ["322551", "Abington", 55.4869, -3.6906, "Clyde"],
    ["99999", "Rain Only", 55.0, -3.0, "Nowhere"],
]

# getTimeseriesList (Flow): 14969 has 15minute + Day.Mean; 322551 only Day.Mean.
MOCK_FLOW_SERIES = [
    ["station_no", "ts_id", "ts_name"],
    ["14969", "67554010", "15minute"],
    ["14969", "67555010", "Day.Mean"],
    ["322551", "67560010", "Day.Mean"],
]

MOCK_TS_VALUES = [
    {
        "ts_id": "67554010",
        "columns": "Timestamp,Value,Quality Code",
        "data": [
            ["2024-06-01T00:00:00.000Z", 1.032, 10],
            ["2024-06-01T00:15:00.000Z", None, 255],
            ["2024-06-01T00:30:00.000Z", 1.040, 254],
        ],
    },
]


def _route(station_list=MOCK_STATION_LIST, flow_series=MOCK_FLOW_SERIES,
           ts_values=MOCK_TS_VALUES):
    def _handler(request):
        req = request.url.params.get("request")
        if req == "getTimeseriesList":
            return httpx.Response(200, json=flow_series)
        if req == "getStationList":
            return httpx.Response(200, json=station_list)
        if req == "getTimeseriesValues":
            return httpx.Response(200, json=ts_values)
        return httpx.Response(404)
    return _handler


@respx.mock
@pytest.mark.asyncio
async def test_fetch_stations_returns_only_flow_stations():
    respx.get(KIWIS_URL).mock(side_effect=_route())

    async with ScotlandSepaConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"14969", "322551"}
    aber = next(s for s in stations if s.native_id == "14969")
    assert aber.id == "scotland_sepa:14969"
    assert aber.name == "Aberuthven"
    assert aber.country_code == "GB"
    assert aber.river == "Ruthven Water"
    assert aber.latitude == pytest.approx(56.3197)
    assert aber.longitude == pytest.approx(-3.6587)


@respx.mock
@pytest.mark.asyncio
async def test_fetch_observations_parses_values_and_quality():
    respx.get(KIWIS_URL).mock(side_effect=_route())

    async with ScotlandSepaConnector() as conn:
        chunk = await conn.fetch_observations(
            "scotland_sepa:14969",
            start=datetime(2024, 6, 1, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 1, 1, 0, tzinfo=UTC),
        )

    assert len(chunk.observations) == 3
    good, missing, raw = chunk.observations
    assert good.discharge_m3s == pytest.approx(1.032)
    assert good.quality == QualityFlag.GOOD          # code 10
    assert missing.discharge_m3s is None
    assert missing.quality == QualityFlag.MISSING    # code 255 / null value
    assert raw.discharge_m3s == pytest.approx(1.040)
    assert raw.quality == QualityFlag.RAW            # code 254 (provisional)


@respx.mock
@pytest.mark.asyncio
async def test_resolve_ts_id_prefers_15minute():
    # getTimeseriesValues must be requested with the 15minute ts_id (67554010).
    requested: list[str | None] = []

    def _handler(request):
        req = request.url.params.get("request")
        if req == "getTimeseriesList":
            return httpx.Response(200, json=MOCK_FLOW_SERIES)
        if req == "getStationList":
            return httpx.Response(200, json=MOCK_STATION_LIST)
        if req == "getTimeseriesValues":
            requested.append(request.url.params.get("ts_id"))
            return httpx.Response(200, json=MOCK_TS_VALUES)
        return httpx.Response(404)

    respx.get(KIWIS_URL).mock(side_effect=_handler)

    async with ScotlandSepaConnector() as conn:
        await conn.fetch_observations(
            "scotland_sepa:14969",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert requested == ["67554010"]  # the 15minute series, not Day.Mean


@respx.mock
@pytest.mark.asyncio
async def test_station_without_flow_series_raises():
    respx.get(KIWIS_URL).mock(side_effect=_route())

    async with ScotlandSepaConnector() as conn:
        with pytest.raises(ConnectorError):
            await conn.fetch_observations(
                "scotland_sepa:00000",
                start=datetime(2024, 6, 1, tzinfo=UTC),
                end=datetime(2024, 6, 2, tzinfo=UTC),
            )


@respx.mock
@pytest.mark.asyncio
async def test_bad_station_columns_raise():
    bad = [["wrong", "columns"], ["x", "y"]]
    respx.get(KIWIS_URL).mock(side_effect=_route(station_list=bad))

    async with ScotlandSepaConnector() as conn:
        with pytest.raises(DataFormatError):
            await conn.fetch_stations()


def test_quality_mapping():
    assert _map_quality(0) == QualityFlag.GOOD
    assert _map_quality(40) == QualityFlag.GOOD
    assert _map_quality(254) == QualityFlag.RAW
    assert _map_quality(255) == QualityFlag.MISSING
    assert _map_quality(130) == QualityFlag.MISSING
    assert _map_quality(None) == QualityFlag.MISSING
    assert _map_quality("nan") == QualityFlag.RAW


def test_connector_is_registered():
    from csfs.core.registry import get_connector

    assert get_connector("scotland_sepa") is ScotlandSepaConnector
