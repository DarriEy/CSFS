"""Tests for the SPW Hydrométrie (Wallonia) KiWIS connector."""

from datetime import UTC, datetime
from unittest.mock import AsyncMock

import httpx
import pytest
import respx

from csfs.connectors.belgium_wallonia import (
    BelgiumWalloniaConnector,
    _map_quality,
)
from csfs.core.exceptions import ConnectorError
from csfs.core.models import QualityFlag

KIWIS_URL = "https://hydrometrie.wallonie.be/services/KiWIS/KiWIS"

# Filtered getTimeseriesList (hourly cadence): two real stations.
MOCK_TS_LIST = [
    ["station_no", "ts_id", "ts_name"],
    ["L5442", "100", "10-Debit.1h.Moyen"],
    ["L6290", "101", "10-Debit.1h.Moyen"],
]

# getStationList: a basin-grouping row (no coords) and a non-discharge station
# must both be dropped.
MOCK_STATION_LIST = [
    ["station_no", "station_name", "station_latitude", "station_longitude", "river_name"],
    ["L5442", "Aiseau", 50.4050, 4.5872, "Biesme"],
    ["L6290", "Amberloup", 50.0356, 5.5264, "Ourthe Occidentale"],
    ["2", "Affluents de l'Escaut", "", "", ""],
    ["L9999", "No Debit", 50.0, 5.0, "X"],
]

MOCK_TS_VALUES = [
    {
        "ts_id": "100",
        "columns": "Timestamp,Value,Quality Code",
        "data": [
            ["2024-06-01T00:00:00.000+02:00", 0.147, 40],
            ["2024-06-01T01:00:00.000+02:00", None, 255],
            ["2024-06-01T02:00:00.000+02:00", 7.719, 200],
        ],
    },
]


def _route(ts_list=MOCK_TS_LIST, station_list=MOCK_STATION_LIST,
           ts_values=MOCK_TS_VALUES):
    def _handler(request):
        req = request.url.params.get("request")
        if req == "getTimeseriesList":
            return httpx.Response(200, json=ts_list)
        if req == "getStationList":
            return httpx.Response(200, json=station_list)
        if req == "getTimeseriesValues":
            return httpx.Response(200, json=ts_values)
        return httpx.Response(404)
    return _handler


@pytest.fixture(autouse=True)
def _no_sleep(monkeypatch):
    """Skip the transient-retry backoff sleeps in tests."""
    monkeypatch.setattr(
        "csfs.connectors.belgium_wallonia.asyncio.sleep", AsyncMock(),
    )


@respx.mock
@pytest.mark.asyncio
async def test_fetch_stations_returns_discharge_stations():
    respx.get(KIWIS_URL).mock(side_effect=_route())

    async with BelgiumWalloniaConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"L5442", "L6290"}
    aiseau = next(s for s in stations if s.native_id == "L5442")
    assert aiseau.id == "belgium_wallonia:L5442"
    assert aiseau.name == "Aiseau"
    assert aiseau.country_code == "BE"
    assert aiseau.river == "Biesme"
    assert aiseau.latitude == pytest.approx(50.4050)


@respx.mock
@pytest.mark.asyncio
async def test_fetch_observations_parses_values_and_quality():
    respx.get(KIWIS_URL).mock(side_effect=_route())

    async with BelgiumWalloniaConnector() as conn:
        chunk = await conn.fetch_observations(
            "belgium_wallonia:L5442",
            start=datetime(2024, 6, 1, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 1, 3, 0, tzinfo=UTC),
        )

    assert len(chunk.observations) == 3
    good, missing, raw = chunk.observations
    assert good.discharge_m3s == pytest.approx(0.147)
    assert good.quality == QualityFlag.GOOD          # code 40
    assert missing.discharge_m3s is None
    assert missing.quality == QualityFlag.MISSING    # code 255 / null
    assert raw.discharge_m3s == pytest.approx(7.719)
    assert raw.quality == QualityFlag.RAW            # code 200


@respx.mock
@pytest.mark.asyncio
async def test_transient_503_is_retried():
    calls = {"n": 0}

    def _handler(request):
        if request.url.params.get("request") == "getTimeseriesList":
            calls["n"] += 1
            if calls["n"] < 3:
                return httpx.Response(503)
            return httpx.Response(200, json=MOCK_TS_LIST)
        if request.url.params.get("request") == "getStationList":
            return httpx.Response(200, json=MOCK_STATION_LIST)
        return httpx.Response(404)

    respx.get(KIWIS_URL).mock(side_effect=_handler)

    async with BelgiumWalloniaConnector() as conn:
        stations = await conn.fetch_stations()

    assert calls["n"] == 3          # two 503s then success
    assert len(stations) == 2


@respx.mock
@pytest.mark.asyncio
async def test_persistent_503_raises_connector_error():
    respx.get(KIWIS_URL).mock(return_value=httpx.Response(503))

    async with BelgiumWalloniaConnector() as conn:
        with pytest.raises(ConnectorError):
            await conn.fetch_stations()


@respx.mock
@pytest.mark.asyncio
async def test_non_transient_error_not_retried():
    # A 500 is not transient — it should surface immediately as HTTPStatusError.
    respx.get(KIWIS_URL).mock(return_value=httpx.Response(500))

    async with BelgiumWalloniaConnector() as conn:
        with pytest.raises(httpx.HTTPStatusError):
            await conn.fetch_stations()


@respx.mock
@pytest.mark.asyncio
async def test_station_without_series_raises():
    respx.get(KIWIS_URL).mock(side_effect=_route())

    async with BelgiumWalloniaConnector() as conn:
        with pytest.raises(ConnectorError):
            await conn.fetch_observations(
                "belgium_wallonia:UNKNOWN",
                start=datetime(2024, 6, 1, tzinfo=UTC),
                end=datetime(2024, 6, 2, tzinfo=UTC),
            )


def test_quality_mapping():
    assert _map_quality(0) == QualityFlag.GOOD
    assert _map_quality(200) == QualityFlag.RAW
    assert _map_quality(255) == QualityFlag.MISSING
    assert _map_quality(None) == QualityFlag.MISSING


def test_connector_is_registered():
    from csfs.core.registry import get_connector

    assert get_connector("belgium_wallonia") is BelgiumWalloniaConnector
