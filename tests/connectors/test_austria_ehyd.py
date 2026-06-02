# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Tests for the Austria eHYD connector with mocked WFS responses."""

from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx

from csfs.connectors.austria_ehyd import AustriaEhydConnector
from csfs.core.models import QualityFlag

MOCK_STATIONS_RESPONSE = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "messstellen_owf.200014",
            "geometry": {
                "type": "Point",
                "coordinates": [9.5348, 47.2737]
            },
            "properties": {
                "hzbnr": "200014",
                "name": "Bangs",
                "gewaesser": "Rhein",
                "messstellenart": "Oberflächengewässer-Messstelle Durchfluss",
                "errichtet": 1856,
                "aufgelassen": None
            }
        },
        {
            "type": "Feature",
            "id": "messstellen_owf.200022",
            "geometry": {
                "type": "Point",
                "coordinates": [10.0224, 46.9907]
            },
            "properties": {
                "hzbnr": "200022",
                "name": "Gaschurn",
                "gewaesser": "Ill",
                "messstellenart": "Oberflächengewässer-Messstelle Wasserstand",
                "errichtet": 1907,
                "aufgelassen": None
            }
        }
    ]
}

MOCK_LATEST_RESPONSE = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": "pegel_aktuell.200014",
            "properties": {
                "hzbnr": 200014,
                "messstelle": "Bangs",
                "wert": 200.5,
                "einheit": "m³/s",
                "zeitpunkt": "2026-06-01T02:20:00+02:00",
                "parameter": "Q"
            }
        }
    ]
}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations():
    respx.get("https://gis.lfrz.gv.at/api/geodata/i000501/wfs").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with AustriaEhydConnector() as conn:
        stations = await conn.fetch_stations()

    # Only 200014 has 'Durchfluss' in messstellenart
    assert len(stations) == 1
    s = stations[0]
    assert s.native_id == "200014"
    assert s.name == "Bangs"
    assert s.river == "Rhein"
    assert s.latitude == 47.2737
    assert s.longitude == 9.5348
    assert s.provider == "austria_ehyd"
    assert s.id == "austria_ehyd:200014"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest():
    respx.get("https://gis.lfrz.gv.at/api/geodata/i000501/wfs").mock(
        return_value=httpx.Response(200, json=MOCK_LATEST_RESPONSE)
    )

    async with AustriaEhydConnector() as conn:
        chunk = await conn.fetch_latest("austria_ehyd:200014")

    assert chunk.station_id == "austria_ehyd:200014"
    assert chunk.provider == "austria_ehyd"
    assert len(chunk.observations) == 1
    
    obs = chunk.observations[0]
    assert obs.discharge_m3s == 200.5
    # 2026-06-01T02:20:00+02:00 -> 2026-06-01T00:20:00+00:00
    assert obs.timestamp == datetime(2026, 6, 1, 0, 20, tzinfo=UTC)
    assert obs.quality == QualityFlag.GOOD


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest_no_data():
    respx.get("https://gis.lfrz.gv.at/api/geodata/i000501/wfs").mock(
        return_value=httpx.Response(200, json={"type": "FeatureCollection", "features": []})
    )

    async with AustriaEhydConnector() as conn:
        chunk = await conn.fetch_latest("austria_ehyd:999999")

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_redirects_to_latest_for_now():
    respx.get("https://gis.lfrz.gv.at/api/geodata/i000501/wfs").mock(
        return_value=httpx.Response(200, json=MOCK_LATEST_RESPONSE)
    )

    async with AustriaEhydConnector() as conn:
        # Requesting a range that includes 'now'
        now = datetime.now(UTC)
        chunk = await conn.fetch_observations(
            "austria_ehyd:200014",
            start=now - timedelta(hours=1),
            end=now + timedelta(hours=1),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == 200.5


@pytest.mark.asyncio
async def test_fetch_observations_unsupported_historical():
    async with AustriaEhydConnector() as conn:
        # Requesting purely historical range
        chunk = await conn.fetch_observations(
            "austria_ehyd:200014",
            start=datetime(2000, 1, 1, tzinfo=UTC),
            end=datetime(2000, 1, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0
