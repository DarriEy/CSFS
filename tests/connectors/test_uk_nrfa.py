# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Tests for the UK NRFA connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.uk_nrfa import UKNRFAConnector
from csfs.core.models import QualityFlag

MOCK_STATIONS_RESPONSE = {
    "data": [
        {
            "id": 10001,
            "name": "Ythan at Ardlethen",
            "latitude": 57.36861,
            "longitude": -2.1274,
            "river": "Ythan",
            "catchment-area": 448.1
        },
        {
            "id": 39001,
            "name": "Thames at Kingston",
            "latitude": 51.4154,
            "longitude": -0.3077,
            "river": "Thames",
            "catchment-area": 9948.0
        }
    ]
}

MOCK_TS_RESPONSE = {
    "data-stream": [
        "2010-01-01", 218.0,
        "2010-01-02", 194.0,
        "2010-01-03", None
    ]
}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations():
    respx.get("https://nrfaapps.ceh.ac.uk/nrfa/ws/station-info").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with UKNRFAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    s = stations[0]
    assert s.native_id == "10001"
    assert s.name == "Ythan at Ardlethen"
    assert s.river == "Ythan"
    assert s.latitude == 57.36861
    assert s.catchment_area_km2 == 448.1


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations():
    respx.get("https://nrfaapps.ceh.ac.uk/nrfa/ws/time-series").mock(
        return_value=httpx.Response(200, json=MOCK_TS_RESPONSE)
    )

    async with UKNRFAConnector() as conn:
        chunk = await conn.fetch_observations(
            "uk_nrfa:39001",
            start=datetime(2010, 1, 1, tzinfo=UTC),
            end=datetime(2010, 1, 3, tzinfo=UTC),
        )

    assert chunk.station_id == "uk_nrfa:39001"
    assert len(chunk.observations) == 3
    
    obs1 = chunk.observations[0]
    assert obs1.timestamp == datetime(2010, 1, 1, tzinfo=UTC)
    assert obs1.discharge_m3s == 218.0
    assert obs1.quality == QualityFlag.GOOD
    
    obs3 = chunk.observations[2]
    assert obs3.discharge_m3s is None
    assert obs3.quality == QualityFlag.MISSING


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest_calls_observations():
    # fetch_latest should just call fetch_observations with a 60-day window
    respx.get("https://nrfaapps.ceh.ac.uk/nrfa/ws/time-series").mock(
        return_value=httpx.Response(200, json={"data-stream": []})
    )

    async with UKNRFAConnector() as conn:
        chunk = await conn.fetch_latest("uk_nrfa:39001")

    assert chunk.station_id == "uk_nrfa:39001"
    assert len(chunk.observations) == 0
