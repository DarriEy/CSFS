# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Tests for the Denmark VanDa Hydro connector with mocked responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.denmark_dmihyd import DenmarkHydroConnector
from csfs.core.models import QualityFlag

MOCK_STATIONS_RESPONSE = [
    {
        "stationUid": "bf0a311a-7d17-4dd6-a008-c65866195529",
        "stationId": "70000590",
        "name": "Nykærvej",
        "measurementPoints": [
            {
                "examinations": [
                    {"parameter": "Vandstand"},
                    {"parameter": "Vandføring"}
                ]
            }
        ]
    },
    {
        "stationUid": "98ae8b11-1595-425a-b0fe-dc2098f34af5",
        "stationId": "31001397",
        "name": "Holme Å",
        "measurementPoints": [
            {
                "examinations": [
                    {"parameter": "Vandstand"}
                ]
            }
        ]
    }
]

MOCK_FLOWS_RESPONSE = [
    {
        "measurementDateTime": "2024-06-01T12:00:00.00Z",
        "result": 1500.0,
        "unit": "l/s"
    },
    {
        "measurementDateTime": "2024-06-01T13:00:00.00Z",
        "result": None,
        "unit": "l/s"
    }
]


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations():
    respx.get("https://vandah.miljoeportal.dk/api/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with DenmarkHydroConnector() as conn:
        stations = await conn.fetch_stations()

    # Only 70000590 has 'Vandføring'
    assert len(stations) == 1
    s = stations[0]
    assert s.native_id == "70000590"
    assert s.name == "Nykærvej"
    assert s.provider == "denmark_dmihyd"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations():
    respx.get("https://vandah.miljoeportal.dk/api/water-flows").mock(
        return_value=httpx.Response(200, json=MOCK_FLOWS_RESPONSE)
    )

    async with DenmarkHydroConnector() as conn:
        chunk = await conn.fetch_observations(
            "denmark_dmihyd:70000590",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 1, 23, 59, tzinfo=UTC),
        )

    assert chunk.station_id == "denmark_dmihyd:70000590"
    assert len(chunk.observations) == 2
    
    # 1500 l/s -> 1.5 m3/s
    obs1 = chunk.observations[0]
    assert obs1.discharge_m3s == 1.5
    assert obs1.quality == QualityFlag.GOOD
    
    obs2 = chunk.observations[1]
    assert obs2.discharge_m3s is None
    assert obs2.quality == QualityFlag.MISSING
