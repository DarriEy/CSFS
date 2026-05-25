"""Tests for USGS connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.usgs import USGSConnector

MOCK_DV_RESPONSE = {
    "value": {
        "timeSeries": [{
            "values": [{
                "value": [
                    {
                        "value": "5000",
                        "dateTime": "2024-06-01T00:00:00.000",
                        "qualifiers": ["A"],
                    },
                    {
                        "value": "4800",
                        "dateTime": "2024-06-02T00:00:00.000",
                        "qualifiers": ["P"],
                    },
                ]
            }]
        }]
    }
}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_json():
    respx.get("https://waterservices.usgs.gov/nwis/iv/").mock(
        return_value=httpx.Response(200, json=MOCK_DV_RESPONSE)
    )

    async with USGSConnector() as conn:
        chunk = await conn.fetch_observations(
            "usgs:01646500",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(5000 * 0.0283168, rel=1e-3)
    assert chunk.observations[0].quality.value == "good"
    assert chunk.observations[1].quality.value == "raw"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty():
    respx.get("https://waterservices.usgs.gov/nwis/iv/").mock(
        return_value=httpx.Response(200, json={"value": {"timeSeries": []}})
    )

    async with USGSConnector() as conn:
        chunk = await conn.fetch_observations(
            "usgs:01646500",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0
