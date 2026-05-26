"""Tests for USGS connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.usgs import USGSConnector
from csfs.core.exceptions import DataFormatError

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


MOCK_RDB_RESPONSE = """\
# USGS test data
agency_cd\tsite_no\tstation_nm\tdec_lat_va\tdec_long_va\tdrain_area_va
5s\t15s\t50s\t16s\t16s\t16s
USGS\t01646500\tPotomac River near Washington DC\t38.9497\t-77.1278\t11560
USGS\t01638500\tPotomac River at Point of Rocks\t39.2736\t-77.5425\t9651
"""

MOCK_RDB_NO_HEADER = """\
# This file has no header line
# Just comments
"""


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_rdb():
    respx.get("https://waterservices.usgs.gov/nwis/site/").mock(
        return_value=httpx.Response(200, text=MOCK_RDB_RESPONSE)
    )

    async with USGSConnector(config={"states": ["MD"]}) as conn:
        stations = await conn.fetch_stations(state_codes=["MD"])

    assert len(stations) == 2
    potomac = next(s for s in stations if s.native_id == "01646500")
    assert potomac.id == "usgs:01646500"
    assert potomac.provider == "usgs"
    assert potomac.name == "Potomac River near Washington DC"
    assert potomac.latitude == pytest.approx(38.9497)
    assert potomac.longitude == pytest.approx(-77.1278)
    assert potomac.country_code == "US"
    assert potomac.catchment_area_km2 == pytest.approx(11560 * 2.58999, rel=1e-3)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_failed_states():
    """A failing state doesn't crash the whole station fetch."""
    respx.get("https://waterservices.usgs.gov/nwis/site/").mock(
        side_effect=[
            httpx.Response(500),
            httpx.Response(200, text=MOCK_RDB_RESPONSE),
        ]
    )

    async with USGSConnector() as conn:
        stations = await conn.fetch_stations(state_codes=["XX", "MD"])

    assert len(stations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_rdb_no_header_raises():
    """RDB without header raises DataFormatError but is caught per-state."""
    respx.get("https://waterservices.usgs.gov/nwis/site/").mock(
        return_value=httpx.Response(200, text=MOCK_RDB_NO_HEADER)
    )

    async with USGSConnector() as conn:
        stations = await conn.fetch_stations(state_codes=["XX"])

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_missing_value():
    """The sentinel -999999 is treated as None/MISSING."""
    response = {
        "value": {
            "timeSeries": [{
                "values": [{
                    "value": [
                        {
                            "value": "-999999",
                            "dateTime": "2024-06-01T00:00:00.000",
                            "qualifiers": ["P"],
                        },
                    ]
                }]
            }]
        }
    }
    respx.get("https://waterservices.usgs.gov/nwis/iv/").mock(
        return_value=httpx.Response(200, json=response)
    )

    async with USGSConnector() as conn:
        chunk = await conn.fetch_observations(
            "usgs:01646500",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[0].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_bad_structure_raises():
    """Unexpected JSON structure raises DataFormatError."""
    respx.get("https://waterservices.usgs.gov/nwis/iv/").mock(
        return_value=httpx.Response(200, json={"value": {}})
    )

    async with USGSConnector() as conn:
        with pytest.raises(DataFormatError):
            await conn.fetch_observations(
                "usgs:01646500",
                start=datetime(2024, 6, 1),
                end=datetime(2024, 6, 2),
            )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest():
    respx.get("https://waterservices.usgs.gov/nwis/iv/").mock(
        return_value=httpx.Response(200, json=MOCK_DV_RESPONSE)
    )

    async with USGSConnector() as conn:
        chunk = await conn.fetch_latest("usgs:01646500")

    assert len(chunk.observations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_estimated_quality():
    response = {
        "value": {
            "timeSeries": [{
                "values": [{
                    "value": [
                        {
                            "value": "3000",
                            "dateTime": "2024-06-01T00:00:00.000",
                            "qualifiers": ["e"],
                        },
                    ]
                }]
            }]
        }
    }
    respx.get("https://waterservices.usgs.gov/nwis/iv/").mock(
        return_value=httpx.Response(200, json=response)
    )

    async with USGSConnector() as conn:
        chunk = await conn.fetch_observations(
            "usgs:01646500",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.observations[0].quality.value == "estimated"


def test_parse_drainage_area():
    assert USGSConnector._parse_drainage_area("100") == pytest.approx(258.999)
    assert USGSConnector._parse_drainage_area("") is None
    assert USGSConnector._parse_drainage_area("N/A") is None
