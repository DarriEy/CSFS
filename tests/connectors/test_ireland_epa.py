# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Tests for the Ireland EPA HydroNet connector with mocked responses."""

import zipfile
import io
from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx

from csfs.connectors.ireland_epa import IrelandEPAConnector
from csfs.core.models import QualityFlag

MOCK_INDEX_RESPONSE = [
    {
        "L1_RESPONSIBLE_BODY": "Environmental Protection Agency",
        "L1_DATA_AVAILABLE": "Water Level and Flow",
        "metadata_station_no": "10017",
        "metadata_station_name": "BALLYMAN",
        "metadata_station_latitude": "53.2045",
        "metadata_station_longitude": "-6.1650",
        "L1_river_name": "BALLYMAN STREAM",
        "metadata_CATCHMENT_SIZE": "3.80 km²",
        "L1_station_status": "Active"
    },
    {
        "L1_RESPONSIBLE_BODY": "Office of Public Works",
        "L1_DATA_AVAILABLE": "Water Level",
        "metadata_station_no": "01041",
        "metadata_station_name": "Sandy Mills"
    }
]

def create_mock_zip(csv_content: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("data.csv", csv_content)
    return buf.getvalue()

MOCK_CSV = """datetime,value,quality
2024-06-01T12:00:00,1.234,Good
2024-06-01T12:15:00,1.235,Valid
2024-06-01T12:30:00,,Missing
"""

@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations():
    respx.get("https://epawebapp.epa.ie/Hydronet/output/internet/layers/20/index.json").mock(
        return_value=httpx.Response(200, json=MOCK_INDEX_RESPONSE)
    )

    async with IrelandEPAConnector() as conn:
        stations = await conn.fetch_stations()

    # Only 10017 is EPA-responsible with Flow
    assert len(stations) == 1
    s = stations[0]
    assert s.native_id == "10017"
    assert s.name == "BALLYMAN"
    assert s.river == "BALLYMAN STREAM"
    assert s.catchment_area_km2 == 3.8
    assert s.is_active is True


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_with_region_discovery():
    # Mock index
    respx.get("https://epawebapp.epa.ie/Hydronet/output/internet/layers/20/index.json").mock(
        return_value=httpx.Response(200, json=MOCK_INDEX_RESPONSE)
    )
    
    # Mock region discovery (DUB works)
    respx.head("https://epawebapp.epa.ie/Hydronet/output/internet/stations/DUB/10017/Q/complete_daymean.zip").mock(
        return_value=httpx.Response(200)
    )
    # Others fail
    respx.head(url__regex=r"https://epawebapp.epa.ie/Hydronet/output/internet/stations/(?!DUB).*/10017/Q/complete_daymean.zip").mock(
        return_value=httpx.Response(404)
    )
    
    # Mock actual download
    respx.get("https://epawebapp.epa.ie/Hydronet/output/internet/stations/DUB/10017/Q/complete_15min.zip").mock(
        return_value=httpx.Response(200, content=create_mock_zip(MOCK_CSV))
    )

    async with IrelandEPAConnector() as conn:
        chunk = await conn.fetch_observations(
            "ireland_epa:10017",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 1, 23, 59, tzinfo=UTC),
        )

    assert chunk.station_id == "ireland_epa:10017"
    assert len(chunk.observations) == 3
    
    obs1 = chunk.observations[0]
    assert obs1.timestamp == datetime(2024, 6, 1, 12, 0, tzinfo=UTC)
    assert obs1.discharge_m3s == 1.234
    assert obs1.quality == QualityFlag.GOOD
    
    obs3 = chunk.observations[2]
    assert obs3.discharge_m3s is None
    assert obs3.quality == QualityFlag.MISSING


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_fallback_to_daymean():
    # Mock region discovery (DUB works)
    respx.head("https://epawebapp.epa.ie/Hydronet/output/internet/stations/DUB/10017/Q/complete_daymean.zip").mock(
        return_value=httpx.Response(200)
    )
    
    # 15min fails
    respx.get("https://epawebapp.epa.ie/Hydronet/output/internet/stations/DUB/10017/Q/complete_15min.zip").mock(
        return_value=httpx.Response(404)
    )
    # daymean works
    respx.get("https://epawebapp.epa.ie/Hydronet/output/internet/stations/DUB/10017/Q/complete_daymean.zip").mock(
        return_value=httpx.Response(200, content=create_mock_zip(MOCK_CSV))
    )

    async with IrelandEPAConnector() as conn:
        chunk = await conn.fetch_observations(
            "ireland_epa:10017",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 1, 23, 59, tzinfo=UTC),
        )

    assert len(chunk.observations) == 3
