# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Tests for the Croatia DHMZ connector with mocked API responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.croatia_dhz import CroatiaDhzConnector
from csfs.core.models import QualityFlag

MOCK_STATIONS_TEXT = """
{'success': 'true', 'postaje': [
    {'kod': '941', 'sifra': '5165', 'gsirina': 46.5, 'gduzina': 16.4, 'ttip': 'Postaja: <b>MURSKO SREDIŠĆE</b><br>Vodotok: <b>GORNJI POTOK</b>'},
    {'kod': '384', 'sifra': '5001', 'gsirina': 45.5, 'gduzina': 18.9, 'ttip': 'Postaja: <b>ALJMAŠ</b><br>Vodotok: <b>DUNAV</b>'}
]}
"""

MOCK_LATEST_TEXT = """
{'success': 'true', 'postaje': [
    {'sifra': '5165', 'zterm': '01. 06. 2026. 05:00', 'zpod': '12.3&nbsp;m3/s'},
    {'sifra': '5001', 'zterm': '01. 06. 2026. 04:00', 'zpod': '50&nbsp;cm'}
]}
"""

@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations():
    respx.get("https://hidro.dhz.hr/hidroweb/skripte/hisbaza.py").mock(
        return_value=httpx.Response(200, text=MOCK_STATIONS_TEXT)
    )

    async with CroatiaDhzConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    s = stations[0]
    assert s.native_id == "5165"
    assert s.name == "MURSKO SREDIŠĆE"
    assert s.river == "GORNJI POTOK"
    assert s.latitude == 46.5
    assert s.longitude == 16.4
    assert s.provider == "croatia_dhz"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest():
    respx.get("https://hidro.dhz.hr/hidroweb/skripte/hisbaza.py").mock(
        return_value=httpx.Response(200, text=MOCK_LATEST_TEXT)
    )

    async with CroatiaDhzConnector() as conn:
        chunk = await conn.fetch_latest("croatia_dhz:5165")

    assert chunk.station_id == "croatia_dhz:5165"
    assert len(chunk.observations) == 1
    
    obs = chunk.observations[0]
    assert obs.discharge_m3s == 12.3
    assert obs.timestamp == datetime(2026, 6, 1, 5, 0, tzinfo=UTC)
    assert obs.quality == QualityFlag.RAW


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest_level():
    respx.get("https://hidro.dhz.hr/hidroweb/skripte/hisbaza.py").mock(
        return_value=httpx.Response(200, text=MOCK_LATEST_TEXT)
    )

    async with CroatiaDhzConnector() as conn:
        # Station 5001 has 'cm' in mock
        chunk = await conn.fetch_latest("croatia_dhz:5001")

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == 50.0
