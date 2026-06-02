# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Tests for the Slovenia ARSO connector with mocked XML responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.slovenia_arso import SloveniaArsoConnector
from csfs.core.models import QualityFlag

MOCK_XML = """<?xml version="1.0" encoding="UTF-8"?>
<arsopodatki verzija="1.5">
<postaja sifra="1060" wgs84_dolzina="15.99" wgs84_sirina="46.68">
    <reka>Mura</reka>
    <merilno_mesto>Gornja Radgona</merilno_mesto>
    <datum>2026-06-01 06:30</datum>
    <pretok>123.4</pretok>
</postaja>
<postaja sifra="1140" wgs84_dolzina="16.23" wgs84_sirina="46.52">
    <reka>Ščavnica</reka>
    <merilno_mesto>Pristava</merilno_mesto>
    <datum>2026-06-01 06:30</datum>
    <pretok>2.99</pretok>
</postaja>
</arsopodatki>
"""

@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations():
    respx.get("https://www.arso.gov.si/xml/vode/hidro_podatki_zadnji.xml").mock(
        return_value=httpx.Response(200, text=MOCK_XML)
    )

    async with SloveniaArsoConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    s = stations[0]
    assert s.native_id == "1060"
    assert s.name == "Gornja Radgona"
    assert s.river == "Mura"
    assert s.latitude == 46.68
    assert s.longitude == 15.99
    assert s.provider == "slovenia_arso"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest():
    respx.get("https://www.arso.gov.si/xml/vode/hidro_podatki_zadnji.xml").mock(
        return_value=httpx.Response(200, text=MOCK_XML)
    )

    async with SloveniaArsoConnector() as conn:
        chunk = await conn.fetch_latest("slovenia_arso:1060")

    assert chunk.station_id == "slovenia_arso:1060"
    assert len(chunk.observations) == 1
    
    obs = chunk.observations[0]
    assert obs.discharge_m3s == 123.4
    assert obs.timestamp == datetime(2026, 6, 1, 6, 30, tzinfo=UTC)
    assert obs.quality == QualityFlag.RAW
