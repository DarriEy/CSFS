# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Tests for the Italy ISPRA HIS-Central WaterOneFlow (SOAP) connector.

The live broker is token-gated (HTTP 403 without a WMO WHOS authToken),
so the production default is graceful-empty. These tests cover:

* registration,
* graceful empty when no token is configured,
* graceful empty on a 403 from the broker (token rejected),
* real WaterML 1.1 parsing of stations and discharge (m3/s) when a token
  *is* supplied and the broker returns data (mocked SOAP responses).

All HTTP is mocked with respx; conftest blocks real network access.
"""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.italy_ispra_wof import _WOF_URL, ISPRAWOFConnector
from csfs.core.models import QualityFlag
from csfs.core.registry import discover, get_connector

# WaterML 1.1 GetSites response: the WaterML doc is XML-escaped inside the
# SOAP <GetSitesResult> wrapper, exactly as WaterOneFlow 1.1 returns it.
_SITES_WATERML = (
    '<sitesResponse xmlns="http://www.cuahsi.org/waterML/1.1/">'
    "<site><siteInfo>"
    "<siteName>Adige a Trento</siteName>"
    '<siteCode network="ISPRA">ADIGE_TN</siteCode>'
    "<geoLocation><geogLocation>"
    "<latitude>46.07</latitude><longitude>11.12</longitude>"
    "</geogLocation></geoLocation>"
    "</siteInfo></site>"
    "</sitesResponse>"
)

GETSITES_SOAP = (
    '<?xml version="1.0" encoding="utf-8"?>'
    '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">'
    "<soap:Body>"
    '<GetSitesResponse xmlns="http://www.cuahsi.org/his/1.1/ws/">'
    "<GetSitesResult>"
    + _SITES_WATERML.replace("<", "&lt;").replace(">", "&gt;")
    + "</GetSitesResult>"
    "</GetSitesResponse>"
    "</soap:Body></soap:Envelope>"
)

# WaterML 1.1 GetValues response with a discharge series in m3/s.
_VALUES_WATERML = (
    '<timeSeriesResponse xmlns="http://www.cuahsi.org/waterML/1.1/">'
    "<timeSeries>"
    "<variable>"
    "<variableCode>Portata</variableCode>"
    "<variableName>Discharge</variableName>"
    "<unit><unitName>m3/s</unitName></unit>"
    "</variable>"
    '<values>'
    '<value dateTime="2026-05-15T00:00:00">123.4</value>'
    '<value dateTime="2026-05-16T00:00:00">130.5</value>'
    '<value dateTime="2026-05-17T00:00:00">-9999</value>'
    "</values>"
    "</timeSeries>"
    "</timeSeriesResponse>"
)

GETVALUES_SOAP = (
    '<?xml version="1.0" encoding="utf-8"?>'
    '<soap:Envelope xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">'
    "<soap:Body>"
    '<GetValuesResponse xmlns="http://www.cuahsi.org/his/1.1/ws/">'
    "<GetValuesResult>"
    + _VALUES_WATERML.replace("<", "&lt;").replace(">", "&gt;")
    + "</GetValuesResult>"
    "</GetValuesResponse>"
    "</soap:Body></soap:Envelope>"
)


def test_registration():
    discover()
    assert get_connector("italy_ispra_wof") is ISPRAWOFConnector


@pytest.mark.asyncio
async def test_fetch_stations_no_token_is_empty():
    """Without a WHOS token the connector returns empty (no fake seed)."""
    async with ISPRAWOFConnector() as conn:
        stations = await conn.fetch_stations()
    assert stations == []


@pytest.mark.asyncio
async def test_fetch_observations_no_token_is_empty():
    async with ISPRAWOFConnector() as conn:
        chunk = await conn.fetch_observations(
            "italy_ispra_wof:ADIGE_TN",
            datetime(2026, 5, 1, tzinfo=UTC),
            datetime(2026, 5, 31, tzinfo=UTC),
        )
    assert chunk.observations == []
    assert chunk.station_id == "italy_ispra_wof:ADIGE_TN"
    assert chunk.provider == "italy_ispra_wof"


@pytest.mark.asyncio
@respx.mock
async def test_broker_403_degrades_to_empty():
    """A 403 from the token-gated broker must not raise — empty instead."""
    respx.post(_WOF_URL).mock(return_value=httpx.Response(403))

    async with ISPRAWOFConnector({"whos_token": "rejected"}) as conn:
        stations = await conn.fetch_stations()
        chunk = await conn.fetch_observations(
            "italy_ispra_wof:ADIGE_TN",
            datetime(2026, 5, 1, tzinfo=UTC),
            datetime(2026, 5, 31, tzinfo=UTC),
        )
    assert stations == []
    assert chunk.observations == []


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_waterml():
    """With a token + a real WaterML GetSites response, parse the site."""
    respx.post(_WOF_URL).mock(
        return_value=httpx.Response(200, text=GETSITES_SOAP)
    )

    async with ISPRAWOFConnector({"whos_token": "good"}) as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    s = stations[0]
    assert s.native_id == "ADIGE_TN"
    assert s.id == "italy_ispra_wof:ADIGE_TN"
    assert s.name == "Adige a Trento"
    assert s.country_code == "IT"
    assert s.latitude == 46.07
    assert s.longitude == 11.12
    assert s.provider == "italy_ispra_wof"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_discharge_m3s():
    """GetValues WaterML parsed into discharge_m3s observations."""
    respx.post(_WOF_URL).mock(
        return_value=httpx.Response(200, text=GETVALUES_SOAP)
    )

    async with ISPRAWOFConnector({"whos_token": "good"}) as conn:
        chunk = await conn.fetch_observations(
            "italy_ispra_wof:ADIGE_TN",
            datetime(2026, 5, 1, tzinfo=UTC),
            datetime(2026, 5, 31, tzinfo=UTC),
        )

    assert chunk.station_id == "italy_ispra_wof:ADIGE_TN"
    assert len(chunk.observations) == 3

    o0 = chunk.observations[0]
    assert o0.discharge_m3s == 123.4
    assert o0.timestamp == datetime(2026, 5, 15, tzinfo=UTC)
    assert o0.quality == QualityFlag.RAW

    o1 = chunk.observations[1]
    assert o1.discharge_m3s == 130.5

    # -9999 no-data sentinel -> None / MISSING
    o2 = chunk.observations[2]
    assert o2.discharge_m3s is None
    assert o2.quality == QualityFlag.MISSING
