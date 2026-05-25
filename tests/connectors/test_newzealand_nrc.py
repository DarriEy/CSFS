"""Tests for the NRC Hilltop (New Zealand) connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.newzealand_nrc import NewZealandNrcConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import QualityFlag

# -- Mock response data ------------------------------------------------

MOCK_SITE_LIST_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<HilltopServer>
  <Site Name="Mangakahia at Titoki">
    <Latitude>-35.83</Latitude>
    <Longitude>174.18</Longitude>
  </Site>
  <Site Name="Wairua at Purua">
    <Latitude>-35.73</Latitude>
    <Longitude>174.22</Longitude>
  </Site>
</HilltopServer>
"""

MOCK_SITE_LIST_EMPTY_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<HilltopServer>
</HilltopServer>
"""

MOCK_DATA_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<Hilltop>
  <Measurement SiteName="Mangakahia at Titoki">
    <Data DateFormat="Calendar" NumItems="1">
      <E><T>2024-06-01T00:00:00</T><I1>5.23</I1></E>
      <E><T>2024-06-01T00:05:00</T><I1>5.31</I1></E>
      <E><T>2024-06-01T00:10:00</T><I1>5.18</I1></E>
    </Data>
  </Measurement>
</Hilltop>
"""

MOCK_DATA_EMPTY_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<Hilltop>
  <Measurement SiteName="Mangakahia at Titoki">
    <Data DateFormat="Calendar" NumItems="1">
    </Data>
  </Measurement>
</Hilltop>
"""

MOCK_DATA_MISSING_VALUE_XML = """\
<?xml version="1.0" encoding="UTF-8"?>
<Hilltop>
  <Measurement SiteName="Mangakahia at Titoki">
    <Data DateFormat="Calendar" NumItems="1">
      <E><T>2024-06-01T00:00:00</T><I1>5.23</I1></E>
      <E><T>2024-06-01T00:05:00</T></E>
    </Data>
  </Measurement>
</Hilltop>
"""

BASE = "https://hilltop.nrc.govt.nz"


# -- Tests: fetch_stations ---------------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_xml():
    """Station list is correctly parsed from the Hilltop SiteList XML."""
    respx.get(f"{BASE}/data.hts").mock(
        return_value=httpx.Response(200, text=MOCK_SITE_LIST_XML),
    )

    async with NewZealandNrcConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2

    mangakahia = next(
        s for s in stations if s.native_id == "Mangakahia_at_Titoki"
    )
    assert mangakahia.id == "newzealand_nrc:Mangakahia_at_Titoki"
    assert mangakahia.provider == "newzealand_nrc"
    assert mangakahia.name == "Mangakahia at Titoki"
    assert mangakahia.latitude == pytest.approx(-35.83)
    assert mangakahia.longitude == pytest.approx(174.18)
    assert mangakahia.country_code == "NZ"

    wairua = next(s for s in stations if s.native_id == "Wairua_at_Purua")
    assert wairua.name == "Wairua at Purua"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty site list returns no stations."""
    respx.get(f"{BASE}/data.hts").mock(
        return_value=httpx.Response(200, text=MOCK_SITE_LIST_EMPTY_XML),
    )

    async with NewZealandNrcConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_invalid_xml_returns_empty():
    """Malformed XML from all councils returns empty list."""
    async with NewZealandNrcConnector() as conn:
        stations = await conn.fetch_stations()

    # All councils unreachable in mock = empty
    assert stations == []


# -- Tests: fetch_observations ----------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_values():
    """Flow data XML is correctly parsed into observations."""
    respx.get(f"{BASE}/data.hts").mock(
        return_value=httpx.Response(200, text=MOCK_DATA_XML),
    )

    async with NewZealandNrcConnector() as conn:
        chunk = await conn.fetch_observations(
            "newzealand_nrc:Mangakahia_at_Titoki",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "newzealand_nrc"
    assert chunk.station_id == "newzealand_nrc:Mangakahia_at_Titoki"
    assert len(chunk.observations) == 3

    assert chunk.observations[0].discharge_m3s == pytest.approx(5.23)
    assert chunk.observations[0].quality == QualityFlag.RAW
    assert chunk.observations[1].discharge_m3s == pytest.approx(5.31)
    assert chunk.observations[2].discharge_m3s == pytest.approx(5.18)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty():
    """An empty data section returns zero observations."""
    respx.get(f"{BASE}/data.hts").mock(
        return_value=httpx.Response(200, text=MOCK_DATA_EMPTY_XML),
    )

    async with NewZealandNrcConnector() as conn:
        chunk = await conn.fetch_observations(
            "newzealand_nrc:Mangakahia_at_Titoki",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_missing_i1_is_missing_quality():
    """When I1 element is absent, discharge is None and quality is MISSING."""
    respx.get(f"{BASE}/data.hts").mock(
        return_value=httpx.Response(200, text=MOCK_DATA_MISSING_VALUE_XML),
    )

    async with NewZealandNrcConnector() as conn:
        chunk = await conn.fetch_observations(
            "newzealand_nrc:Mangakahia_at_Titoki",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(5.23)
    assert chunk.observations[1].discharge_m3s is None
    assert chunk.observations[1].quality == QualityFlag.MISSING


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_invalid_timestamp_raises():
    """Invalid timestamp in data raises DataFormatError."""
    bad_xml = """\
<?xml version="1.0" encoding="UTF-8"?>
<Hilltop>
  <Measurement SiteName="Test">
    <Data><E><T>NOT-A-TIMESTAMP</T><I1>1.0</I1></E></Data>
  </Measurement>
</Hilltop>
"""
    respx.get(f"{BASE}/data.hts").mock(
        return_value=httpx.Response(200, text=bad_xml),
    )

    async with NewZealandNrcConnector() as conn:
        with pytest.raises(DataFormatError, match="Invalid timestamp"):
            await conn.fetch_observations(
                "newzealand_nrc:Test",
                start=datetime(2024, 6, 1),
                end=datetime(2024, 6, 2),
            )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_invalid_data_xml_raises():
    """Malformed data XML raises DataFormatError."""
    respx.get(f"{BASE}/data.hts").mock(
        return_value=httpx.Response(200, text="<broken xml"),
    )

    async with NewZealandNrcConnector() as conn:
        with pytest.raises(DataFormatError, match="Failed to parse data XML"):
            await conn.fetch_observations(
                "newzealand_nrc:Test",
                start=datetime(2024, 6, 1),
                end=datetime(2024, 6, 2),
            )


# -- Tests: registration -----------------------------------------------

def test_connector_is_registered():
    """The connector is registered with the expected slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("newzealand_nrc")
    assert cls is NewZealandNrcConnector


def test_connector_class_attributes():
    """Class-level attributes match expectations."""
    assert NewZealandNrcConnector.slug == "newzealand_nrc"
    assert NewZealandNrcConnector.country_codes == ["NZ"]
    assert "hilltop.nrc.govt.nz" in NewZealandNrcConnector.base_url
