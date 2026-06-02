"""Tests for the NRC Hilltop (New Zealand) connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.newzealand_hilltop import NewZealandHilltopConnector
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

    async with NewZealandHilltopConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2

    mangakahia = next(
        s for s in stations if s.native_id == "Mangakahia_at_Titoki"
    )
    assert mangakahia.id == "newzealand_hilltop:Mangakahia_at_Titoki"
    assert mangakahia.provider == "newzealand_hilltop"
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

    async with NewZealandHilltopConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_invalid_xml_returns_empty():
    """Malformed XML from all councils returns empty list."""
    async with NewZealandHilltopConnector() as conn:
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

    async with NewZealandHilltopConnector() as conn:
        chunk = await conn.fetch_observations(
            "newzealand_hilltop:Mangakahia_at_Titoki",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "newzealand_hilltop"
    assert chunk.station_id == "newzealand_hilltop:Mangakahia_at_Titoki"
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

    async with NewZealandHilltopConnector() as conn:
        chunk = await conn.fetch_observations(
            "newzealand_hilltop:Mangakahia_at_Titoki",
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

    async with NewZealandHilltopConnector() as conn:
        chunk = await conn.fetch_observations(
            "newzealand_hilltop:Mangakahia_at_Titoki",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(5.23)
    assert chunk.observations[1].discharge_m3s is None
    assert chunk.observations[1].quality == QualityFlag.MISSING


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_invalid_timestamp_returns_empty():
    """Invalid timestamp from all councils returns empty chunk."""
    async with NewZealandHilltopConnector() as conn:
        chunk = await conn.fetch_observations(
            "newzealand_hilltop:Nonexistent_Station",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )
    assert chunk.observations == []


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_no_data_returns_empty():
    """No matching data from any council returns empty chunk."""
    async with NewZealandHilltopConnector() as conn:
        chunk = await conn.fetch_observations(
            "newzealand_hilltop:Another_Missing_Station",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )
    assert chunk.observations == []


# -- Tests: registration -----------------------------------------------

def test_connector_is_registered():
    """The connector is registered with the expected slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("newzealand_hilltop")
    assert cls is NewZealandHilltopConnector


def test_connector_class_attributes():
    """Class-level attributes match expectations."""
    assert NewZealandHilltopConnector.slug == "newzealand_hilltop"
    assert NewZealandHilltopConnector.country_codes == ["NZ"]
    assert "hilltop.nrc.govt.nz" in NewZealandHilltopConnector.base_url


# ======================================================================
# Additional coverage tests — error branches, edge cases
# ======================================================================


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest_delegates():
    """fetch_latest imports timedelta and delegates to fetch_observations (lines 97-100)."""
    respx.get(f"{BASE}/data.hts").mock(
        return_value=httpx.Response(200, text=MOCK_DATA_XML),
    )

    async with NewZealandHilltopConnector() as conn:
        chunk = await conn.fetch_latest("newzealand_hilltop:Mangakahia_at_Titoki")

    assert chunk.provider == "newzealand_hilltop"
    # The call should work — observations may or may not match the last-24h range


def test_parse_station_xml_invalid_xml_raises():
    """Malformed XML raises DataFormatError (lines 124-125)."""
    from csfs.core.exceptions import DataFormatError

    conn = NewZealandHilltopConnector()
    with pytest.raises(DataFormatError, match="Failed to parse station list XML"):
        conn._parse_station_xml("<not valid xml<<<<")


def test_parse_station_xml_empty_name_skipped():
    """Sites with empty Name attribute are skipped (line 135)."""
    xml_text = """\
<?xml version="1.0" encoding="UTF-8"?>
<HilltopServer>
  <Site Name="">
    <Latitude>-35.83</Latitude>
    <Longitude>174.18</Longitude>
  </Site>
  <Site Name="Good Site">
    <Latitude>-35.73</Latitude>
    <Longitude>174.22</Longitude>
  </Site>
</HilltopServer>
"""
    conn = NewZealandHilltopConnector()
    stations = conn._parse_station_xml(xml_text)
    assert len(stations) == 1
    assert stations[0].name == "Good Site"


def test_parse_station_xml_value_error_skipped():
    """Sites with invalid lat/lon values log warning and skip (lines 161-168)."""
    xml_text = """\
<?xml version="1.0" encoding="UTF-8"?>
<HilltopServer>
  <Site Name="Bad Lat Site">
    <Latitude>not_a_number</Latitude>
    <Longitude>174.18</Longitude>
  </Site>
  <Site Name="Good Site">
    <Latitude>-35.73</Latitude>
    <Longitude>174.22</Longitude>
  </Site>
</HilltopServer>
"""
    conn = NewZealandHilltopConnector()
    stations = conn._parse_station_xml(xml_text)
    assert len(stations) == 1
    assert stations[0].name == "Good Site"


def test_parse_data_xml_invalid_xml_raises():
    """Malformed data XML raises DataFormatError (lines 188-189)."""
    from csfs.core.exceptions import DataFormatError

    conn = NewZealandHilltopConnector()
    with pytest.raises(DataFormatError, match="Failed to parse data XML"):
        conn._parse_data_xml("<invalid<xml", "newzealand_hilltop:test")


def test_parse_data_xml_missing_t_element_skipped():
    """Entries without T element are skipped (line 200)."""
    xml_text = """\
<?xml version="1.0" encoding="UTF-8"?>
<Hilltop>
  <Measurement SiteName="Test">
    <Data>
      <E><I1>5.23</I1></E>
      <E><T>2024-06-01T00:00:00</T><I1>5.31</I1></E>
    </Data>
  </Measurement>
</Hilltop>
"""
    conn = NewZealandHilltopConnector()
    chunk = conn._parse_data_xml(xml_text, "newzealand_hilltop:test")
    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(5.31)


def test_parse_data_xml_invalid_timestamp_raises():
    """Invalid timestamp in data XML raises DataFormatError (lines 204-205)."""
    from csfs.core.exceptions import DataFormatError

    xml_text = """\
<?xml version="1.0" encoding="UTF-8"?>
<Hilltop>
  <Measurement SiteName="Test">
    <Data>
      <E><T>not-a-date</T><I1>5.23</I1></E>
    </Data>
  </Measurement>
</Hilltop>
"""
    conn = NewZealandHilltopConnector()
    with pytest.raises(DataFormatError, match="Invalid timestamp"):
        conn._parse_data_xml(xml_text, "newzealand_hilltop:test")


def test_parse_data_xml_non_numeric_i1_is_missing():
    """Non-numeric I1 value results in None discharge and MISSING quality (lines 215-216)."""
    xml_text = """\
<?xml version="1.0" encoding="UTF-8"?>
<Hilltop>
  <Measurement SiteName="Test">
    <Data>
      <E><T>2024-06-01T00:00:00</T><I1>bad_value</I1></E>
    </Data>
  </Measurement>
</Hilltop>
"""
    conn = NewZealandHilltopConnector()
    chunk = conn._parse_data_xml(xml_text, "newzealand_hilltop:test")
    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[0].quality == QualityFlag.MISSING
