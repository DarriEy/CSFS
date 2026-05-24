"""Tests for the New Zealand Hilltop connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.newzealand_hilltop import NewZealandHilltopConnector

MOCK_SITE_LIST_XML = """\
<?xml version="1.0" encoding="utf-8"?>
<HilltopServer>
  <Agency>Environment Canterbury</Agency>
  <Site Name="Ashley River at Gorge">
    <Latitude>-43.2015</Latitude>
    <Longitude>172.3011</Longitude>
  </Site>
  <Site Name="Waimakariri River at Old Highway Bridge">
    <Latitude>-43.3890</Latitude>
    <Longitude>172.5432</Longitude>
  </Site>
  <Site Name="Incomplete Site">
    <Latitude></Latitude>
    <Longitude></Longitude>
  </Site>
  <Site Name="">
    <Latitude>-44.0</Latitude>
    <Longitude>171.0</Longitude>
  </Site>
</HilltopServer>
"""

MOCK_MEASUREMENT_LIST_XML = """\
<?xml version="1.0" encoding="utf-8"?>
<HilltopServer>
  <DataSource>
    <MeasurementName>Water Level</MeasurementName>
  </DataSource>
  <DataSource>
    <MeasurementName>Flow</MeasurementName>
  </DataSource>
  <DataSource>
    <MeasurementName>Rainfall</MeasurementName>
  </DataSource>
</HilltopServer>
"""

MOCK_MEASUREMENT_LIST_NO_FLOW_XML = """\
<?xml version="1.0" encoding="utf-8"?>
<HilltopServer>
  <DataSource>
    <MeasurementName>Water Level</MeasurementName>
  </DataSource>
  <DataSource>
    <MeasurementName>Rainfall</MeasurementName>
  </DataSource>
</HilltopServer>
"""

MOCK_GET_DATA_XML = """\
<?xml version="1.0" encoding="utf-8"?>
<Hilltop>
  <Measurement SiteName="Ashley River at Gorge" DataSourceName="Flow">
    <Data DateFormat="Calendar" NumItems="1">
      <E>
        <T>2024-06-01T12:00:00</T>
        <I1>5.23</I1>
      </E>
      <E>
        <T>2024-06-01T12:15:00</T>
        <I1>5.45</I1>
      </E>
      <E>
        <T>2024-06-01T12:30:00</T>
        <I1></I1>
      </E>
    </Data>
  </Measurement>
</Hilltop>
"""

MOCK_GET_DATA_EMPTY_XML = """\
<?xml version="1.0" encoding="utf-8"?>
<Hilltop>
  <Measurement SiteName="Ashley River at Gorge" DataSourceName="Flow">
    <Data DateFormat="Calendar" NumItems="1">
    </Data>
  </Measurement>
</Hilltop>
"""

BASE_URL = "https://data.ecan.govt.nz/data/hilltop.hts"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_xml():
    """Stations are parsed from SiteList XML; incomplete entries are skipped."""
    respx.get(BASE_URL).mock(
        return_value=httpx.Response(200, text=MOCK_SITE_LIST_XML),
    )

    async with NewZealandHilltopConnector() as conn:
        stations = await conn.fetch_stations()

    # Two valid stations; one has empty lat/lon, one has empty name
    assert len(stations) == 2

    ashley = stations[0]
    assert ashley.id == "newzealand_hilltop:Ashley River at Gorge"
    assert ashley.provider == "newzealand_hilltop"
    assert ashley.native_id == "Ashley River at Gorge"
    assert ashley.name == "Ashley River at Gorge"
    assert ashley.latitude == pytest.approx(-43.2015)
    assert ashley.longitude == pytest.approx(172.3011)
    assert ashley.country_code == "NZ"

    waimak = stations[1]
    assert waimak.native_id == "Waimakariri River at Old Highway Bridge"
    assert waimak.latitude == pytest.approx(-43.3890)
    assert waimak.longitude == pytest.approx(172.5432)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty site list returns no stations."""
    empty_xml = (
        '<?xml version="1.0" encoding="utf-8"?>'
        "<HilltopServer></HilltopServer>"
    )
    respx.get(BASE_URL).mock(
        return_value=httpx.Response(200, text=empty_xml),
    )

    async with NewZealandHilltopConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_malformed_xml():
    """Malformed XML raises DataFormatError."""
    respx.get(BASE_URL).mock(
        return_value=httpx.Response(200, text="<broken><xml"),
    )

    async with NewZealandHilltopConnector() as conn:
        with pytest.raises(Exception, match="Invalid XML"):
            await conn.fetch_stations()


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_xml():
    """Observations are parsed from GetData XML."""
    # Mock MeasurementList for _resolve_measurement
    respx.get(BASE_URL, params__contains={"Request": "MeasurementList"}).mock(
        return_value=httpx.Response(200, text=MOCK_MEASUREMENT_LIST_XML),
    )
    # Mock GetData
    respx.get(BASE_URL, params__contains={"Request": "GetData"}).mock(
        return_value=httpx.Response(200, text=MOCK_GET_DATA_XML),
    )

    async with NewZealandHilltopConnector() as conn:
        chunk = await conn.fetch_observations(
            "newzealand_hilltop:Ashley River at Gorge",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.station_id == "newzealand_hilltop:Ashley River at Gorge"
    assert chunk.provider == "newzealand_hilltop"
    assert len(chunk.observations) == 3

    # First observation
    assert chunk.observations[0].discharge_m3s == pytest.approx(5.23)
    assert chunk.observations[0].quality.value == "raw"

    # Second observation
    assert chunk.observations[1].discharge_m3s == pytest.approx(5.45)

    # Third observation — empty value yields MISSING
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_with_cached_measurement():
    """When measurement is cached, MeasurementList is not called."""
    conn = NewZealandHilltopConnector()
    conn._site_measurement["Ashley River at Gorge"] = "Flow"

    # Only mock GetData — MeasurementList should NOT be called
    respx.get(BASE_URL, params__contains={"Request": "GetData"}).mock(
        return_value=httpx.Response(200, text=MOCK_GET_DATA_XML),
    )

    async with conn:
        chunk = await conn.fetch_observations(
            "newzealand_hilltop:Ashley River at Gorge",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 3


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty():
    """An empty GetData response returns zero observations."""
    conn = NewZealandHilltopConnector()
    conn._site_measurement["Ashley River at Gorge"] = "Flow"

    respx.get(BASE_URL, params__contains={"Request": "GetData"}).mock(
        return_value=httpx.Response(200, text=MOCK_GET_DATA_EMPTY_XML),
    )

    async with conn:
        chunk = await conn.fetch_observations(
            "newzealand_hilltop:Ashley River at Gorge",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_blank_body():
    """A blank response body returns zero observations."""
    conn = NewZealandHilltopConnector()
    conn._site_measurement["Ashley River at Gorge"] = "Flow"

    respx.get(BASE_URL, params__contains={"Request": "GetData"}).mock(
        return_value=httpx.Response(200, text=""),
    )

    async with conn:
        chunk = await conn.fetch_observations(
            "newzealand_hilltop:Ashley River at Gorge",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0
    assert chunk.station_id == "newzealand_hilltop:Ashley River at Gorge"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_malformed_xml():
    """Malformed XML in GetData response raises DataFormatError."""
    conn = NewZealandHilltopConnector()
    conn._site_measurement["Ashley River at Gorge"] = "Flow"

    respx.get(BASE_URL, params__contains={"Request": "GetData"}).mock(
        return_value=httpx.Response(200, text="<broken><xml"),
    )

    async with conn:
        with pytest.raises(Exception, match="Invalid XML"):
            await conn.fetch_observations(
                "newzealand_hilltop:Ashley River at Gorge",
                start=datetime(2024, 6, 1),
                end=datetime(2024, 6, 2),
            )


@pytest.mark.asyncio
@respx.mock
async def test_resolve_measurement_falls_back_to_flow():
    """When no discharge measurement is found, falls back to 'Flow'."""
    respx.get(BASE_URL, params__contains={"Request": "MeasurementList"}).mock(
        return_value=httpx.Response(200, text=MOCK_MEASUREMENT_LIST_NO_FLOW_XML),
    )
    respx.get(BASE_URL, params__contains={"Request": "GetData"}).mock(
        return_value=httpx.Response(200, text=MOCK_GET_DATA_EMPTY_XML),
    )

    async with NewZealandHilltopConnector() as conn:
        chunk = await conn.fetch_observations(
            "newzealand_hilltop:Ashley River at Gorge",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0
    # Should have cached the fallback
    assert conn._site_measurement["Ashley River at Gorge"] == "Flow"


@pytest.mark.asyncio
@respx.mock
async def test_time_interval_format():
    """Verify the TimeInterval parameter uses ISO format."""
    conn = NewZealandHilltopConnector()
    conn._site_measurement["Ashley River at Gorge"] = "Flow"

    route = respx.get(BASE_URL, params__contains={"Request": "GetData"}).mock(
        return_value=httpx.Response(200, text=MOCK_GET_DATA_EMPTY_XML),
    )

    async with conn:
        await conn.fetch_observations(
            "newzealand_hilltop:Ashley River at Gorge",
            start=datetime(2024, 1, 15, 8, 30, 0),
            end=datetime(2024, 12, 25, 16, 45, 0),
        )

    assert route.called
    request = route.calls[0].request
    url_str = str(request.url)
    # TimeInterval should contain the formatted start/end
    assert "2024-01-15T08%3A30%3A00" in url_str or "2024-01-15T08:30:00" in url_str
    assert "2024-12-25T16%3A45%3A00" in url_str or "2024-12-25T16:45:00" in url_str


@pytest.mark.asyncio
@respx.mock
async def test_custom_base_url_via_config():
    """The base_url can be overridden via config for other regional councils."""
    custom_url = "https://hilltopserver.horizons.govt.nz/boo.hts"
    conn = NewZealandHilltopConnector(config={"base_url": custom_url})
    assert conn.base_url == custom_url


@pytest.mark.asyncio
@respx.mock
async def test_connector_sets_xml_accept_header():
    """Verify that the connector sets Accept: text/xml header."""
    route = respx.get(BASE_URL).mock(
        return_value=httpx.Response(200, text=MOCK_SITE_LIST_XML),
    )

    async with NewZealandHilltopConnector() as conn:
        await conn.fetch_stations()

    request = route.calls[0].request
    assert request.headers["accept"] == "text/xml"
