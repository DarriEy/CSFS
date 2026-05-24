"""Tests for Brazil ANA connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.brazil_ana import BrazilANAConnector

MOCK_STATIONS_XML = """\
<?xml version="1.0" encoding="utf-8"?>
<DataSet>
  <diffgr:diffgram xmlns:diffgr="urn:schemas-microsoft-com:xml-diffgram-v1">
    <NewDataSet>
      <Table>
        <Codigo>60435000</Codigo>
        <Nome>PORTO NACIONAL</Nome>
        <Latitude>-10.7</Latitude>
        <Longitude>-48.4</Longitude>
        <RioNome>TOCANTINS</RioNome>
        <AreaDrenagem>175360.0</AreaDrenagem>
        <Operando>1</Operando>
      </Table>
      <Table>
        <Codigo>60436000</Codigo>
        <Nome>MIRACEMA DO TOCANTINS</Nome>
        <Latitude>-9.56</Latitude>
        <Longitude>-48.39</Longitude>
        <RioNome>TOCANTINS</RioNome>
        <AreaDrenagem>192000.0</AreaDrenagem>
        <Operando>0</Operando>
      </Table>
      <Table>
        <Codigo>60437000</Codigo>
        <Nome>INCOMPLETE STATION</Nome>
        <Latitude></Latitude>
        <Longitude></Longitude>
        <RioNome>RIO X</RioNome>
        <AreaDrenagem></AreaDrenagem>
        <Operando>1</Operando>
      </Table>
    </NewDataSet>
  </diffgr:diffgram>
</DataSet>
"""

MOCK_OBSERVATIONS_XML = """\
<?xml version="1.0" encoding="utf-8"?>
<DataSet>
  <diffgr:diffgram xmlns:diffgr="urn:schemas-microsoft-com:xml-diffgram-v1">
    <NewDataSet>
      <DadosHidrometworolgicos>
        <DataHora>2024-06-01 12:00:00</DataHora>
        <Media>1250.5</Media>
        <Maxima>1400.0</Maxima>
        <Minima>1100.0</Minima>
      </DadosHidrometworolgicos>
      <DadosHidrometworolgicos>
        <DataHora>2024-06-02 12:00:00</DataHora>
        <Media>1300.0</Media>
        <Maxima>1450.0</Maxima>
        <Minima>1150.0</Minima>
      </DadosHidrometworolgicos>
      <DadosHidrometworolgicos>
        <DataHora>2024-06-03 12:00:00</DataHora>
        <Media></Media>
        <Maxima>1500.0</Maxima>
        <Minima></Minima>
      </DadosHidrometworolgicos>
    </NewDataSet>
  </diffgr:diffgram>
</DataSet>
"""

MOCK_EMPTY_OBSERVATIONS_XML = """\
<?xml version="1.0" encoding="utf-8"?>
<DataSet>
  <diffgr:diffgram xmlns:diffgr="urn:schemas-microsoft-com:xml-diffgram-v1">
    <NewDataSet />
  </diffgr:diffgram>
</DataSet>
"""


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_xml():
    respx.get(
        "https://telemetriaws1.ana.gov.br/ServiceANA.asmx/HidroInventario"
    ).mock(return_value=httpx.Response(200, text=MOCK_STATIONS_XML))

    async with BrazilANAConnector() as conn:
        stations = await conn.fetch_stations()

    # Third station has empty lat/lon and should be skipped
    assert len(stations) == 2

    assert stations[0].native_id == "60435000"
    assert stations[0].id == "brazil_ana:60435000"
    assert stations[0].name == "PORTO NACIONAL"
    assert stations[0].latitude == -10.7
    assert stations[0].longitude == -48.4
    assert stations[0].river == "TOCANTINS"
    assert stations[0].catchment_area_km2 == 175360.0
    assert stations[0].is_active is True
    assert stations[0].country_code == "BR"

    assert stations[1].native_id == "60436000"
    assert stations[1].is_active is False


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_xml():
    respx.get(
        "https://telemetriaws1.ana.gov.br/ServiceANA.asmx/DadosHidrometeorologicos"
    ).mock(return_value=httpx.Response(200, text=MOCK_OBSERVATIONS_XML))

    async with BrazilANAConnector() as conn:
        chunk = await conn.fetch_observations(
            "brazil_ana:60435000",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 3),
        )

    assert chunk.station_id == "brazil_ana:60435000"
    assert chunk.provider == "brazil_ana"
    assert len(chunk.observations) == 3

    # First obs uses Media
    assert chunk.observations[0].discharge_m3s == pytest.approx(1250.5)
    assert chunk.observations[0].quality.value == "raw"

    # Second obs uses Media
    assert chunk.observations[1].discharge_m3s == pytest.approx(1300.0)

    # Third obs has empty Media, falls back to Maxima
    assert chunk.observations[2].discharge_m3s == pytest.approx(1500.0)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty_response():
    respx.get(
        "https://telemetriaws1.ana.gov.br/ServiceANA.asmx/DadosHidrometeorologicos"
    ).mock(return_value=httpx.Response(200, text=MOCK_EMPTY_OBSERVATIONS_XML))

    async with BrazilANAConnector() as conn:
        chunk = await conn.fetch_observations(
            "brazil_ana:60435000",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 3),
        )

    assert len(chunk.observations) == 0
    assert chunk.station_id == "brazil_ana:60435000"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_blank_body():
    respx.get(
        "https://telemetriaws1.ana.gov.br/ServiceANA.asmx/DadosHidrometeorologicos"
    ).mock(return_value=httpx.Response(200, text=""))

    async with BrazilANAConnector() as conn:
        chunk = await conn.fetch_observations(
            "brazil_ana:60435000",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 3),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_malformed_xml():
    respx.get(
        "https://telemetriaws1.ana.gov.br/ServiceANA.asmx/DadosHidrometeorologicos"
    ).mock(return_value=httpx.Response(200, text="<broken><xml"))

    async with BrazilANAConnector() as conn:
        with pytest.raises(Exception, match="Invalid XML"):
            await conn.fetch_observations(
                "brazil_ana:60435000",
                start=datetime(2024, 6, 1),
                end=datetime(2024, 6, 3),
            )


@pytest.mark.asyncio
@respx.mock
async def test_date_format_in_request():
    """Verify that the date parameters use dd/MM/yyyy format."""
    route = respx.get(
        "https://telemetriaws1.ana.gov.br/ServiceANA.asmx/DadosHidrometeorologicos"
    ).mock(return_value=httpx.Response(200, text=MOCK_EMPTY_OBSERVATIONS_XML))

    async with BrazilANAConnector() as conn:
        await conn.fetch_observations(
            "brazil_ana:60435000",
            start=datetime(2024, 1, 15),
            end=datetime(2024, 12, 25),
        )

    assert route.called
    request = route.calls[0].request
    assert "dataInicio=15%2F01%2F2024" in str(request.url) or "dataInicio=15/01/2024" in str(
        request.url
    )
    assert "dataFim=25%2F12%2F2024" in str(request.url) or "dataFim=25/12/2024" in str(
        request.url
    )


@pytest.mark.asyncio
@respx.mock
async def test_connector_sets_xml_accept_header():
    """Verify that the connector sets Accept: text/xml header."""
    route = respx.get(
        "https://telemetriaws1.ana.gov.br/ServiceANA.asmx/HidroInventario"
    ).mock(return_value=httpx.Response(200, text=MOCK_STATIONS_XML))

    async with BrazilANAConnector() as conn:
        await conn.fetch_stations()

    request = route.calls[0].request
    assert request.headers["accept"] == "text/xml"
