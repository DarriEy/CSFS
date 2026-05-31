"""Tests for the Brazil ANA connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.brazil_ana import _SERVICE, BrazilAnaConnector
from csfs.core.exceptions import ConnectorError

BASE_URL = "https://telemetriaws1.ana.gov.br"
INVENTORY_URL = f"{BASE_URL}{_SERVICE}/HidroInventario"
SERIES_URL = f"{BASE_URL}{_SERVICE}/HidroSerieHistorica"

# One valid fluviometric station and one with missing coords (to be skipped).
MOCK_INVENTORY = """<?xml version="1.0" encoding="utf-8"?>
<DataSet xmlns:diffgr="urn:schemas-microsoft-com:xml-diffgram-v1">
  <diffgr:diffgram>
    <NewDataSet>
      <Table>
        <BaciaCodigo>1</BaciaCodigo>
        <Codigo>17050001</Codigo>
        <Nome>OBIDOS</Nome>
        <Latitude>-1.9192</Latitude>
        <Longitude>-55.5131</Longitude>
        <RioNome>RIO SOLIMOES-AMAZONAS</RioNome>
        <AreaDrenagem>4.67e+006</AreaDrenagem>
        <Altitude>17.0</Altitude>
      </Table>
      <Table>
        <Codigo>00047008</Codigo>
        <Nome>CURUCA</Nome>
        <Latitude></Latitude>
        <Longitude></Longitude>
      </Table>
    </NewDataSet>
  </diffgr:diffgram>
</DataSet>
"""

# January 2025: day 1 has both raw (100) and consolidated (105); day 2 raw only.
MOCK_SERIES = """<?xml version="1.0" encoding="utf-8"?>
<DataSet xmlns:diffgr="urn:schemas-microsoft-com:xml-diffgram-v1">
  <diffgr:diffgram>
    <NewDataSet>
      <SerieHistorica>
        <DataHora>2025-01-01 00:00:00</DataHora>
        <NivelConsistencia>1</NivelConsistencia>
        <Vazao01>100.0</Vazao01>
        <Vazao02>110.0</Vazao02>
      </SerieHistorica>
      <SerieHistorica>
        <DataHora>2025-01-01 00:00:00</DataHora>
        <NivelConsistencia>2</NivelConsistencia>
        <Vazao01>105.0</Vazao01>
      </SerieHistorica>
    </NewDataSet>
  </diffgr:diffgram>
</DataSet>
"""


@respx.mock
@pytest.mark.asyncio
async def test_fetch_stations_enumerates_basins_and_skips_missing_coords():
    route = respx.get(url__startswith=INVENTORY_URL).mock(
        return_value=httpx.Response(200, text=MOCK_INVENTORY),
    )

    async with BrazilAnaConnector() as conn:
        stations = await conn.fetch_stations()

    # 8 basin calls, deduped by code; the no-coords station is dropped.
    assert route.call_count == 8
    assert len(stations) == 1
    obidos = stations[0]
    assert obidos.id == "brazil_ana:17050001"
    assert obidos.native_id == "17050001"
    assert obidos.name == "OBIDOS"
    assert obidos.country_code == "BR"
    assert obidos.river == "RIO SOLIMOES-AMAZONAS"
    assert obidos.latitude == pytest.approx(-1.9192)
    assert obidos.longitude == pytest.approx(-55.5131)
    assert obidos.catchment_area_km2 == pytest.approx(4_670_000.0)


@respx.mock
@pytest.mark.asyncio
async def test_fetch_stations_cached_after_first_call():
    route = respx.get(url__startswith=INVENTORY_URL).mock(
        return_value=httpx.Response(200, text=MOCK_INVENTORY),
    )

    async with BrazilAnaConnector() as conn:
        await conn.fetch_stations()
        await conn.fetch_stations()

    # Second call uses the cache — still only the original 8 basin requests.
    assert route.call_count == 8


@respx.mock
@pytest.mark.asyncio
async def test_fetch_observations_expands_daily_and_prefers_consolidated():
    respx.get(url__startswith=SERIES_URL).mock(
        return_value=httpx.Response(200, text=MOCK_SERIES),
    )

    async with BrazilAnaConnector() as conn:
        chunk = await conn.fetch_observations(
            "brazil_ana:17050001",
            start=datetime(2025, 1, 1, tzinfo=UTC),
            end=datetime(2025, 1, 31, tzinfo=UTC),
        )

    assert chunk.station_id == "brazil_ana:17050001"
    assert len(chunk.observations) == 2

    day1, day2 = chunk.observations
    assert day1.timestamp == datetime(2025, 1, 1, tzinfo=UTC)
    # Consolidated (105) wins over raw (100), and is flagged GOOD.
    assert day1.discharge_m3s == pytest.approx(105.0)
    assert day1.quality.value == "good"

    assert day2.timestamp == datetime(2025, 1, 2, tzinfo=UTC)
    assert day2.discharge_m3s == pytest.approx(110.0)
    assert day2.quality.value == "raw"


@respx.mock
@pytest.mark.asyncio
async def test_fetch_observations_filters_to_window():
    respx.get(url__startswith=SERIES_URL).mock(
        return_value=httpx.Response(200, text=MOCK_SERIES),
    )

    async with BrazilAnaConnector() as conn:
        # Only day 2 falls inside this window.
        chunk = await conn.fetch_observations(
            "brazil_ana:17050001",
            start=datetime(2025, 1, 2, tzinfo=UTC),
            end=datetime(2025, 1, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].timestamp == datetime(2025, 1, 2, tzinfo=UTC)


@respx.mock
@pytest.mark.asyncio
async def test_fetch_observations_empty_series():
    empty = (
        '<?xml version="1.0"?>'
        '<DataSet xmlns:diffgr="urn:schemas-microsoft-com:xml-diffgram-v1">'
        "<diffgr:diffgram><NewDataSet></NewDataSet></diffgr:diffgram></DataSet>"
    )
    respx.get(url__startswith=SERIES_URL).mock(
        return_value=httpx.Response(200, text=empty),
    )

    async with BrazilAnaConnector() as conn:
        chunk = await conn.fetch_observations(
            "brazil_ana:17050001",
            start=datetime(2025, 1, 1, tzinfo=UTC),
            end=datetime(2025, 1, 31, tzinfo=UTC),
        )

    assert chunk.observations == []
    assert chunk.provider == "brazil_ana"


@respx.mock
@pytest.mark.asyncio
async def test_fetch_observations_http_error_raises():
    respx.get(url__startswith=SERIES_URL).mock(
        return_value=httpx.Response(500),
    )

    async with BrazilAnaConnector() as conn:
        with pytest.raises(ConnectorError):
            await conn.fetch_observations(
                "brazil_ana:17050001",
                start=datetime(2025, 1, 1, tzinfo=UTC),
                end=datetime(2025, 1, 31, tzinfo=UTC),
            )


@respx.mock
@pytest.mark.asyncio
async def test_fetch_stations_tolerates_basin_error():
    # First basin errors; the rest return the inventory. Station still surfaces.
    responses = [httpx.Response(500)] + [
        httpx.Response(200, text=MOCK_INVENTORY) for _ in range(7)
    ]
    respx.get(url__startswith=INVENTORY_URL).mock(side_effect=responses)

    async with BrazilAnaConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "17050001"


def test_connector_is_registered():
    from csfs.core.registry import get_connector

    assert get_connector("brazil_ana") is BrazilAnaConnector
