"""Tests for the Slovenia ARSO connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.slovenia_arso import (
    _FEED_PATH,
    SloveniaArsoConnector,
)
from csfs.core.exceptions import DataFormatError

BASE_URL = "https://www.arso.gov.si"
FEED_URL = f"{BASE_URL}{_FEED_PATH}"

# Two discharge stations, one level-only station (no <pretok>), and one with
# an empty pretok — only the two real discharge stations should survive.
MOCK_FEED = """<?xml version="1.0" encoding="UTF-8"?>
<arsopodatki verzija="1.5">
  <vir>Agencija RS za okolje</vir>
  <postaja sifra="1060" wgs84_dolzina="15.9953" wgs84_sirina="46.68124" kota_0="202.35">
    <reka>Mura</reka>
    <merilno_mesto>Gornja Radgona</merilno_mesto>
    <ime_kratko>Mura - Gor. Radgona I</ime_kratko>
    <datum>2024-06-01 13:00</datum>
    <datum_cet>2024-06-01 12:00</datum_cet>
    <vodostaj>73</vodostaj>
    <pretok>74.6</pretok>
    <temp_vode>22.1</temp_vode>
  </postaja>
  <postaja sifra="2150" wgs84_dolzina="14.5103" wgs84_sirina="46.0552" kota_0="280.0">
    <reka>Ljubljanica</reka>
    <merilno_mesto>Moste</merilno_mesto>
    <ime_kratko>Ljubljanica - Moste</ime_kratko>
    <datum_cet>2024-06-01 12:00</datum_cet>
    <vodostaj>120</vodostaj>
    <pretok>30.2</pretok>
  </postaja>
  <postaja sifra="9999" wgs84_dolzina="14.0" wgs84_sirina="46.0" kota_0="300.0">
    <reka>LevelOnly</reka>
    <ime_kratko>Level only station</ime_kratko>
    <datum_cet>2024-06-01 12:00</datum_cet>
    <vodostaj>50</vodostaj>
  </postaja>
  <postaja sifra="8888" wgs84_dolzina="14.1" wgs84_sirina="46.1" kota_0="310.0">
    <ime_kratko>Empty discharge</ime_kratko>
    <datum_cet>2024-06-01 12:00</datum_cet>
    <pretok></pretok>
  </postaja>
</arsopodatki>
"""


@respx.mock
@pytest.mark.asyncio
async def test_fetch_stations_returns_only_discharge_stations():
    respx.get(FEED_URL).mock(return_value=httpx.Response(200, text=MOCK_FEED))

    async with SloveniaArsoConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"1060", "2150"}


@respx.mock
@pytest.mark.asyncio
async def test_station_fields_parsed():
    respx.get(FEED_URL).mock(return_value=httpx.Response(200, text=MOCK_FEED))

    async with SloveniaArsoConnector() as conn:
        stations = await conn.fetch_stations()

    mura = next(s for s in stations if s.native_id == "1060")
    assert mura.id == "slovenia_arso:1060"
    assert mura.provider == "slovenia_arso"
    assert mura.name == "Mura - Gor. Radgona I"
    assert mura.country_code == "SI"
    assert mura.river == "Mura"
    assert mura.latitude == pytest.approx(46.68124)
    assert mura.longitude == pytest.approx(15.9953)
    assert mura.elevation_m == pytest.approx(202.35)


@respx.mock
@pytest.mark.asyncio
async def test_datum_cet_converted_to_utc():
    """datum_cet is CET (UTC+1); 12:00 CET -> 11:00 UTC."""
    respx.get(FEED_URL).mock(return_value=httpx.Response(200, text=MOCK_FEED))

    async with SloveniaArsoConnector() as conn:
        chunk = await conn.fetch_latest("slovenia_arso:1060")

    assert len(chunk.observations) == 1
    obs = chunk.observations[0]
    assert obs.timestamp == datetime(2024, 6, 1, 11, 0, tzinfo=UTC)
    assert obs.discharge_m3s == pytest.approx(74.6)


@respx.mock
@pytest.mark.asyncio
async def test_fetch_observations_within_window():
    respx.get(FEED_URL).mock(return_value=httpx.Response(200, text=MOCK_FEED))

    async with SloveniaArsoConnector() as conn:
        chunk = await conn.fetch_observations(
            "slovenia_arso:2150",
            start=datetime(2024, 6, 1, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 1, 23, 59, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(30.2)


@respx.mock
@pytest.mark.asyncio
async def test_fetch_observations_outside_window_is_empty():
    respx.get(FEED_URL).mock(return_value=httpx.Response(200, text=MOCK_FEED))

    async with SloveniaArsoConnector() as conn:
        chunk = await conn.fetch_observations(
            "slovenia_arso:1060",
            start=datetime(2024, 6, 2, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 3, 0, 0, tzinfo=UTC),
        )

    assert chunk.observations == []
    assert chunk.provider == "slovenia_arso"


@respx.mock
@pytest.mark.asyncio
async def test_unknown_station_returns_empty_chunk():
    respx.get(FEED_URL).mock(return_value=httpx.Response(200, text=MOCK_FEED))

    async with SloveniaArsoConnector() as conn:
        chunk = await conn.fetch_latest("slovenia_arso:0000")

    assert chunk.observations == []


@respx.mock
@pytest.mark.asyncio
async def test_empty_feed_raises():
    empty = '<?xml version="1.0" encoding="UTF-8"?><arsopodatki></arsopodatki>'
    respx.get(FEED_URL).mock(return_value=httpx.Response(200, text=empty))

    async with SloveniaArsoConnector() as conn:
        with pytest.raises(DataFormatError):
            await conn.fetch_stations()


@respx.mock
@pytest.mark.asyncio
async def test_snapshot_fetched_once_and_cached():
    route = respx.get(FEED_URL).mock(
        return_value=httpx.Response(200, text=MOCK_FEED),
    )

    async with SloveniaArsoConnector() as conn:
        await conn.fetch_stations()
        await conn.fetch_latest("slovenia_arso:1060")
        await conn.fetch_observations(
            "slovenia_arso:2150",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    # One HTTP round-trip despite three calls — the snapshot is cached.
    assert route.call_count == 1


def test_connector_is_registered():
    from csfs.core.registry import get_connector

    assert get_connector("slovenia_arso") is SloveniaArsoConnector
