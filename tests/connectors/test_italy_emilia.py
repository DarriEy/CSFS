"""Tests for the ARPAE Emilia-Romagna (Italy) discharge connector.

The connector consumes ARPAE's open-data newline-delimited (NDJSON) feed of
observed instantaneous discharge.  All HTTP is mocked with respx; the suite
blocks real network access via tests/conftest.py.
"""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.italy_emilia import ItalyEmiliaConnector
from csfs.core.registry import discover, get_connector

_FEED_URL = (
    "https://dati-simc.arpae.it"
    "/opendata/osservati/portata_istantanea/portata_istantanea.json"
)

# Two stations, multiple timestamps each, NDJSON (one JSON object per line).
# B13226 is discharge in m3/s; B01019 is the station name.
_LINES = [
    # Pontelagoscuro / Po, t1
    '{"version":"0.1","network":"simnpr","ident":null,'
    '"lon":1160807,"lat":4488830,"date":"2024-06-01T00:00:00Z",'
    '"data":[{"vars":{"B01019":{"v":"Pontelagoscuro"},"B01194":{"v":"simnpr"},'
    '"B05001":{"v":44.88830},"B06001":{"v":11.60807}}},'
    '{"timerange":[254,0,0],"level":[1,null,null,null],'
    '"vars":{"B13226":{"v":787.18}}}]}',
    # Pontelagoscuro / Po, t2
    '{"version":"0.1","network":"simnpr","ident":null,'
    '"lon":1160807,"lat":4488830,"date":"2024-06-01T00:15:00Z",'
    '"data":[{"vars":{"B01019":{"v":"Pontelagoscuro"}}},'
    '{"vars":{"B13226":{"v":790.00}}}]}',
    # Boretto / Po, t1
    '{"version":"0.1","network":"simnpr","ident":null,'
    '"lon":1055893,"lat":4490633,"date":"2024-06-01T00:00:00Z",'
    '"data":[{"vars":{"B01019":{"v":"Boretto"}}},'
    '{"vars":{"B13226":{"v":646.54}}}]}',
    # Pontelagoscuro / Po, t3 -- OUTSIDE the query window used below
    '{"version":"0.1","network":"simnpr","ident":null,'
    '"lon":1160807,"lat":4488830,"date":"2024-06-05T00:00:00Z",'
    '"data":[{"vars":{"B01019":{"v":"Pontelagoscuro"}}},'
    '{"vars":{"B13226":{"v":900.00}}}]}',
    # Malformed line -- must be skipped, not crash.
    "{not valid json",
]
_FEED_BODY = "\n".join(_LINES) + "\n"


def _mock_feed() -> None:
    respx.get(_FEED_URL).mock(
        return_value=httpx.Response(200, text=_FEED_BODY)
    )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_distinct_stations():
    """One Station per distinct gauge, with name and decimal coordinates."""
    _mock_feed()
    async with ItalyEmiliaConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    by_name = {s.name: s for s in stations}
    assert set(by_name) == {"Pontelagoscuro", "Boretto"}

    pont = by_name["Pontelagoscuro"]
    assert pont.id == "italy_emilia:1160807,4488830,simnpr"
    assert pont.native_id == "1160807,4488830,simnpr"
    assert pont.provider == "italy_emilia"
    assert pont.country_code == "IT"
    assert pont.river == "Po"
    # lon/lat stored as decimal degrees * 1e5 in the feed.
    assert pont.latitude == pytest.approx(44.88830)
    assert pont.longitude == pytest.approx(11.60807)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_returns_discharge_m3s():
    """Observations carry discharge in m3/s (B13226), not water level."""
    _mock_feed()
    async with ItalyEmiliaConnector() as conn:
        chunk = await conn.fetch_observations(
            "italy_emilia:1160807,4488830,simnpr",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert chunk.provider == "italy_emilia"
    assert chunk.station_id == "italy_emilia:1160807,4488830,simnpr"
    # Two in-window timestamps for this station; the 2024-06-05 one is excluded.
    assert len(chunk.observations) == 2

    # Sorted ascending by timestamp.
    assert chunk.observations[0].timestamp == datetime(2024, 6, 1, 0, 0, tzinfo=UTC)
    assert chunk.observations[1].timestamp == datetime(2024, 6, 1, 0, 15, tzinfo=UTC)

    # Discharge values are the m3/s figures (Po river magnitude), not ~10 m level.
    assert chunk.observations[0].discharge_m3s == pytest.approx(787.18)
    assert chunk.observations[1].discharge_m3s == pytest.approx(790.00)
    assert chunk.observations[0].quality.value == "raw"
    # All discharge values are well above any plausible water-level reading.
    assert all(o.discharge_m3s > 100 for o in chunk.observations)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_filters_by_station_and_window():
    """Only the requested station and time window are returned."""
    _mock_feed()
    async with ItalyEmiliaConnector() as conn:
        # Boretto only has one record, at 00:00.
        chunk = await conn.fetch_observations(
            "italy_emilia:1055893,4490633,simnpr",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )
    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(646.54)


@pytest.mark.asyncio
@respx.mock
async def test_feed_downloaded_once_and_cached():
    """The ~2 MB feed is fetched a single time and reused across calls."""
    route = respx.get(_FEED_URL).mock(
        return_value=httpx.Response(200, text=_FEED_BODY)
    )
    async with ItalyEmiliaConnector() as conn:
        await conn.fetch_stations()
        await conn.fetch_observations(
            "italy_emilia:1160807,4488830,simnpr",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )
    assert route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_empty_feed_raises():
    """A feed with no parseable records raises a ConnectorError."""
    from csfs.core.exceptions import ConnectorError

    respx.get(_FEED_URL).mock(return_value=httpx.Response(200, text="\n\n"))
    async with ItalyEmiliaConnector() as conn:
        with pytest.raises(ConnectorError):
            await conn.fetch_stations()


def test_connector_is_registered():
    """The connector is discoverable under its slug."""
    discover()
    cls = get_connector("italy_emilia")
    assert cls is ItalyEmiliaConnector
    assert cls.slug == "italy_emilia"
    assert cls.country_codes == ["IT"]
