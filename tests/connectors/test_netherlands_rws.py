"""Tests for the Rijkswaterstaat (Netherlands) connector with mocked HTTP responses."""

from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx

from csfs.connectors.netherlands_rws import NetherlandsRWSConnector

# -- Mock payloads -------------------------------------------------------

MOCK_STATIONS_RESPONSE = {
    "stations": [
        {
            "slug": "lobith",
            "name": "Lobith",
            "coordinates": {"latitude": 51.8575, "longitude": 6.1089},
        },
        {
            "slug": "maastricht-borgharen",
            "name": "Maastricht Borgharen",
            "coordinates": {"latitude": 50.8667, "longitude": 5.6833},
        },
    ],
}

MOCK_STATIONS_LIST_RESPONSE = [
    {
        "slug": "lobith",
        "name": "Lobith",
        "coordinates": {"latitude": 51.8575, "longitude": 6.1089},
    },
]

MOCK_OBSERVATIONS_RESPONSE = {
    "series": [
        {
            "data": [
                [1717243200000, 2150.0],
                [1717246800000, 2175.5],
                [1717250400000, None],
            ],
        },
    ],
}

MOCK_OBSERVATIONS_DICT_RESPONSE = {
    "data": [
        {"dateTime": "2024-06-01T12:00:00+00:00", "value": 2150.0},
        {"dateTime": "2024-06-01T13:00:00+00:00", "value": 2175.5},
    ],
}


# -- Station tests -------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_wrapped_dict():
    """Stations wrapped in a ``stations`` key are parsed correctly."""
    respx.get("https://waterinfo.rws.nl/api/chart/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE),
    )

    async with NetherlandsRWSConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    slugs = {s.native_id for s in stations}
    assert slugs == {"lobith", "maastricht-borgharen"}

    lobith = next(s for s in stations if s.native_id == "lobith")
    assert lobith.id == "netherlands_rws:lobith"
    assert lobith.provider == "netherlands_rws"
    assert lobith.country_code == "NL"
    assert lobith.latitude == pytest.approx(51.8575)
    assert lobith.longitude == pytest.approx(6.1089)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_bare_list():
    """A bare JSON list of stations is accepted too."""
    respx.get("https://waterinfo.rws.nl/api/chart/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_LIST_RESPONSE),
    )

    async with NetherlandsRWSConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "lobith"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty station list returns no stations."""
    respx.get("https://waterinfo.rws.nl/api/chart/stations").mock(
        return_value=httpx.Response(200, json={"stations": []}),
    )

    async with NetherlandsRWSConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_entries_without_slug():
    """Entries lacking a slug are silently skipped."""
    payload = {"stations": [{"name": "No Slug", "coordinates": {"latitude": 0, "longitude": 0}}]}
    respx.get("https://waterinfo.rws.nl/api/chart/stations").mock(
        return_value=httpx.Response(200, json=payload),
    )

    async with NetherlandsRWSConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


# -- Observation tests ---------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_epoch_ms_format():
    """Observations with [epoch_ms, value] pairs are parsed correctly."""
    respx.get("https://waterinfo.rws.nl/api/chart/get").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE),
    )

    now = datetime.now(UTC)
    async with NetherlandsRWSConnector() as conn:
        chunk = await conn.fetch_observations(
            "netherlands_rws:lobith",
            start=now - timedelta(hours=48),
            end=now,
        )

    assert chunk.provider == "netherlands_rws"
    assert chunk.station_id == "netherlands_rws:lobith"
    assert len(chunk.observations) == 3

    assert chunk.observations[0].discharge_m3s == pytest.approx(2150.0)
    assert chunk.observations[0].quality.value == "raw"

    # None value -> MISSING quality
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_dict_format():
    """Observations in {dateTime, value} dict format are parsed correctly."""
    respx.get("https://waterinfo.rws.nl/api/chart/get").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_DICT_RESPONSE),
    )

    now = datetime.now(UTC)
    async with NetherlandsRWSConnector() as conn:
        chunk = await conn.fetch_observations(
            "netherlands_rws:lobith",
            start=now - timedelta(hours=24),
            end=now,
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[1].discharge_m3s == pytest.approx(2175.5)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty():
    """An empty series returns zero observations."""
    respx.get("https://waterinfo.rws.nl/api/chart/get").mock(
        return_value=httpx.Response(200, json={"series": [{"data": []}]}),
    )

    now = datetime.now(UTC)
    async with NetherlandsRWSConnector() as conn:
        chunk = await conn.fetch_observations(
            "netherlands_rws:lobith",
            start=now - timedelta(hours=24),
            end=now,
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_start_after_end():
    """If start >= end the connector returns an empty chunk without any HTTP call."""
    async with NetherlandsRWSConnector() as conn:
        now = datetime.now(UTC)
        chunk = await conn.fetch_observations(
            "netherlands_rws:lobith",
            start=now,
            end=now - timedelta(hours=1),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_strips_prefix():
    """The provider prefix is stripped from station_id to get the native slug."""
    route = respx.get("https://waterinfo.rws.nl/api/chart/get").mock(
        return_value=httpx.Response(200, json={"data": []}),
    )

    now = datetime.now(UTC)
    async with NetherlandsRWSConnector() as conn:
        await conn.fetch_observations(
            "netherlands_rws:lobith",
            start=now - timedelta(hours=1),
            end=now,
        )

    assert route.called
    request = route.calls[0].request
    assert "lobith" in str(request.url)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest_uses_48h_window():
    """fetch_latest requests a 48-hour window."""
    route = respx.get("https://waterinfo.rws.nl/api/chart/get").mock(
        return_value=httpx.Response(200, json={"data": []}),
    )

    async with NetherlandsRWSConnector() as conn:
        chunk = await conn.fetch_latest("netherlands_rws:lobith")

    assert route.called
    assert chunk.provider == "netherlands_rws"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_invalid_json():
    """A non-JSON response raises DataFormatError."""
    from csfs.core.exceptions import DataFormatError

    respx.get("https://waterinfo.rws.nl/api/chart/stations").mock(
        return_value=httpx.Response(200, content=b"not json", headers={"content-type": "text/plain"}),
    )

    async with NetherlandsRWSConnector() as conn:
        with pytest.raises(DataFormatError, match="not valid JSON"):
            await conn.fetch_stations()


@pytest.mark.asyncio
@respx.mock
async def test_registration():
    """The connector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("netherlands_rws")
    assert cls is NetherlandsRWSConnector
