"""Tests for the Netherlands RWS connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.netherlands_rws import NetherlandsRwsConnector

BASE_URL = "https://geo.rijkswaterstaat.nl"

# -- Station fixtures (GeoJSON FeatureCollection) --------------------------

MOCK_STATIONS_GEOJSON = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [4.3571, 51.8908],
            },
            "properties": {"NAAM": "Rotterdam"},
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [5.1214, 52.0907],
            },
            "properties": {"NAAM": "Utrecht"},
        },
    ],
}

MOCK_STATIONS_WITH_BAD_ENTRIES = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [4.3571, 51.8908],
            },
            "properties": {"NAAM": "Rotterdam"},
        },
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": []},
            "properties": {"NAAM": "NoCoords"},
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [5.5, 52.5],
            },
            "properties": {"NAAM": ""},
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [5.1214, 52.0907],
            },
            "properties": {"NAAM": "Utrecht"},
        },
    ],
}

MOCK_STATIONS_EMPTY = {
    "type": "FeatureCollection",
    "features": [],
}

# -- Latest observations fixtures ------------------------------------------

MOCK_LATEST_GEOJSON = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [4.3571, 51.8908],
            },
            "properties": {
                "NAAM": "Rotterdam",
                "WAARDE_LAATSTE_METING": 125.4,
                "TIJDSTIP_LAATSTE_METING": "2024-06-01T14:00:00",
            },
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [5.1214, 52.0907],
            },
            "properties": {
                "NAAM": "Utrecht",
                "WAARDE_LAATSTE_METING": 87.2,
                "TIJDSTIP_LAATSTE_METING": "2024-06-01T14:15:00",
            },
        },
    ],
}

MOCK_LATEST_MISSING_VALUE = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [4.3571, 51.8908],
            },
            "properties": {
                "NAAM": "Rotterdam",
                "WAARDE_LAATSTE_METING": None,
                "TIJDSTIP_LAATSTE_METING": "2024-06-01T14:00:00",
            },
        },
    ],
}

WFS_PATH = "/services/ogc/hws/DDAPI20/ows"


# ======================================================================
# Station tests
# ======================================================================


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_geojson():
    """Station features are parsed from GeoJSON FeatureCollection."""
    respx.get(f"{BASE_URL}{WFS_PATH}").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_GEOJSON),
    )

    async with NetherlandsRwsConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    names = {s.native_id for s in stations}
    assert names == {"Rotterdam", "Utrecht"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_coords_correct():
    """Coordinates are correctly extracted from GeoJSON [lon, lat]."""
    respx.get(f"{BASE_URL}{WFS_PATH}").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_GEOJSON),
    )

    async with NetherlandsRwsConnector() as conn:
        stations = await conn.fetch_stations()

    rotterdam = next(s for s in stations if s.native_id == "Rotterdam")
    assert rotterdam.id == "netherlands_rws:Rotterdam"
    assert rotterdam.provider == "netherlands_rws"
    assert rotterdam.country_code == "NL"
    assert rotterdam.latitude == pytest.approx(51.8908)
    assert rotterdam.longitude == pytest.approx(4.3571)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_invalid_entries():
    """Entries with empty name or missing coordinates are skipped."""
    respx.get(f"{BASE_URL}{WFS_PATH}").mock(
        return_value=httpx.Response(
            200, json=MOCK_STATIONS_WITH_BAD_ENTRIES,
        ),
    )

    async with NetherlandsRwsConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    names = {s.native_id for s in stations}
    assert names == {"Rotterdam", "Utrecht"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty features list returns no stations."""
    respx.get(f"{BASE_URL}{WFS_PATH}").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_EMPTY),
    )

    async with NetherlandsRwsConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


# ======================================================================
# Observation tests
# ======================================================================


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_latest():
    """Latest observation is correctly extracted for the target station."""
    respx.get(f"{BASE_URL}{WFS_PATH}").mock(
        return_value=httpx.Response(200, json=MOCK_LATEST_GEOJSON),
    )

    async with NetherlandsRwsConnector() as conn:
        chunk = await conn.fetch_observations(
            "netherlands_rws:Rotterdam",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "netherlands_rws"
    assert chunk.station_id == "netherlands_rws:Rotterdam"
    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(125.4)
    assert chunk.observations[0].quality.value == "raw"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_station_not_found():
    """When the target station is not in the response, return empty chunk."""
    respx.get(f"{BASE_URL}{WFS_PATH}").mock(
        return_value=httpx.Response(200, json=MOCK_LATEST_GEOJSON),
    )

    async with NetherlandsRwsConnector() as conn:
        chunk = await conn.fetch_observations(
            "netherlands_rws:NonExistent",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_missing_value():
    """A null measurement value results in MISSING quality flag."""
    respx.get(f"{BASE_URL}{WFS_PATH}").mock(
        return_value=httpx.Response(
            200, json=MOCK_LATEST_MISSING_VALUE,
        ),
    )

    async with NetherlandsRwsConnector() as conn:
        chunk = await conn.fetch_observations(
            "netherlands_rws:Rotterdam",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[0].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty_features():
    """An empty features list returns zero observations."""
    respx.get(f"{BASE_URL}{WFS_PATH}").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_EMPTY),
    )

    async with NetherlandsRwsConnector() as conn:
        chunk = await conn.fetch_observations(
            "netherlands_rws:Rotterdam",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


# ======================================================================
# Registry
# ======================================================================


def test_connector_is_registered():
    """The connector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("netherlands_rws")
    assert cls is NetherlandsRwsConnector


# ======================================================================
# Additional coverage tests — error branches, edge cases
# ======================================================================


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_http_error_raises_connector_error():
    """HTTPStatusError on station listing raises ConnectorError (lines 87-88)."""
    from csfs.core.exceptions import ConnectorError

    respx.get(f"{BASE_URL}{WFS_PATH}").mock(
        return_value=httpx.Response(500),
    )

    async with NetherlandsRwsConnector() as conn:
        with pytest.raises(ConnectorError, match="Failed to fetch station list"):
            await conn.fetch_stations()


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_http_error_raises_connector_error():
    """HTTPStatusError on observations raises ConnectorError (lines 118-119)."""
    from csfs.core.exceptions import ConnectorError

    respx.get(f"{BASE_URL}{WFS_PATH}").mock(
        return_value=httpx.Response(500),
    )

    async with NetherlandsRwsConnector() as conn:
        with pytest.raises(ConnectorError, match="Failed to fetch observations"):
            await conn.fetch_observations(
                "netherlands_rws:Rotterdam",
                start=datetime(2024, 6, 1),
                end=datetime(2024, 6, 2),
            )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest_delegates():
    """fetch_latest delegates to fetch_observations (lines 133-134)."""
    respx.get(f"{BASE_URL}{WFS_PATH}").mock(
        return_value=httpx.Response(200, json=MOCK_LATEST_GEOJSON),
    )

    async with NetherlandsRwsConnector() as conn:
        chunk = await conn.fetch_latest("netherlands_rws:Rotterdam")

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(125.4)


@pytest.mark.asyncio
@respx.mock
async def test_parse_station_features_invalid_coords_skipped():
    """Features with non-numeric coordinates are skipped (lines 173-180)."""
    bad_coords = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": ["not_a_number", "also_bad"],
                },
                "properties": {"NAAM": "BadCoords"},
            },
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [4.3571, 51.8908],
                },
                "properties": {"NAAM": "Rotterdam"},
            },
        ],
    }
    respx.get(f"{BASE_URL}{WFS_PATH}").mock(
        return_value=httpx.Response(200, json=bad_coords),
    )

    async with NetherlandsRwsConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "Rotterdam"


@pytest.mark.asyncio
@respx.mock
async def test_parse_latest_missing_timestamp_skipped():
    """Observation with missing timestamp is skipped (lines 213-218)."""
    missing_ts = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [4.3571, 51.8908]},
                "properties": {
                    "NAAM": "Rotterdam",
                    "WAARDE_LAATSTE_METING": 125.4,
                    "TIJDSTIP_LAATSTE_METING": None,
                },
            },
        ],
    }
    respx.get(f"{BASE_URL}{WFS_PATH}").mock(
        return_value=httpx.Response(200, json=missing_ts),
    )

    async with NetherlandsRwsConnector() as conn:
        chunk = await conn.fetch_observations(
            "netherlands_rws:Rotterdam",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_parse_latest_invalid_timestamp_skipped():
    """Observation with invalid timestamp is skipped (lines 222-229)."""
    invalid_ts = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [4.3571, 51.8908]},
                "properties": {
                    "NAAM": "Rotterdam",
                    "WAARDE_LAATSTE_METING": 125.4,
                    "TIJDSTIP_LAATSTE_METING": "not-a-date",
                },
            },
        ],
    }
    respx.get(f"{BASE_URL}{WFS_PATH}").mock(
        return_value=httpx.Response(200, json=invalid_ts),
    )

    async with NetherlandsRwsConnector() as conn:
        chunk = await conn.fetch_observations(
            "netherlands_rws:Rotterdam",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0
