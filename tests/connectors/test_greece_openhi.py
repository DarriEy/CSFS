"""Tests for the Greece OpenHI connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.greece_openhi import GreeceOpenhiConnector

BASE_URL = "https://system.openhi.net"

# -- Station fixtures (DRF-paginated, GeoJSON point coords) ---------------

MOCK_STATIONS_PAGE1 = {
    "count": 3,
    "next": f"{BASE_URL}/api/stations/?page=2",
    "previous": None,
    "results": [
        {
            "id": "STA001",
            "name": "Aliakmonas - Ilarion",
            "point": {
                "type": "Point",
                "coordinates": [21.74, 40.19],  # [lon, lat]
            },
            "river": "Aliakmonas",
        },
        {
            "id": "STA002",
            "name": "Acheloos - Kremasta",
            "point": {
                "type": "Point",
                "coordinates": [21.51, 38.88],
            },
            "river": "Acheloos",
        },
    ],
}

MOCK_STATIONS_PAGE2 = {
    "count": 3,
    "next": None,
    "previous": f"{BASE_URL}/api/stations/?page=1",
    "results": [
        {
            "id": "STA003",
            "name": "Pinios - Larissa",
            "point": {
                "type": "Point",
                "coordinates": [22.42, 39.64],
            },
            "river": "Pinios",
        },
    ],
}

MOCK_STATIONS_SINGLE_PAGE = {
    "count": 2,
    "next": None,
    "previous": None,
    "results": [
        {
            "id": "STA001",
            "name": "Aliakmonas - Ilarion",
            "point": {
                "type": "Point",
                "coordinates": [21.74, 40.19],
            },
            "river": "Aliakmonas",
        },
        {
            "id": "STA002",
            "name": "Acheloos - Kremasta",
            "point": {
                "type": "Point",
                "coordinates": [21.51, 38.88],
            },
            "river": "Acheloos",
        },
    ],
}

# Entries that should be skipped (no id / no coords)
MOCK_STATIONS_WITH_BAD_ENTRIES = {
    "count": 4,
    "next": None,
    "previous": None,
    "results": [
        {
            "id": "STA001",
            "name": "Aliakmonas - Ilarion",
            "point": {"type": "Point", "coordinates": [21.74, 40.19]},
            "river": "Aliakmonas",
        },
        {
            "id": "",
            "name": "Missing ID",
            "point": {"type": "Point", "coordinates": [22.0, 39.0]},
        },
        {
            "id": "STA003",
            "name": "No Coords",
            "river": "Pinios",
        },
        {
            "id": "STA002",
            "name": "Acheloos - Kremasta",
            "point": {"type": "Point", "coordinates": [21.51, 38.88]},
            "river": "Acheloos",
        },
    ],
}

# -- Observation fixtures --------------------------------------------------

MOCK_OBSERVATIONS = [
    {
        "timestamp": "2024-06-01T12:00:00",
        "value": 45.3,
        "flag": "VALIDATED",
    },
    {
        "timestamp": "2024-06-01T12:15:00",
        "value": 44.1,
        "flag": "RAW",
    },
    {
        "timestamp": "2024-06-01T12:30:00",
        "value": None,
        "flag": "MISSING",
    },
]


# ======================================================================
# Station tests
# ======================================================================


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_paginated():
    """Station list is parsed across multiple paginated responses."""
    respx.get(f"{BASE_URL}/api/stations/", params={"page": "1"}).mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_PAGE1),
    )
    respx.get(f"{BASE_URL}/api/stations/", params={"page": "2"}).mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_PAGE2),
    )

    async with GreeceOpenhiConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 3
    ids = {s.native_id for s in stations}
    assert ids == {"STA001", "STA002", "STA003"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_single_page():
    """Single-page response (next=None) works correctly."""
    respx.get(f"{BASE_URL}/api/stations/", params={"page": "1"}).mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_SINGLE_PAGE),
    )

    async with GreeceOpenhiConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2

    ilarion = next(s for s in stations if s.native_id == "STA001")
    assert ilarion.id == "greece_openhi:STA001"
    assert ilarion.provider == "greece_openhi"
    assert ilarion.country_code == "GR"
    assert ilarion.river == "Aliakmonas"
    assert ilarion.latitude == pytest.approx(40.19)
    assert ilarion.longitude == pytest.approx(21.74)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_invalid_entries():
    """Entries with missing id or coordinates are skipped."""
    respx.get(f"{BASE_URL}/api/stations/", params={"page": "1"}).mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_WITH_BAD_ENTRIES),
    )

    async with GreeceOpenhiConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"STA001", "STA002"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty results list returns no stations."""
    respx.get(f"{BASE_URL}/api/stations/", params={"page": "1"}).mock(
        return_value=httpx.Response(200, json={
            "count": 0, "next": None, "previous": None, "results": [],
        }),
    )

    async with GreeceOpenhiConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


# ======================================================================
# Observation tests
# ======================================================================


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_json():
    """Observations are correctly parsed with quality flags."""
    respx.get(f"{BASE_URL}/api/stations/STA001/data/").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS),
    )

    async with GreeceOpenhiConnector() as conn:
        chunk = await conn.fetch_observations(
            "greece_openhi:STA001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "greece_openhi"
    assert chunk.station_id == "greece_openhi:STA001"
    assert len(chunk.observations) == 3

    assert chunk.observations[0].discharge_m3s == pytest.approx(45.3)
    assert chunk.observations[0].quality.value == "good"

    assert chunk.observations[1].discharge_m3s == pytest.approx(44.1)
    assert chunk.observations[1].quality.value == "raw"

    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty():
    """An empty observation list returns zero observations."""
    respx.get(f"{BASE_URL}/api/stations/STA001/data/").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with GreeceOpenhiConnector() as conn:
        chunk = await conn.fetch_observations(
            "greece_openhi:STA001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_wrapped_in_results():
    """Observations wrapped in a 'results' key (DRF pagination) are parsed."""
    wrapped = {"results": MOCK_OBSERVATIONS[:2]}
    respx.get(f"{BASE_URL}/api/stations/STA001/data/").mock(
        return_value=httpx.Response(200, json=wrapped),
    )

    async with GreeceOpenhiConnector() as conn:
        chunk = await conn.fetch_observations(
            "greece_openhi:STA001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_fallback_to_ts_records():
    """When /api/stations/{id}/data/ returns 404, falls back to /api/ts_records/."""
    respx.get(f"{BASE_URL}/api/stations/STA001/data/").mock(
        return_value=httpx.Response(404),
    )
    respx.get(f"{BASE_URL}/api/ts_records/").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS[:1]),
    )

    async with GreeceOpenhiConnector() as conn:
        chunk = await conn.fetch_observations(
            "greece_openhi:STA001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(45.3)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_suspect_flag():
    """SUSPECT flag is mapped correctly."""
    data = [
        {"timestamp": "2024-06-01T12:00:00", "value": 30.0, "flag": "SUSPECT"},
    ]
    respx.get(f"{BASE_URL}/api/stations/STA001/data/").mock(
        return_value=httpx.Response(200, json=data),
    )

    async with GreeceOpenhiConnector() as conn:
        chunk = await conn.fetch_observations(
            "greece_openhi:STA001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.observations[0].quality.value == "suspect"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_date_key():
    """Observations that use 'date' instead of 'timestamp' are parsed."""
    data = [
        {"date": "2024-06-01T12:00:00", "value": 25.0, "flag": "RAW"},
    ]
    respx.get(f"{BASE_URL}/api/stations/STA001/data/").mock(
        return_value=httpx.Response(200, json=data),
    )

    async with GreeceOpenhiConnector() as conn:
        chunk = await conn.fetch_observations(
            "greece_openhi:STA001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(25.0)


# ======================================================================
# Coordinate extraction
# ======================================================================


@pytest.mark.asyncio
@respx.mock
async def test_stations_with_flat_lat_lon():
    """Stations with flat latitude/longitude keys (no GeoJSON point)."""
    page = {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [
            {
                "id": "STA010",
                "name": "Flat coords station",
                "latitude": 39.5,
                "longitude": 22.0,
                "river": "Penios",
            },
        ],
    }
    respx.get(f"{BASE_URL}/api/stations/", params={"page": "1"}).mock(
        return_value=httpx.Response(200, json=page),
    )

    async with GreeceOpenhiConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].latitude == pytest.approx(39.5)
    assert stations[0].longitude == pytest.approx(22.0)


# ======================================================================
# Registry
# ======================================================================


def test_connector_is_registered():
    """The connector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("greece_openhi")
    assert cls is GreeceOpenhiConnector
