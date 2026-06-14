"""Tests for the Chile DGA connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.chile_dga import ChileDgaConnector

BASE_URL = "https://rest-sit.mop.gob.cl"
QUERY_PATH = (
    "/arcgis/rest/services/DGA"
    "/Red_Hidrometrica/MapServer/0/query"
)

# -- Station fixtures (ArcGIS JSON) ----------------------------------------

MOCK_STATIONS_ARCGIS = {
    "objectIdFieldName": "OBJECTID",
    "features": [
        {
            "attributes": {
                "codigo_estacion": "04530001",
                "nombre_estacion": "Rio Mapocho en Los Almendros",
                "rio": "Mapocho",
                "area_drenaje": 620.5,
                "altitud": 1020.0,
            },
            "geometry": {"x": -70.533, "y": -33.379},
        },
        {
            "attributes": {
                "codigo_estacion": "05710002",
                "nombre_estacion": "Rio Biobio en Rucalhue",
                "rio": "Biobio",
                "area_drenaje": 12400.0,
                "altitud": 220.0,
            },
            "geometry": {"x": -71.747, "y": -37.618},
        },
    ],
}

MOCK_STATIONS_WITH_BAD_ENTRIES = {
    "features": [
        {
            "attributes": {
                "codigo_estacion": "04530001",
                "nombre_estacion": "Rio Mapocho en Los Almendros",
                "rio": "Mapocho",
            },
            "geometry": {"x": -70.533, "y": -33.379},
        },
        {
            "attributes": {
                "nombre_estacion": "No ID",
            },
            "geometry": {"x": -71.0, "y": -34.0},
        },
        {
            "attributes": {
                "codigo_estacion": "05710002",
                "nombre_estacion": "Rio Biobio en Rucalhue",
            },
            "geometry": {},
        },
        {
            "attributes": {
                "codigo_estacion": "06200003",
                "nombre_estacion": "Rio Laja",
                "rio": "Laja",
            },
            "geometry": {"x": -71.5, "y": -37.2},
        },
    ],
}

MOCK_STATIONS_EMPTY = {"features": []}

MOCK_ARCGIS_ERROR = {
    "error": {
        "code": 400,
        "message": "Unable to complete operation.",
    },
}


# ======================================================================
# Station tests
# ======================================================================


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_arcgis_json():
    """Station features are parsed from ArcGIS JSON response."""
    respx.get(f"{BASE_URL}{QUERY_PATH}").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_ARCGIS),
    )

    async with ChileDgaConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"04530001", "05710002"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fields_correct():
    """Station metadata is correctly extracted from ArcGIS attributes."""
    respx.get(f"{BASE_URL}{QUERY_PATH}").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_ARCGIS),
    )

    async with ChileDgaConnector() as conn:
        stations = await conn.fetch_stations()

    mapocho = next(
        s for s in stations if s.native_id == "04530001"
    )
    assert mapocho.id == "chile_dga:04530001"
    assert mapocho.provider == "chile_dga"
    assert mapocho.name == "Rio Mapocho en Los Almendros"
    assert mapocho.country_code == "CL"
    assert mapocho.river == "Mapocho"
    assert mapocho.latitude == pytest.approx(-33.379)
    assert mapocho.longitude == pytest.approx(-70.533)
    assert mapocho.catchment_area_km2 == pytest.approx(620.5)
    assert mapocho.elevation_m == pytest.approx(1020.0)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_invalid_entries():
    """Entries with missing ID or geometry are skipped."""
    respx.get(f"{BASE_URL}{QUERY_PATH}").mock(
        return_value=httpx.Response(
            200, json=MOCK_STATIONS_WITH_BAD_ENTRIES,
        ),
    )

    async with ChileDgaConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"04530001", "06200003"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty features list returns no stations."""
    respx.get(f"{BASE_URL}{QUERY_PATH}").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_EMPTY),
    )

    async with ChileDgaConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_arcgis_error():
    """An ArcGIS error response raises ConnectorError."""
    from csfs.core.exceptions import ConnectorError

    respx.get(f"{BASE_URL}{QUERY_PATH}").mock(
        return_value=httpx.Response(200, json=MOCK_ARCGIS_ERROR),
    )

    async with ChileDgaConnector() as conn:
        with pytest.raises(ConnectorError, match="ArcGIS error"):
            await conn.fetch_stations()


# ======================================================================
# Observation tests
# ======================================================================


@pytest.mark.asyncio
async def test_fetch_observations_returns_empty():
    """Observations always return an empty chunk (no confirmed endpoint)."""
    async with ChileDgaConnector() as conn:
        chunk = await conn.fetch_observations(
            "chile_dga:04530001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "chile_dga"
    assert chunk.station_id == "chile_dga:04530001"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_has_fetched_at():
    """Empty observations chunk still carries a fetched_at timestamp."""
    async with ChileDgaConnector() as conn:
        chunk = await conn.fetch_observations(
            "chile_dga:05710002",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 12, 31),
        )

    assert chunk.fetched_at is not None


# ======================================================================
# Coordinate extraction
# ======================================================================


@pytest.mark.asyncio
@respx.mock
async def test_stations_with_attribute_coords():
    """Stations with lat/lon in attributes instead of geometry."""
    data = {
        "features": [
            {
                "attributes": {
                    "codigo_estacion": "09999001",
                    "nombre_estacion": "Attr Coords Station",
                    "latitud": -35.5,
                    "longitud": -72.1,
                },
                "geometry": {},
            },
        ],
    }
    respx.get(f"{BASE_URL}{QUERY_PATH}").mock(
        return_value=httpx.Response(200, json=data),
    )

    async with ChileDgaConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].latitude == pytest.approx(-35.5)
    assert stations[0].longitude == pytest.approx(-72.1)


# ======================================================================
# Registry
# ======================================================================


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_pagination():
    """Connector paginates through all station pages when page is full."""
    # Create a full page of 1000 features (will cause offset increment)
    full_page = {
        "features": [
            {
                "attributes": {
                    "codigo_estacion": f"STA{i:04d}",
                    "nombre_estacion": f"Station {i}",
                },
                "geometry": {"x": -70.0 + i * 0.001, "y": -33.0},
            }
            for i in range(1000)
        ],
    }
    second_page = {
        "features": [
            {
                "attributes": {
                    "codigo_estacion": "LAST_STATION",
                    "nombre_estacion": "Last One",
                },
                "geometry": {"x": -71.0, "y": -34.0},
            },
        ],
    }

    route = respx.get(f"{BASE_URL}{QUERY_PATH}")
    route.side_effect = [
        httpx.Response(200, json=full_page),
        httpx.Response(200, json=second_page),
    ]

    async with ChileDgaConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1001
    native_ids = {s.native_id for s in stations}
    assert "LAST_STATION" in native_ids


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_pagination_cap_stops_runaway():
    """A server that always returns a full page must not loop forever.

    If the ArcGIS endpoint ignores ``resultOffset`` and keeps returning a
    full page, pagination is bounded by ``max_pages`` (50) rather than
    spinning until the runner is killed.
    """
    call_count = 0

    def always_full(request):
        nonlocal call_count
        # Unique codes per call so parsed stations are not deduplicated,
        # letting us assert the exact page cap was honoured.
        page = [
            {
                "attributes": {
                    "codigo_estacion": f"STA{call_count:02d}{i:04d}",
                    "nombre_estacion": f"Station {i}",
                },
                "geometry": {"x": -70.0 + i * 0.001, "y": -33.0},
            }
            for i in range(1000)
        ]
        call_count += 1
        return httpx.Response(200, json={"features": page})

    respx.get(f"{BASE_URL}{QUERY_PATH}").side_effect = always_full

    async with ChileDgaConnector() as conn:
        stations = await conn.fetch_stations()

    # Exactly max_pages (50) requests of 1000 features, then it gives up.
    assert call_count == 50
    assert len(stations) == 50_000


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_http_error_first_page_raises():
    """HTTPStatusError on the first page raises ConnectorError."""
    from csfs.core.exceptions import ConnectorError

    respx.get(f"{BASE_URL}{QUERY_PATH}").mock(
        return_value=httpx.Response(500),
    )

    async with ChileDgaConnector() as conn:
        with pytest.raises(ConnectorError, match="Failed to fetch"):
            await conn.fetch_stations()


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_http_error_second_page_returns_first():
    """HTTPStatusError on the second page still returns first-page results."""
    full_page = {
        "features": [
            {
                "attributes": {
                    "codigo_estacion": f"STA{i:04d}",
                    "nombre_estacion": f"Station {i}",
                },
                "geometry": {"x": -70.0, "y": -33.0},
            }
            for i in range(1000)
        ],
    }

    route = respx.get(f"{BASE_URL}{QUERY_PATH}")
    route.side_effect = [
        httpx.Response(200, json=full_page),
        httpx.Response(500),
    ]

    async with ChileDgaConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1000


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_arcgis_error_second_page_returns_first():
    """An ArcGIS error on the second page still returns first-page results."""
    full_page = {
        "features": [
            {
                "attributes": {
                    "codigo_estacion": f"STA{i:04d}",
                    "nombre_estacion": f"Station {i}",
                },
                "geometry": {"x": -70.0, "y": -33.0},
            }
            for i in range(1000)
        ],
    }

    route = respx.get(f"{BASE_URL}{QUERY_PATH}")
    route.side_effect = [
        httpx.Response(200, json=full_page),
        httpx.Response(200, json=MOCK_ARCGIS_ERROR),
    ]

    async with ChileDgaConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1000


@pytest.mark.asyncio
@respx.mock
async def test_extract_coords_invalid_geometry_values():
    """Invalid geometry values fall through to attribute coords."""
    data = {
        "features": [
            {
                "attributes": {
                    "codigo_estacion": "BAD_GEOM",
                    "nombre_estacion": "Bad Geom Station",
                    "latitud": -35.5,
                    "longitud": -72.1,
                },
                "geometry": {"x": "not_a_number", "y": "also_bad"},
            },
        ],
    }
    respx.get(f"{BASE_URL}{QUERY_PATH}").mock(
        return_value=httpx.Response(200, json=data),
    )

    async with ChileDgaConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].latitude == pytest.approx(-35.5)
    assert stations[0].longitude == pytest.approx(-72.1)


@pytest.mark.asyncio
@respx.mock
async def test_extract_coords_invalid_attr_coords_returns_none():
    """Invalid attribute coordinate values result in skipping the station."""
    data = {
        "features": [
            {
                "attributes": {
                    "codigo_estacion": "NO_COORDS",
                    "nombre_estacion": "No Coords",
                    "latitud": "bad",
                    "longitud": "bad",
                },
                "geometry": {},
            },
        ],
    }
    respx.get(f"{BASE_URL}{QUERY_PATH}").mock(
        return_value=httpx.Response(200, json=data),
    )

    async with ChileDgaConnector() as conn:
        stations = await conn.fetch_stations()

    # Station is skipped because coords are invalid
    assert len(stations) == 0


def test_connector_is_registered():
    """The connector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("chile_dga")
    assert cls is ChileDgaConnector
