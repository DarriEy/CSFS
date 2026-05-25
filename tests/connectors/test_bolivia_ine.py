"""Tests for the Bolivia INE connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.bolivia_ine import (
    _GRDC_BOLIVIAN_STATIONS,
    _SEED_STATIONS,
    BoliviaIneConnector,
)

BASE_URL = "https://anda.ine.gob.bo"

MOCK_CATALOG_VARIABLES = {
    "variables": [
        {
            "id": "3716400",
            "label": "Desaguadero",
            "latitude": -16.56,
            "longitude": -69.04,
            "river": "Desaguadero",
        },
        {
            "id": "BO-003",
            "label": "Rurrenabaque",
            "latitude": -14.44,
            "longitude": -67.53,
            "river": "Beni",
        },
        {
            "id": "",
            "label": "Missing ID",
            "latitude": -15.0,
            "longitude": -65.0,
        },
    ],
}

MOCK_LONG_CSV = """station,date,caudal
BO-001,2024-06-01,150.3
BO-001,2024-06-02,148.7
BO-003,2024-06-01,200.5
BO-001,2024-06-03,
"""

MOCK_WIDE_CSV = """date,BO-001,BO-003
2024-06-01,150.3,200.5
2024-06-02,148.7,195.0
2024-06-03,,210.0
"""


@pytest.mark.asyncio
async def test_fetch_stations_returns_seed_list():
    """Seed stations are returned when API is unreachable."""
    async with BoliviaIneConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    native_ids = {s.native_id for s in stations}
    assert "3716400" in native_ids
    assert "3717600" in native_ids


@pytest.mark.asyncio
async def test_fetch_stations_seed_fields():
    """Seed stations have correct metadata."""
    async with BoliviaIneConnector() as conn:
        stations = await conn.fetch_stations()

    abapo = next(
        s for s in stations if s.native_id == "3717600"
    )
    assert abapo.id == "bolivia_ine:3717600"
    assert abapo.provider == "bolivia_ine"
    assert abapo.name == "Abapo"
    assert abapo.country_code == "BO"
    assert abapo.river == "Rio Grande"
    assert abapo.latitude == pytest.approx(-18.85)
    assert abapo.catchment_area_km2 is None


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_from_catalog_api():
    """Station list is parsed from NADA catalog when available."""
    respx.get(f"{BASE_URL}/index.php/catalog/209").mock(
        return_value=httpx.Response(
            200, json=MOCK_CATALOG_VARIABLES,
        ),
    )

    async with BoliviaIneConnector() as conn:
        stations = await conn.fetch_stations()

    # Only 2 valid entries (empty ID is skipped)
    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"3716400", "BO-003"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_long_format_csv():
    """Long-format CSV observations are parsed correctly."""
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/BO-001").mock(
        return_value=httpx.Response(
            200,
            text=MOCK_LONG_CSV,
            headers={"content-type": "text/csv"},
        ),
    )

    async with BoliviaIneConnector() as conn:
        chunk = await conn.fetch_observations(
            "bolivia_ine:BO-001",
            start=datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 3, 23, 59, 59, tzinfo=UTC),
        )

    assert chunk.provider == "bolivia_ine"
    assert chunk.station_id == "bolivia_ine:BO-001"
    # 3 rows for BO-001 (2024-06-01, 02, 03)
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(150.3)
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_returns_empty_on_html():
    """HTML responses (auth needed) are skipped gracefully."""
    html_response = "<html><body>Login required</body></html>"
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/BO-001").mock(
        return_value=httpx.Response(
            200,
            text=html_response,
            headers={"content-type": "text/html"},
        ),
    )
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/data").mock(
        return_value=httpx.Response(
            200,
            text=html_response,
            headers={"content-type": "text/html"},
        ),
    )
    respx.get(
        f"{BASE_URL}/index.php/catalog/209/download/caudales"
    ).mock(
        return_value=httpx.Response(
            200,
            text=html_response,
            headers={"content-type": "text/html"},
        ),
    )

    async with BoliviaIneConnector() as conn:
        chunk = await conn.fetch_observations(
            "bolivia_ine:BO-001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0
    assert chunk.provider == "bolivia_ine"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_returns_empty_on_failure():
    """Server errors return an empty chunk."""
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/BO-008").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{BASE_URL}/index.php/catalog/209/download/data").mock(
        return_value=httpx.Response(500),
    )
    respx.get(
        f"{BASE_URL}/index.php/catalog/209/download/caudales"
    ).mock(
        return_value=httpx.Response(500),
    )

    async with BoliviaIneConnector() as conn:
        chunk = await conn.fetch_observations(
            "bolivia_ine:BO-008",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


def test_grdc_cross_reference():
    """GRDC station cross-reference list is available."""
    grdc = BoliviaIneConnector.grdc_station_ids()
    assert len(grdc) == len(_GRDC_BOLIVIAN_STATIONS)
    ids = {g[0] for g in grdc}
    assert "3716400" in ids  # Angosto del Bala


def test_connector_is_registered():
    """The connector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("bolivia_ine")
    assert cls is BoliviaIneConnector
