"""Tests for the Kazakhstan Kazhydromet connector."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.kazakhstan_kazhydromet import (
    KazakhstanKazhydrometConnector,
)
from csfs.core.exceptions import DataFormatError


@pytest.mark.asyncio
async def test_fetch_stations_returns_seed_list():
    """Seed list is always returned (portal is unreliable)."""
    async with KazakhstanKazhydrometConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 5
    rivers = {s.river for s in stations}
    assert "Irtysh" in rivers
    assert "Syr Darya" in rivers
    assert "Ural" in rivers


@pytest.mark.asyncio
async def test_seed_station_fields():
    """Seed stations have correct field values."""
    async with KazakhstanKazhydrometConnector() as conn:
        stations = await conn.fetch_stations()

    st = stations[0]
    assert st.id.startswith("kazakhstan_kazhydromet:")
    assert st.provider == "kazakhstan_kazhydromet"
    assert st.country_code == "KZ"
    assert st.latitude != 0
    assert st.longitude != 0
    assert st.river is not None


@pytest.mark.asyncio
async def test_fetch_observations_returns_empty():
    """Observations return empty (portal unreliable, no live fetch)."""
    async with KazakhstanKazhydrometConnector() as conn:
        chunk = await conn.fetch_observations(
            "kazakhstan_kazhydromet:KZ-001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "kazakhstan_kazhydromet"
    assert chunk.station_id == "kazakhstan_kazhydromet:KZ-001"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_connector_registration():
    """Connector is discoverable via the registry."""
    from csfs.core.registry import discover, get_connector

    discover()
    cls = get_connector("kazakhstan_kazhydromet")
    assert cls is KazakhstanKazhydrometConnector


@pytest.mark.asyncio
async def test_fetch_latest():
    async with KazakhstanKazhydrometConnector() as conn:
        chunk = await conn.fetch_latest("kazakhstan_kazhydromet:2311104")

    assert chunk.provider == "kazakhstan_kazhydromet"
    assert len(chunk.observations) == 0


def test_parse_stations_bare_list():
    """Parser handles a bare list of station dicts."""
    conn = KazakhstanKazhydrometConnector()
    data = [
        {"station_id": "100", "name": "Test", "latitude": 50.0, "longitude": 70.0},
        {"station_id": "101", "name": "Test2", "lat": 51.0, "lon": 71.0},
    ]
    stations = conn._parse_stations(data)
    assert len(stations) == 2
    assert stations[0].native_id == "100"
    assert stations[1].latitude == pytest.approx(51.0)


def test_parse_stations_wrapped_dict():
    """Parser handles wrapped response with 'stations' key."""
    conn = KazakhstanKazhydrometConnector()
    data = {
        "stations": [
            {"id": "200", "name": "Wrapped", "latitude": 48.0, "longitude": 68.0},
        ]
    }
    stations = conn._parse_stations(data)
    assert len(stations) == 1
    assert stations[0].native_id == "200"


def test_parse_stations_skips_missing_coords():
    """Stations without coordinates are skipped."""
    conn = KazakhstanKazhydrometConnector()
    data = [
        {"station_id": "100", "name": "No Coords"},
        {"station_id": "101", "name": "Has Coords", "latitude": 50.0, "longitude": 70.0},
    ]
    stations = conn._parse_stations(data)
    assert len(stations) == 1


def test_parse_stations_skips_empty_id():
    conn = KazakhstanKazhydrometConnector()
    data = [
        {"station_id": "", "name": "Empty ID", "latitude": 50.0, "longitude": 70.0},
        {"station_id": "101", "name": "OK", "latitude": 50.0, "longitude": 70.0},
    ]
    stations = conn._parse_stations(data)
    assert len(stations) == 1


def test_parse_stations_optional_fields():
    """catchment_area_km2 and elevation_m are parsed when present."""
    conn = KazakhstanKazhydrometConnector()
    data = [
        {
            "station_id": "100",
            "name": "Full",
            "latitude": 50.0,
            "longitude": 70.0,
            "river": "Irtysh",
            "catchment_area_km2": 1500.0,
            "elevation_m": 350.0,
        },
    ]
    stations = conn._parse_stations(data)
    assert stations[0].catchment_area_km2 == pytest.approx(1500.0)
    assert stations[0].elevation_m == pytest.approx(350.0)
    assert stations[0].river == "Irtysh"


def test_parse_observations_bare_list():
    conn = KazakhstanKazhydrometConnector()
    data = [
        {"timestamp": "2024-06-01T00:00:00", "discharge": 120.5, "quality": "good"},
        {"datetime": "2024-06-02T00:00:00", "discharge_m3s": 115.0},
    ]
    chunk = conn._parse_observations(data, "kz:100")
    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(120.5)
    assert chunk.observations[0].quality.value == "good"
    assert chunk.observations[1].discharge_m3s == pytest.approx(115.0)
    assert chunk.observations[1].quality.value == "raw"


def test_parse_observations_wrapped_dict():
    conn = KazakhstanKazhydrometConnector()
    data = {
        "data": [
            {"timestamp": "2024-06-01T00:00:00", "value": 100.0, "quality": "estimated"},
        ]
    }
    chunk = conn._parse_observations(data, "kz:100")
    assert len(chunk.observations) == 1
    assert chunk.observations[0].quality.value == "estimated"


def test_parse_observations_null_discharge():
    conn = KazakhstanKazhydrometConnector()
    data = [{"timestamp": "2024-06-01T00:00:00", "discharge": None}]
    chunk = conn._parse_observations(data, "kz:100")
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[0].quality.value == "missing"


def test_parse_observations_suspect_quality():
    conn = KazakhstanKazhydrometConnector()
    data = [{"timestamp": "2024-06-01T00:00:00", "discharge": 50.0, "quality": "suspect"}]
    chunk = conn._parse_observations(data, "kz:100")
    assert chunk.observations[0].quality.value == "suspect"


def test_parse_observations_skips_no_timestamp():
    conn = KazakhstanKazhydrometConnector()
    data = [
        {"discharge": 100.0},
        {"timestamp": "2024-06-01T00:00:00", "discharge": 50.0},
    ]
    chunk = conn._parse_observations(data, "kz:100")
    assert len(chunk.observations) == 1


def test_parse_observations_unexpected_type_raises():
    conn = KazakhstanKazhydrometConnector()
    with pytest.raises(DataFormatError, match="Unexpected response type"):
        conn._parse_observations("not a dict or list", "kz:100")


@pytest.mark.asyncio
@respx.mock
async def test_try_live_stations_all_fail():
    """When all API paths fail, returns empty list."""
    respx.get(url__startswith="https://meteo.kazhydromet.kz/").mock(
        return_value=httpx.Response(500)
    )

    async with KazakhstanKazhydrometConnector() as conn:
        result = await conn._try_live_stations()

    assert result == []


@pytest.mark.asyncio
@respx.mock
async def test_try_live_stations_success():
    """Live station list parsed when API responds."""
    respx.get("https://meteo.kazhydromet.kz/database_hydro/stations").mock(
        return_value=httpx.Response(200, json=[
            {"station_id": "999", "name": "Live", "latitude": 50.0, "longitude": 70.0},
        ])
    )

    async with KazakhstanKazhydrometConnector() as conn:
        result = await conn._try_live_stations()

    assert len(result) == 1
    assert result[0].native_id == "999"


@pytest.mark.asyncio
@respx.mock
async def test_try_live_observations_all_fail():
    respx.get(url__startswith="https://meteo.kazhydromet.kz/").mock(
        return_value=httpx.Response(500)
    )

    async with KazakhstanKazhydrometConnector() as conn:
        result = await conn._try_live_observations(
            "100", "kz:100", datetime(2024, 6, 1), datetime(2024, 6, 2),
        )

    assert result is None


@pytest.mark.asyncio
@respx.mock
async def test_try_live_observations_success():
    respx.get("https://meteo.kazhydromet.kz/database_hydro/data").mock(
        return_value=httpx.Response(200, json={
            "data": [
                {"timestamp": "2024-06-01T00:00:00", "discharge": 80.0},
            ]
        })
    )

    async with KazakhstanKazhydrometConnector() as conn:
        result = await conn._try_live_observations(
            "100", "kz:100", datetime(2024, 6, 1), datetime(2024, 6, 2),
        )

    assert result is not None
    assert len(result.observations) == 1
