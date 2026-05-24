"""Tests for the CHMU (Czechia) connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.czechia_chmu import CzechiaChmuConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import QualityFlag

# -- Mock response data ------------------------------------------------

MOCK_STATIONS_RESPONSE = [
    {
        "DBCN": "PBK0001",
        "NAZEV": "Praha - Branik",
        "ZEMEPISNASIRKA": 50.041,
        "ZEMEPISNADELKA": 14.413,
        "TOK": "Vltava",
    },
    {
        "DBCN": "PBK0002",
        "NAZEV": "Brno - Ponavka",
        "ZEMEPISNASIRKA": 49.195,
        "ZEMEPISNADELKA": 16.608,
        "TOK": "Svratka",
    },
]

MOCK_OBSERVATIONS_RESPONSE = [
    {
        "DTM": "2024-06-01T12:00:00",
        "PRUTOK": 123.4,
    },
    {
        "DTM": "2024-06-01T13:00:00",
        "PRUTOK": 125.0,
    },
    {
        "DTM": "2024-06-01T14:00:00",
        "PRUTOK": None,
    },
]

# Observations outside the typical query range
MOCK_OBSERVATIONS_WIDE_RANGE = [
    {
        "DTM": "2024-05-31T23:00:00",
        "PRUTOK": 100.0,
    },
    {
        "DTM": "2024-06-01T12:00:00",
        "PRUTOK": 123.4,
    },
    {
        "DTM": "2024-06-03T01:00:00",
        "PRUTOK": 130.0,
    },
]

BASE = "https://hydro.chmi.cz/hppsoldv"


# -- Tests: fetch_stations ---------------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_response():
    """Station list is correctly parsed from the CHMU JSON response."""
    respx.get(f"{BASE}/hpps_act_rain.php").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE),
    )

    async with CzechiaChmuConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2

    praha = next(s for s in stations if s.native_id == "PBK0001")
    assert praha.id == "czechia_chmu:PBK0001"
    assert praha.provider == "czechia_chmu"
    assert praha.name == "Praha - Branik"
    assert praha.latitude == pytest.approx(50.041)
    assert praha.longitude == pytest.approx(14.413)
    assert praha.country_code == "CZ"
    assert praha.river == "Vltava"

    brno = next(s for s in stations if s.native_id == "PBK0002")
    assert brno.name == "Brno - Ponavka"
    assert brno.river == "Svratka"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty station list returns no stations."""
    respx.get(f"{BASE}/hpps_act_rain.php").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with CzechiaChmuConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_dict_wrapper():
    """Response wrapped in a dict with 'stations' key is handled."""
    respx.get(f"{BASE}/hpps_act_rain.php").mock(
        return_value=httpx.Response(
            200,
            json={"stations": MOCK_STATIONS_RESPONSE},
        ),
    )

    async with CzechiaChmuConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_malformed_entries():
    """Entries without DBCN are skipped."""
    response = [
        {
            "DBCN": "PBK0001",
            "NAZEV": "Praha - Branik",
            "ZEMEPISNASIRKA": 50.041,
            "ZEMEPISNADELKA": 14.413,
            "TOK": "Vltava",
        },
        {
            # Missing DBCN
            "NAZEV": "Bad Station",
            "ZEMEPISNASIRKA": 49.0,
            "ZEMEPISNADELKA": 15.0,
        },
    ]
    respx.get(f"{BASE}/hpps_act_rain.php").mock(
        return_value=httpx.Response(200, json=response),
    )

    async with CzechiaChmuConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "PBK0001"


# -- Tests: fetch_observations ----------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_json():
    """Observations are correctly parsed into a TimeSeriesChunk."""
    respx.get(f"{BASE}/hpps_prutoky.php").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE),
    )

    async with CzechiaChmuConnector() as conn:
        chunk = await conn.fetch_observations(
            "czechia_chmu:PBK0001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "czechia_chmu"
    assert chunk.station_id == "czechia_chmu:PBK0001"
    assert len(chunk.observations) == 3

    # First observation
    assert chunk.observations[0].discharge_m3s == pytest.approx(123.4)
    assert chunk.observations[0].quality == QualityFlag.RAW

    # Third observation — None value should yield MISSING
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality == QualityFlag.MISSING


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty():
    """An empty observations array returns zero observations."""
    respx.get(f"{BASE}/hpps_prutoky.php").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with CzechiaChmuConnector() as conn:
        chunk = await conn.fetch_observations(
            "czechia_chmu:PBK0001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_filters_by_time_range():
    """Observations outside the requested range are filtered out."""
    respx.get(f"{BASE}/hpps_prutoky.php").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_WIDE_RANGE),
    )

    async with CzechiaChmuConnector() as conn:
        chunk = await conn.fetch_observations(
            "czechia_chmu:PBK0001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    # Only the middle observation falls in range
    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(123.4)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_dict_wrapper():
    """Response wrapped in a dict with 'data' key is handled."""
    respx.get(f"{BASE}/hpps_prutoky.php").mock(
        return_value=httpx.Response(
            200,
            json={"data": MOCK_OBSERVATIONS_RESPONSE},
        ),
    )

    async with CzechiaChmuConnector() as conn:
        chunk = await conn.fetch_observations(
            "czechia_chmu:PBK0001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 3


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_invalid_timestamp_raises():
    """Invalid timestamp in observation data raises DataFormatError."""
    bad_response = [
        {"DTM": "NOT-A-TIMESTAMP", "PRUTOK": 100.0},
    ]
    respx.get(f"{BASE}/hpps_prutoky.php").mock(
        return_value=httpx.Response(200, json=bad_response),
    )

    async with CzechiaChmuConnector() as conn:
        with pytest.raises(DataFormatError, match="Invalid timestamp"):
            await conn.fetch_observations(
                "czechia_chmu:PBK0001",
                start=datetime(2024, 6, 1),
                end=datetime(2024, 6, 2),
            )


# -- Tests: registration -----------------------------------------------

def test_connector_is_registered():
    """The connector is registered with the expected slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("czechia_chmu")
    assert cls is CzechiaChmuConnector


def test_connector_class_attributes():
    """Class-level attributes match expectations."""
    assert CzechiaChmuConnector.slug == "czechia_chmu"
    assert CzechiaChmuConnector.country_codes == ["CZ"]
    assert "hydro.chmi.cz" in CzechiaChmuConnector.base_url
