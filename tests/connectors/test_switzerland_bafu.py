"""Tests for the BAFU Hydrodaten (Switzerland) connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.switzerland_bafu import SwitzerlandBafuConnector
from csfs.core.exceptions import ConnectorError

# Epoch timestamps in milliseconds for test data
_TS1_MS = int(datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC).timestamp() * 1000)
_TS2_MS = int(datetime(2024, 6, 1, 12, 10, 0, tzinfo=UTC).timestamp() * 1000)
_TS3_MS = int(datetime(2024, 6, 1, 12, 20, 0, tzinfo=UTC).timestamp() * 1000)

MOCK_STATIONS_RESPONSE = {
    "2009": {
        "Name": "Rhein - Basel, Rheinhalle",
        "GewässerName": "Rhein",
        "Kanton": "BS",
        "Koordinaten": {"lat": 47.559, "lng": 7.613},
        "parameters": ["Abfluss", "Pegel"],
    },
    "2033": {
        "Name": "Aare - Bern, Schönau",
        "GewässerName": "Aare",
        "Kanton": "BE",
        "Koordinaten": {"lat": 46.935, "lng": 7.451},
        "parameters": ["Pegel"],
    },
    "2091": {
        "Name": "Limmat - Zürich, Unterhard",
        "GewässerName": "Limmat",
        "Kanton": "ZH",
        "Koordinaten": {"lat": 47.397, "lng": 8.498},
        "parameters": ["Abfluss", "Wassertemperatur"],
    },
}

MOCK_TIMESERIES_RESPONSE = {
    "data": [
        [_TS1_MS, 523.4],
        [_TS2_MS, 530.1],
        [_TS3_MS, None],
    ],
}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_filters_discharge():
    """Only stations with 'Abfluss' in their parameters are returned."""
    respx.get(
        "https://www.hydrodaten.admin.ch/graphs/messstationen_uebersicht.json"
    ).mock(return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE))

    async with SwitzerlandBafuConnector() as conn:
        stations = await conn.fetch_stations()

    # Station 2033 has only Pegel, so it should be excluded
    assert len(stations) == 2
    native_ids = {s.native_id for s in stations}
    assert native_ids == {"2009", "2091"}

    # Check fields on first station
    station_rhein = next(s for s in stations if s.native_id == "2009")
    assert station_rhein.id == "switzerland_bafu:2009"
    assert station_rhein.provider == "switzerland_bafu"
    assert station_rhein.country_code == "CH"
    assert station_rhein.river == "Rhein"
    assert station_rhein.latitude == 47.559
    assert station_rhein.longitude == 7.613


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty station dict returns no stations."""
    respx.get(
        "https://www.hydrodaten.admin.ch/graphs/messstationen_uebersicht.json"
    ).mock(return_value=httpx.Response(200, json={}))

    async with SwitzerlandBafuConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_unexpected_format():
    """A non-dict top-level response returns no stations with a warning."""
    respx.get(
        "https://www.hydrodaten.admin.ch/graphs/messstationen_uebersicht.json"
    ).mock(return_value=httpx.Response(200, json=[]))

    async with SwitzerlandBafuConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_malformed_entries():
    """Entries with missing coordinates or bad data are skipped."""
    data = {
        "9999": {
            "Name": "Broken Station",
            # Missing GewässerName, Koordinaten
            "parameters": ["Abfluss"],
        },
        "2009": {
            "Name": "Good Station",
            "GewässerName": "Rhein",
            "Koordinaten": {"lat": 47.0, "lng": 8.0},
            "parameters": ["Abfluss"],
        },
    }
    respx.get(
        "https://www.hydrodaten.admin.ch/graphs/messstationen_uebersicht.json"
    ).mock(return_value=httpx.Response(200, json=data))

    async with SwitzerlandBafuConnector() as conn:
        stations = await conn.fetch_stations()

    # Both should parse (missing coords default to 0.0, missing river is None)
    assert len(stations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_first_pattern_succeeds():
    """Observations are parsed when the first endpoint pattern responds."""
    respx.get(
        "https://www.hydrodaten.admin.ch/graphs/messwerte/2009_Abfluss_m3s_10min.json"
    ).mock(return_value=httpx.Response(200, json=MOCK_TIMESERIES_RESPONSE))

    async with SwitzerlandBafuConnector() as conn:
        chunk = await conn.fetch_observations(
            "switzerland_bafu:2009",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert chunk.provider == "switzerland_bafu"
    assert chunk.station_id == "switzerland_bafu:2009"
    assert len(chunk.observations) == 3

    # First observation
    assert chunk.observations[0].discharge_m3s == pytest.approx(523.4)
    assert chunk.observations[0].quality.value == "raw"
    assert chunk.observations[0].timestamp == datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC)

    # Third observation — None value should yield MISSING
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_falls_back_to_second_pattern():
    """When the first endpoint pattern fails, the second is tried."""
    respx.get(
        "https://www.hydrodaten.admin.ch/graphs/messwerte/2009_Abfluss_m3s_10min.json"
    ).mock(return_value=httpx.Response(404))

    respx.get(
        "https://www.hydrodaten.admin.ch/graphs/messwerte/lhg_2009_AbflussPegel_10min.json"
    ).mock(return_value=httpx.Response(200, json=MOCK_TIMESERIES_RESPONSE))

    async with SwitzerlandBafuConnector() as conn:
        chunk = await conn.fetch_observations(
            "switzerland_bafu:2009",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 3


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_all_patterns_fail():
    """When all endpoint patterns fail, an empty chunk is returned."""
    respx.get(
        "https://www.hydrodaten.admin.ch/graphs/messwerte/2009_Abfluss_m3s_10min.json"
    ).mock(return_value=httpx.Response(404))

    respx.get(
        "https://www.hydrodaten.admin.ch/graphs/messwerte/lhg_2009_AbflussPegel_10min.json"
    ).mock(return_value=httpx.Response(404))

    async with SwitzerlandBafuConnector() as conn:
        chunk = await conn.fetch_observations(
            "switzerland_bafu:2009",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_filters_by_date_range():
    """Only observations within the requested date range are returned."""
    respx.get(
        "https://www.hydrodaten.admin.ch/graphs/messwerte/2009_Abfluss_m3s_10min.json"
    ).mock(return_value=httpx.Response(200, json=MOCK_TIMESERIES_RESPONSE))

    async with SwitzerlandBafuConnector() as conn:
        # Request a narrow window that only includes the first timestamp
        chunk = await conn.fetch_observations(
            "switzerland_bafu:2009",
            start=datetime(2024, 6, 1, 11, 59, 0, tzinfo=UTC),
            end=datetime(2024, 6, 1, 12, 5, 0, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(523.4)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty_data():
    """An empty data array returns zero observations."""
    respx.get(
        "https://www.hydrodaten.admin.ch/graphs/messwerte/2009_Abfluss_m3s_10min.json"
    ).mock(return_value=httpx.Response(200, json={"data": []}))

    async with SwitzerlandBafuConnector() as conn:
        chunk = await conn.fetch_observations(
            "switzerland_bafu:2009",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_bare_array_format():
    """The connector handles a bare JSON array (no wrapping object)."""
    bare_array = [
        [_TS1_MS, 100.0],
        [_TS2_MS, 200.0],
    ]
    respx.get(
        "https://www.hydrodaten.admin.ch/graphs/messwerte/2009_Abfluss_m3s_10min.json"
    ).mock(return_value=httpx.Response(200, json=bare_array))

    async with SwitzerlandBafuConnector() as conn:
        chunk = await conn.fetch_observations(
            "switzerland_bafu:2009",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(100.0)
    assert chunk.observations[1].discharge_m3s == pytest.approx(200.0)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_dict_record_format():
    """The connector handles dict-style records with 'timestamp' and 'value' keys."""
    dict_records = {
        "measurements": [
            {"timestamp": "2024-06-01T12:00:00+00:00", "value": 42.5},
            {"timestamp": "2024-06-01T12:10:00+00:00", "value": 43.0},
        ],
    }
    respx.get(
        "https://www.hydrodaten.admin.ch/graphs/messwerte/2009_Abfluss_m3s_10min.json"
    ).mock(return_value=httpx.Response(200, json=dict_records))

    async with SwitzerlandBafuConnector() as conn:
        chunk = await conn.fetch_observations(
            "switzerland_bafu:2009",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(42.5)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_server_error_raises():
    """A server error on the station listing raises ConnectorError."""
    respx.get(
        "https://www.hydrodaten.admin.ch/graphs/messstationen_uebersicht.json"
    ).mock(return_value=httpx.Response(500))

    async with SwitzerlandBafuConnector() as conn:
        with pytest.raises(ConnectorError):
            await conn.fetch_stations()


@pytest.mark.asyncio
@respx.mock
async def test_station_id_prefix_is_stripped():
    """The slug prefix is correctly stripped from station_id."""
    respx.get(
        "https://www.hydrodaten.admin.ch/graphs/messwerte/2009_Abfluss_m3s_10min.json"
    ).mock(return_value=httpx.Response(200, json={"data": [[_TS1_MS, 10.0]]}))

    async with SwitzerlandBafuConnector() as conn:
        chunk = await conn.fetch_observations(
            "switzerland_bafu:2009",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert chunk.station_id == "switzerland_bafu:2009"
    assert len(chunk.observations) == 1


@pytest.mark.asyncio
@respx.mock
async def test_registration():
    """The connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("switzerland_bafu")
    assert cls is SwitzerlandBafuConnector
