"""Tests for the BAFU Hydrodaten (Switzerland) connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.switzerland_bafu import SwitzerlandBafuConnector
from csfs.core.exceptions import ConnectorError, DataFormatError

# ISO timestamps for test data
_TS1_ISO = "2024-06-01T12:00:00+00:00"
_TS2_ISO = "2024-06-01T12:10:00+00:00"
_TS3_ISO = "2024-06-01T12:20:00+00:00"

EXISTENZ_BASE = "https://api.existenz.ch"

MOCK_STATIONS_RESPONSE = {
    "source": "Swiss FOEN/BAFU",
    "payload": [
        {
            "timestamp": _TS1_ISO,
            "loc": "2009",
            "par": "flow",
            "val": 523.4,
            "name": "Rhein - Basel, Rheinhalle",
            "river": "Rhein",
            "lat": 47.559,
            "lon": 7.613,
        },
        {
            "timestamp": _TS1_ISO,
            "loc": "2091",
            "par": "flow",
            "val": 100.0,
            "name": "Limmat - Zürich, Unterhard",
            "river": "Limmat",
            "lat": 47.397,
            "lon": 8.498,
        },
    ],
}

MOCK_OBSERVATIONS_RESPONSE = {
    "source": "Swiss FOEN/BAFU",
    "payload": [
        {"timestamp": _TS1_ISO, "loc": "2009", "par": "flow", "val": 523.4},
        {"timestamp": _TS2_ISO, "loc": "2009", "par": "flow", "val": 530.1},
        {"timestamp": _TS3_ISO, "loc": "2009", "par": "flow", "val": None},
    ],
}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_payload():
    """Stations are extracted from the api.existenz.ch payload."""
    respx.get(f"{EXISTENZ_BASE}/apiv1/hydro/latest").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with SwitzerlandBafuConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    native_ids = {s.native_id for s in stations}
    assert native_ids == {"2009", "2091"}

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
    """An empty payload returns no stations."""
    respx.get(f"{EXISTENZ_BASE}/apiv1/hydro/latest").mock(
        return_value=httpx.Response(200, json={"source": "Swiss FOEN/BAFU", "payload": []})
    )

    async with SwitzerlandBafuConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_unexpected_format():
    """A non-dict top-level response returns no stations with a warning."""
    respx.get(f"{EXISTENZ_BASE}/apiv1/hydro/latest").mock(
        return_value=httpx.Response(200, json=[])
    )

    async with SwitzerlandBafuConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_deduplicates_locations():
    """Multiple payload entries for the same loc produce only one station."""
    data = {
        "source": "Swiss FOEN/BAFU",
        "payload": [
            {"timestamp": _TS1_ISO, "loc": "2009", "par": "flow", "val": 100.0},
            {"timestamp": _TS2_ISO, "loc": "2009", "par": "flow", "val": 200.0},
        ],
    }
    respx.get(f"{EXISTENZ_BASE}/apiv1/hydro/latest").mock(
        return_value=httpx.Response(200, json=data)
    )

    async with SwitzerlandBafuConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "2009"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_values():
    """Observations are parsed from the api.existenz.ch payload."""
    respx.get(f"{EXISTENZ_BASE}/apiv1/hydro/latest").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE)
    )

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
async def test_fetch_observations_filters_by_date_range():
    """Only observations within the requested date range are returned."""
    respx.get(f"{EXISTENZ_BASE}/apiv1/hydro/latest").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE)
    )

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
async def test_fetch_observations_handles_empty_payload():
    """An empty payload returns zero observations."""
    respx.get(f"{EXISTENZ_BASE}/apiv1/hydro/latest").mock(
        return_value=httpx.Response(200, json={"source": "Swiss FOEN/BAFU", "payload": []})
    )

    async with SwitzerlandBafuConnector() as conn:
        chunk = await conn.fetch_observations(
            "switzerland_bafu:2009",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_numeric_timestamp():
    """The connector handles numeric (epoch) timestamps in the payload."""
    epoch_ts = datetime(2024, 6, 1, 12, 0, 0, tzinfo=UTC).timestamp()
    data = {
        "source": "Swiss FOEN/BAFU",
        "payload": [
            {"timestamp": epoch_ts, "loc": "2009", "par": "flow", "val": 42.5},
        ],
    }
    respx.get(f"{EXISTENZ_BASE}/apiv1/hydro/latest").mock(
        return_value=httpx.Response(200, json=data)
    )

    async with SwitzerlandBafuConnector() as conn:
        chunk = await conn.fetch_observations(
            "switzerland_bafu:2009",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(42.5)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_server_error_raises():
    """A server error on the station listing raises ConnectorError."""
    respx.get(f"{EXISTENZ_BASE}/apiv1/hydro/latest").mock(
        return_value=httpx.Response(500)
    )

    async with SwitzerlandBafuConnector() as conn:
        with pytest.raises(ConnectorError):
            await conn.fetch_stations()


@pytest.mark.asyncio
@respx.mock
async def test_station_id_prefix_is_stripped():
    """The slug prefix is correctly stripped from station_id."""
    data = {
        "source": "Swiss FOEN/BAFU",
        "payload": [
            {"timestamp": _TS1_ISO, "loc": "2009", "par": "flow", "val": 10.0},
        ],
    }
    respx.get(f"{EXISTENZ_BASE}/apiv1/hydro/latest").mock(
        return_value=httpx.Response(200, json=data)
    )

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
async def test_fetch_observations_non_dict_response_raises():
    """A non-dict response raises DataFormatError."""
    respx.get(f"{EXISTENZ_BASE}/apiv1/hydro/latest").mock(
        return_value=httpx.Response(200, json=[1, 2, 3])
    )

    async with SwitzerlandBafuConnector() as conn:
        with pytest.raises(DataFormatError, match="Unexpected response type"):
            await conn.fetch_observations(
                "switzerland_bafu:2009",
                start=datetime(2024, 6, 1, tzinfo=UTC),
                end=datetime(2024, 6, 2, tzinfo=UTC),
            )


@pytest.mark.asyncio
@respx.mock
async def test_registration():
    """The connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("switzerland_bafu")
    assert cls is SwitzerlandBafuConnector
