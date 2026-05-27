"""Tests for the Iceland LamaH-Ice connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.iceland_lamahice import IcelandLamahIceConnector

MOCK_LIVE_STATIONS = [
    {"id": "LIVE01", "name": "Skogafoss", "lat": 63.53, "lon": -19.51},
]

MOCK_OBSERVATIONS_JSON = {
    "data": [
        {"time": "2024-06-01T00:00:00", "discharge": 150.5},
        {"time": "2024-06-01T01:00:00", "discharge": 152.3},
        {"time": "2024-06-01T02:00:00", "discharge": None},
    ]
}


@respx.mock
async def test_fetch_stations_returns_seed_list():
    """Always returns the seed station list."""
    respx.get("https://api.vedur.is/hydro/stations.json").mock(
        return_value=httpx.Response(500)
    )

    async with IcelandLamahIceConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 30
    s = next(s for s in stations if s.native_id == "VHM001")
    assert s.id == "iceland_lamahice:VHM001"
    assert s.name == "Selfoss"
    assert s.river == "Olfusa"
    assert s.country_code == "IS"


@respx.mock
async def test_fetch_stations_augments_from_live():
    """Live stations are merged with the seed list."""
    respx.get("https://api.vedur.is/hydro/stations.json").mock(
        return_value=httpx.Response(200, json=MOCK_LIVE_STATIONS)
    )

    async with IcelandLamahIceConnector() as conn:
        stations = await conn.fetch_stations()

    # 30 seed + 1 live
    assert len(stations) == 31
    assert any(s.native_id == "LIVE01" for s in stations)


@respx.mock
async def test_fetch_observations():
    """Observations are parsed from Vedur.is API."""
    respx.get("https://api.vedur.is/hydro/latest.json").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_JSON)
    )

    async with IcelandLamahIceConnector() as conn:
        chunk = await conn.fetch_observations(
            "iceland_lamahice:VHM001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert chunk.provider == "iceland_lamahice"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(150.5)
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@respx.mock
async def test_fetch_observations_failure():
    """Returns empty chunk on API failure."""
    respx.get("https://api.vedur.is/hydro/latest.json").mock(
        return_value=httpx.Response(500)
    )

    async with IcelandLamahIceConnector() as conn:
        chunk = await conn.fetch_observations(
            "iceland_lamahice:VHM001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert chunk.observations == []


@respx.mock
async def test_fetch_observations_empty_data():
    """Empty data array returns zero observations."""
    respx.get("https://api.vedur.is/hydro/latest.json").mock(
        return_value=httpx.Response(200, json={"data": []})
    )

    async with IcelandLamahIceConnector() as conn:
        chunk = await conn.fetch_observations(
            "iceland_lamahice:VHM001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@respx.mock
async def test_connector_registration():
    """Connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("iceland_lamahice")
    assert cls is IcelandLamahIceConnector


# =====================================================================
# Additional coverage tests
# =====================================================================


@respx.mock
async def test_fetch_latest():
    """fetch_latest fetches the most recent 24 hours."""
    respx.get("https://api.vedur.is/hydro/latest.json").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_JSON)
    )

    async with IcelandLamahIceConnector() as conn:
        chunk = await conn.fetch_latest("iceland_lamahice:VHM001")

    assert chunk.station_id == "iceland_lamahice:VHM001"
    assert len(chunk.observations) == 3


@respx.mock
async def test_discover_stations_dict_response():
    """Live discovery handles dict-wrapped station list."""
    wrapped = {"stations": MOCK_LIVE_STATIONS}
    respx.get("https://api.vedur.is/hydro/stations.json").mock(
        return_value=httpx.Response(200, json=wrapped)
    )

    async with IcelandLamahIceConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 31
    assert any(s.native_id == "LIVE01" for s in stations)


@respx.mock
async def test_discover_stations_dict_unexpected_format():
    """Dict without known keys returns empty live list."""
    respx.get("https://api.vedur.is/hydro/stations.json").mock(
        return_value=httpx.Response(200, json={"message": "hi"})
    )

    async with IcelandLamahIceConnector() as conn:
        stations = await conn.fetch_stations()

    # Only seed stations
    assert len(stations) == 30


@respx.mock
async def test_discover_stations_skips_no_id():
    """Live stations without an id are skipped."""
    data = [
        {"name": "NoID", "lat": 63.0, "lon": -20.0},
        {"id": "LIVE02", "name": "Has ID", "lat": 64.0, "lon": -19.0},
    ]
    respx.get("https://api.vedur.is/hydro/stations.json").mock(
        return_value=httpx.Response(200, json=data)
    )

    async with IcelandLamahIceConnector() as conn:
        stations = await conn.fetch_stations()

    live_ids = [
        s.native_id for s in stations if s.native_id == "LIVE02"
    ]
    assert len(live_ids) == 1


@respx.mock
async def test_discover_stations_skips_no_coords():
    """Live stations without coords are skipped."""
    data = [
        {"id": "LIVE03", "name": "NoCords"},
    ]
    respx.get("https://api.vedur.is/hydro/stations.json").mock(
        return_value=httpx.Response(200, json=data)
    )

    async with IcelandLamahIceConnector() as conn:
        stations = await conn.fetch_stations()

    # Only seed stations
    assert len(stations) == 30


@respx.mock
async def test_observations_list_format():
    """Observations as bare list (not wrapped in dict) are parsed."""
    bare_list = [
        {"time": "2024-06-01T00:00:00", "discharge": 150.5},
    ]
    respx.get("https://api.vedur.is/hydro/latest.json").mock(
        return_value=httpx.Response(200, json=bare_list)
    )

    async with IcelandLamahIceConnector() as conn:
        chunk = await conn.fetch_observations(
            "iceland_lamahice:VHM001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1


@respx.mock
async def test_observations_obs_list_not_a_list():
    """When obs_list resolves to a non-list, it's replaced with []."""
    respx.get("https://api.vedur.is/hydro/latest.json").mock(
        return_value=httpx.Response(
            200, json={"data": "not-a-list"},
        )
    )

    async with IcelandLamahIceConnector() as conn:
        chunk = await conn.fetch_observations(
            "iceland_lamahice:VHM001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@respx.mock
async def test_observations_missing_timestamp_skipped():
    """Entries without a time field are skipped."""
    data = {"data": [{"discharge": 100.0}]}
    respx.get("https://api.vedur.is/hydro/latest.json").mock(
        return_value=httpx.Response(200, json=data)
    )

    async with IcelandLamahIceConnector() as conn:
        chunk = await conn.fetch_observations(
            "iceland_lamahice:VHM001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@respx.mock
async def test_observations_invalid_timestamp_skipped():
    """Entries with invalid timestamps are skipped."""
    data = {"data": [
        {"time": "not-a-date", "discharge": 100.0},
    ]}
    respx.get("https://api.vedur.is/hydro/latest.json").mock(
        return_value=httpx.Response(200, json=data)
    )

    async with IcelandLamahIceConnector() as conn:
        chunk = await conn.fetch_observations(
            "iceland_lamahice:VHM001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


def test_safe_float_edge_cases():
    """Module-level _safe_float handles edge cases."""
    from csfs.connectors.iceland_lamahice import _safe_float

    assert _safe_float(None) is None
    assert _safe_float("abc") is None
    assert _safe_float("123.4") == pytest.approx(123.4)
