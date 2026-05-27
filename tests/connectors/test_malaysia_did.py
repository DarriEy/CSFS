"""Tests for the Malaysia DID connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.malaysia_did import (
    _SEED_STATIONS,
    MalaysiaDIDConnector,
)

MOCK_STATIONS_RESPONSE = [
    {
        "station_id": "3527412",
        "station_name": "Temerloh",
        "latitude": 3.45,
        "longitude": 102.42,
        "river_name": "Pahang",
    },
    {
        "station_id": "5721442",
        "station_name": "Kuala Krai",
        "latitude": 5.53,
        "longitude": 102.20,
        "river_name": "Kelantan",
    },
]

MOCK_OBSERVATIONS_RESPONSE = {
    "data": [
        {"date": "2024-08-01", "flow_rate": 85.2},
        {"date": "2024-08-02", "flow_rate": 92.7},
        {"date": "2024-08-03", "flow_rate": None},
    ],
}

BASE = "https://publicinfobanjir.water.gov.my"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_primary_endpoint():
    """Stations are fetched from the primary endpoint."""
    respx.get(
        f"{BASE}/aras-air/data-paras-air/",
    ).mock(
        return_value=httpx.Response(
            200, json=MOCK_STATIONS_RESPONSE,
        ),
    )

    async with MalaysiaDIDConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    temerloh = next(
        s for s in stations if s.native_id == "3527412"
    )
    assert temerloh.id == "malaysia_did:3527412"
    assert temerloh.provider == "malaysia_did"
    assert temerloh.name == "Temerloh"
    assert temerloh.country_code == "MY"
    assert temerloh.river == "Pahang"
    assert temerloh.latitude == pytest.approx(3.45)
    assert temerloh.longitude == pytest.approx(102.42)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fallback_to_seed():
    """Falls back to seed list when all endpoints fail."""
    respx.get(
        f"{BASE}/aras-air/data-paras-air/",
    ).mock(return_value=httpx.Response(500))
    respx.get(f"{BASE}/api/stations").mock(
        return_value=httpx.Response(500),
    )

    async with MalaysiaDIDConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    pahang = next(
        s for s in stations if s.native_id == "3527412"
    )
    assert pahang.river == "Pahang"
    assert pahang.country_code == "MY"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fallback_endpoint():
    """Falls back to second endpoint when primary fails."""
    respx.get(
        f"{BASE}/aras-air/data-paras-air/",
    ).mock(return_value=httpx.Response(500))
    respx.get(f"{BASE}/api/stations").mock(
        return_value=httpx.Response(
            200, json=MOCK_STATIONS_RESPONSE,
        ),
    )

    async with MalaysiaDIDConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_primary_endpoint():
    """Flow rate observations are parsed correctly."""
    respx.get(
        f"{BASE}/cerapan/kadar-alir/data-kadar-alir/",
    ).mock(
        return_value=httpx.Response(
            200, json=MOCK_OBSERVATIONS_RESPONSE,
        ),
    )

    async with MalaysiaDIDConnector() as conn:
        chunk = await conn.fetch_observations(
            "malaysia_did:3527412",
            start=datetime(2024, 8, 1, tzinfo=UTC),
            end=datetime(2024, 8, 4, tzinfo=UTC),
        )

    assert chunk.provider == "malaysia_did"
    assert chunk.station_id == "malaysia_did:3527412"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(
        85.2,
    )
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_all_fail_returns_empty():
    """Returns empty chunk when all endpoints fail."""
    respx.get(
        f"{BASE}/cerapan/kadar-alir/data-kadar-alir/",
    ).mock(return_value=httpx.Response(500))
    respx.get(f"{BASE}/api/flow-rate").mock(
        return_value=httpx.Response(500),
    )

    async with MalaysiaDIDConnector() as conn:
        chunk = await conn.fetch_observations(
            "malaysia_did:3527412",
            start=datetime(2024, 8, 1, tzinfo=UTC),
            end=datetime(2024, 8, 3, tzinfo=UTC),
        )

    assert chunk.observations == []
    assert chunk.station_id == "malaysia_did:3527412"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_entries_without_id():
    """Entries without station_id are skipped."""
    data = [
        {"station_name": "No ID", "latitude": 3.0},
        {"station_id": "", "station_name": "Empty"},
        MOCK_STATIONS_RESPONSE[0],
    ]
    respx.get(
        f"{BASE}/aras-air/data-paras-air/",
    ).mock(
        return_value=httpx.Response(200, json=data),
    )

    async with MalaysiaDIDConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "3527412"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_malay_field_names():
    """Observations with Malay field names are parsed."""
    malay_data = [
        {
            "tarikh": "2024-08-01",
            "kadar_alir": 75.0,
        },
        {
            "tarikh": "2024-08-02",
            "kadar_alir": 80.5,
        },
    ]
    respx.get(
        f"{BASE}/cerapan/kadar-alir/data-kadar-alir/",
    ).mock(
        return_value=httpx.Response(
            200, json=malay_data,
        ),
    )

    async with MalaysiaDIDConnector() as conn:
        chunk = await conn.fetch_observations(
            "malaysia_did:5721442",
            start=datetime(2024, 8, 1, tzinfo=UTC),
            end=datetime(2024, 8, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(
        75.0,
    )
    assert chunk.observations[1].discharge_m3s == pytest.approx(
        80.5,
    )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest():
    """fetch_latest fetches last 7 days of observations."""
    respx.get(
        url__startswith=f"{BASE}/cerapan/kadar-alir/data-kadar-alir/",
    ).mock(
        return_value=httpx.Response(
            200, json=MOCK_OBSERVATIONS_RESPONSE,
        ),
    )

    async with MalaysiaDIDConnector() as conn:
        chunk = await conn.fetch_latest("malaysia_did:3527412")

    assert chunk.station_id == "malaysia_did:3527412"
    assert len(chunk.observations) == 3


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_dict_response_with_nested_data():
    """Stations returned as dict with 'data' key are unwrapped."""
    wrapped = {"data": MOCK_STATIONS_RESPONSE}
    respx.get(
        f"{BASE}/aras-air/data-paras-air/",
    ).mock(
        return_value=httpx.Response(200, json=wrapped),
    )

    async with MalaysiaDIDConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_dict_unexpected_format_skips():
    """Dict without list values logs and continues to next endpoint."""
    bad_dict = {"message": "not stations"}
    respx.get(
        f"{BASE}/aras-air/data-paras-air/",
    ).mock(
        return_value=httpx.Response(200, json=bad_dict),
    )
    respx.get(f"{BASE}/api/stations").mock(
        return_value=httpx.Response(200, json=bad_dict),
    )

    async with MalaysiaDIDConnector() as conn:
        stations = await conn.fetch_stations()

    # Falls back to seed stations
    assert len(stations) == len(_SEED_STATIONS)


@pytest.mark.asyncio
@respx.mock
async def test_station_parse_exception_skips_entry():
    """Entries that raise during parsing are skipped gracefully."""
    data = [
        {
            "station_id": "S1",
            "station_name": "OK",
            "latitude": "not-a-float",
            "longitude": "not-a-float",
        },
    ]
    respx.get(
        f"{BASE}/aras-air/data-paras-air/",
    ).mock(
        return_value=httpx.Response(200, json=data),
    )

    async with MalaysiaDIDConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "S1"


@pytest.mark.asyncio
@respx.mock
async def test_observations_obs_list_not_a_list():
    """When obs_list resolves to a non-list, it's replaced with []."""
    data = {"data": "not-a-list"}
    respx.get(
        f"{BASE}/cerapan/kadar-alir/data-kadar-alir/",
    ).mock(
        return_value=httpx.Response(200, json=data),
    )

    async with MalaysiaDIDConnector() as conn:
        chunk = await conn.fetch_observations(
            "malaysia_did:3527412",
            start=datetime(2024, 8, 1, tzinfo=UTC),
            end=datetime(2024, 8, 4, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_observations_missing_timestamp_skipped():
    """Entries with no parseable timestamp are skipped."""
    data = [
        {"flow_rate": 100.0},  # no date key at all
        {"date": "", "flow_rate": 50.0},  # empty string
    ]
    respx.get(
        f"{BASE}/cerapan/kadar-alir/data-kadar-alir/",
    ).mock(
        return_value=httpx.Response(200, json=data),
    )

    async with MalaysiaDIDConnector() as conn:
        chunk = await conn.fetch_observations(
            "malaysia_did:3527412",
            start=datetime(2024, 8, 1, tzinfo=UTC),
            end=datetime(2024, 8, 4, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_observation_parse_exception_skips_entry():
    """Entries that raise ValueError/TypeError during parse are skipped."""
    data = [
        {"date": "2024-08-01", "flow_rate": 100.0},
        {"tarikh": "2024-08-02", "kadar_alir": 200.0},
    ]
    respx.get(
        f"{BASE}/cerapan/kadar-alir/data-kadar-alir/",
    ).mock(
        return_value=httpx.Response(200, json=data),
    )

    async with MalaysiaDIDConnector() as conn:
        chunk = await conn.fetch_observations(
            "malaysia_did:3527412",
            start=datetime(2024, 8, 1, tzinfo=UTC),
            end=datetime(2024, 8, 4, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_timestamp_unparseable_returns_none():
    """Completely unparseable timestamps are skipped."""
    data = [
        {"date": "not-a-date-at-all!!!", "flow_rate": 100.0},
    ]
    respx.get(
        f"{BASE}/cerapan/kadar-alir/data-kadar-alir/",
    ).mock(
        return_value=httpx.Response(200, json=data),
    )

    async with MalaysiaDIDConnector() as conn:
        chunk = await conn.fetch_observations(
            "malaysia_did:3527412",
            start=datetime(2024, 8, 1, tzinfo=UTC),
            end=datetime(2024, 8, 4, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_timestamp_fallback_formats():
    """Timestamps in non-ISO formats are parsed via fallback."""
    data = [
        {"date": "01/08/2024", "flow_rate": 100.0},
        {"date": "01-08-2024", "flow_rate": 200.0},
    ]
    respx.get(
        f"{BASE}/cerapan/kadar-alir/data-kadar-alir/",
    ).mock(
        return_value=httpx.Response(200, json=data),
    )

    async with MalaysiaDIDConnector() as conn:
        chunk = await conn.fetch_observations(
            "malaysia_did:3527412",
            start=datetime(2024, 1, 1, tzinfo=UTC),
            end=datetime(2024, 12, 31, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2


def test_safe_float_non_numeric():
    """_safe_float returns None for non-numeric strings."""
    conn = MalaysiaDIDConnector()
    assert conn._safe_float("abc") is None
    assert conn._safe_float(None) is None
    assert conn._safe_float("123.4") == pytest.approx(123.4)


@pytest.mark.asyncio
@respx.mock
async def test_connector_registration():
    """Connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("malaysia_did")
    assert cls is MalaysiaDIDConnector
