"""Tests for the ARPA Piemonte (Italy) connector with mocked HTTP responses.

ARPA Piemonte is a LEVEL-ONLY provider: the upstream API publishes
``hydrometric_level`` (metres), never discharge (portata, m3/s). These tests
pin that behaviour -- observations must carry ``discharge_m3s=None`` and
``QualityFlag.MISSING`` rather than fabricating a flow value from the level.
"""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.italy_piedmont import ItalyPiedmontConnector
from csfs.core.models import QualityFlag

BASE = "https://utility.arpa.piemonte.it/api_realtime"

# /pie_anag returns a bare list of registry records. "station_type" letters
# flag sensors; "I" marks an idrometric (water-level) station.
MOCK_ANAG_RESPONSE = [
    {
        "id_network": "PIE",
        "station_code": "200",
        "name": "PALESTRO SESIA",
        "lat": 45.285,
        "lng": 8.54,
        "station_type": "I",
        "river_name": "SESIA",
        "quote": 121.0,
    },
    {
        "id_network": "PIE",
        "station_code": "138",
        "name": "ASTI TANARO",
        "lat": 44.885,
        "lng": 8.21222,
        "station_type": "IPT",
        "river_name": "TANARO",
        "quote": 108.1,
    },
    {
        # Pure rain/temp station (no "I") -> must be excluded.
        "id_network": "PIE",
        "station_code": "001",
        "name": "COLLE LOMBARDA",
        "lat": 44.2078,
        "lng": 7.14778,
        "station_type": "NPTV",
        "river_name": "-",
        "quote": 2305.0,
    },
]

# /data_pie wraps records in a pagination envelope. Records expose
# "hydrometric_level" (metres) -- there is NO discharge field.
MOCK_DATA_RESPONSE = {
    "page": 1,
    "page_size": 10000,
    "total_pages": 1,
    "total_items": 3,
    "data": [
        {
            "id_network": "PIE",
            "station_code": "200",
            "date": "2026-05-30T00:00:00+02:00",
            "hydrometric_level": 2.0,
        },
        {
            "id_network": "PIE",
            "station_code": "200",
            "date": "2026-05-30T01:00:00+02:00",
            "hydrometric_level": 1.99,
        },
        {
            # No level reading at all -> skipped as a pure gap.
            "id_network": "PIE",
            "station_code": "200",
            "date": "2026-05-30T02:00:00+02:00",
            "hydrometric_level": None,
        },
    ],
}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_filters_hydrometric():
    """Only stations whose station_type includes 'I' are returned."""
    respx.get(f"{BASE}/pie_anag").mock(
        return_value=httpx.Response(200, json=MOCK_ANAG_RESPONSE)
    )

    async with ItalyPiedmontConnector() as conn:
        stations = await conn.fetch_stations()

    # COLLE LOMBARDA (NPTV, no 'I') is excluded.
    assert len(stations) == 2
    native_ids = {s.native_id for s in stations}
    assert native_ids == {"200", "138"}

    sesia = next(s for s in stations if s.native_id == "200")
    assert sesia.id == "italy_piedmont:200"
    assert sesia.provider == "italy_piedmont"
    assert sesia.country_code == "IT"
    assert sesia.river == "SESIA"
    assert sesia.latitude == 45.285
    assert sesia.longitude == 8.54
    assert sesia.elevation_m == 121.0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_strips_placeholder_river():
    """A '-' river_name is normalised to None (verified on an 'I' station)."""
    data = [
        {
            "station_code": "999",
            "name": "LAKE STATION",
            "lat": 45.0,
            "lng": 8.0,
            "station_type": "IPRTV",
            "river_name": "-",
            "quote": 200.0,
        }
    ]
    respx.get(f"{BASE}/pie_anag").mock(return_value=httpx.Response(200, json=data))

    async with ItalyPiedmontConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].river is None


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_level_only_no_discharge():
    """Observations carry NO discharge: m3/s is None and quality is MISSING."""
    respx.get(f"{BASE}/data_pie").mock(
        return_value=httpx.Response(200, json=MOCK_DATA_RESPONSE)
    )

    async with ItalyPiedmontConnector() as conn:
        chunk = await conn.fetch_observations(
            "italy_piedmont:200",
            start=datetime(2026, 5, 30),
            end=datetime(2026, 5, 31),
        )

    assert chunk.provider == "italy_piedmont"
    assert chunk.station_id == "italy_piedmont:200"

    # Two records have a level; the third (level=None) is dropped as a gap.
    assert len(chunk.observations) == 2

    # CRITICAL: discharge is never fabricated from level.
    for obs in chunk.observations:
        assert obs.discharge_m3s is None
        assert obs.quality == QualityFlag.MISSING

    # Timestamps are preserved (with their local offset).
    assert chunk.observations[0].timestamp.isoformat() == "2026-05-30T00:00:00+02:00"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty_envelope():
    """An empty pagination envelope yields zero observations."""
    empty = {"page": 1, "page_size": 10000, "total_pages": 0, "total_items": 0, "data": []}
    respx.get(f"{BASE}/data_pie").mock(return_value=httpx.Response(200, json=empty))

    async with ItalyPiedmontConnector() as conn:
        chunk = await conn.fetch_observations(
            "italy_piedmont:200",
            start=datetime(2026, 5, 30),
            end=datetime(2026, 5, 31),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_tolerates_bare_list():
    """A bare-list /data_pie payload (no envelope) is also parsed."""
    bare = [
        {
            "station_code": "200",
            "date": "2026-05-30T00:00:00+02:00",
            "hydrometric_level": 2.0,
        }
    ]
    respx.get(f"{BASE}/data_pie").mock(return_value=httpx.Response(200, json=bare))

    async with ItalyPiedmontConnector() as conn:
        chunk = await conn.fetch_observations(
            "italy_piedmont:200",
            start=datetime(2026, 5, 30),
            end=datetime(2026, 5, 31),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s is None


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_empty_registry():
    """An empty registry returns no stations."""
    respx.get(f"{BASE}/pie_anag").mock(return_value=httpx.Response(200, json=[]))

    async with ItalyPiedmontConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


def test_parse_stations_rejects_non_list():
    """A non-list pie_anag payload raises DataFormatError."""
    from csfs.core.exceptions import DataFormatError

    conn = ItalyPiedmontConnector()
    with pytest.raises(DataFormatError):
        conn._parse_stations({"unexpected": "shape"})


def test_parse_observations_skips_bad_timestamp():
    """A record with an unparseable timestamp is skipped, not fatal."""
    conn = ItalyPiedmontConnector()
    data = {
        "data": [
            {"date": "not-a-date", "hydrometric_level": 1.0},
            {"date": "2026-05-30T00:00:00+02:00", "hydrometric_level": 1.5},
        ]
    }
    chunk = conn._parse_observations(data, "italy_piedmont:200")
    assert len(chunk.observations) == 1


def test_registration():
    """The connector registers under the 'italy_piedmont' slug."""
    from csfs.core.registry import discover, get_connector

    discover()
    cls = get_connector("italy_piedmont")
    assert cls is ItalyPiedmontConnector
    assert cls.slug == "italy_piedmont"
    assert cls.country_codes == ["IT"]
