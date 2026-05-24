"""Tests for the SYKE (Finland) hydrology connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.finland_syke import FinlandSYKEConnector, _quality_from_syke
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import QualityFlag

SYKE_BASE = "https://rajapinnat.ymparisto.fi/api/hydrology/v1"

MOCK_STATIONS_RESPONSE = [
    {
        "id": "1001",
        "name": "Ounasjoki - Kittila",
        "lat": 67.65,
        "lon": 24.91,
        "river": "Ounasjoki",
        "catchmentArea": 8415.0,
        "active": True,
    },
    {
        "id": "1002",
        "name": "Kemijoki - Rovaniemi",
        "lat": 66.50,
        "lon": 25.78,
        "river": "Kemijoki",
        "catchmentArea": 50680.0,
        "active": True,
    },
    {
        "id": "1003",
        "name": "Tornionjoki - Pello",
        "lat": 66.77,
        "lon": 23.96,
        "river": "Tornionjoki",
        "catchmentArea": 25370.0,
        "active": False,
    },
]

MOCK_SITES_RESPONSE = [
    {
        "siteId": "2001",
        "siteName": "Vuoksi - Imatra",
        "latitude": 61.17,
        "longitude": 28.77,
        "catchmentArea": 61061.0,
    },
    {
        "siteId": "2002",
        "siteName": "Kokemaenjoki - Pori",
        "latitude": 61.48,
        "longitude": 21.80,
        "catchmentArea": 27046.0,
    },
]

# Observations: 2024-06-01T00:00Z, 01:00Z, 02:00Z, and one outside range (June 15)
MOCK_OBSERVATIONS_RESPONSE = [
    {
        "time": "2024-06-01T00:00:00Z",
        "value": 120.5,
        "quality": "good",
    },
    {
        "time": "2024-06-01T01:00:00Z",
        "value": 121.3,
        "quality": "verified",
    },
    {
        "time": "2024-06-01T02:00:00Z",
        "value": 119.0,
        "quality": "suspect",
    },
    {
        "time": "2024-06-15T00:00:00Z",
        "value": 200.0,
        "quality": "good",
    },
]

MOCK_VALUES_RESPONSE = [
    {
        "dateTime": "2024-06-01T00:00:00Z",
        "value": 55.0,
        "quality": "good",
    },
    {
        "dateTime": "2024-06-01T06:00:00Z",
        "value": 56.2,
        "quality": "estimated",
    },
]


# ------------------------------------------------------------------
# Station tests (primary endpoint)
# ------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_primary_parses_all():
    """All stations in the response are returned from /stations."""
    respx.get(f"{SYKE_BASE}/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with FinlandSYKEConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 3
    native_ids = {s.native_id for s in stations}
    assert native_ids == {"1001", "1002", "1003"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_primary_fields():
    """Station fields are correctly mapped from primary endpoint."""
    respx.get(f"{SYKE_BASE}/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with FinlandSYKEConnector() as conn:
        stations = await conn.fetch_stations()

    ounasjoki = next(s for s in stations if s.native_id == "1001")
    assert ounasjoki.id == "finland_syke:1001"
    assert ounasjoki.provider == "finland_syke"
    assert ounasjoki.name == "Ounasjoki - Kittila"
    assert ounasjoki.latitude == 67.65
    assert ounasjoki.longitude == 24.91
    assert ounasjoki.country_code == "FI"
    assert ounasjoki.river == "Ounasjoki"
    assert ounasjoki.catchment_area_km2 == 8415.0
    assert ounasjoki.is_active is True


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_inactive_flag():
    """Inactive stations are parsed with is_active=False."""
    respx.get(f"{SYKE_BASE}/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with FinlandSYKEConnector() as conn:
        stations = await conn.fetch_stations()

    pello = next(s for s in stations if s.native_id == "1003")
    assert pello.is_active is False


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty station array returns no stations."""
    respx.get(f"{SYKE_BASE}/stations").mock(
        return_value=httpx.Response(200, json=[])
    )

    async with FinlandSYKEConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_missing_id():
    """Stations without an 'id' field are silently skipped."""
    data = [
        {"name": "No ID", "lat": 60.0, "lon": 25.0, "active": True},
        {"id": "99", "name": "Has ID", "lat": 61.0, "lon": 26.0, "active": True},
    ]
    respx.get(f"{SYKE_BASE}/stations").mock(
        return_value=httpx.Response(200, json=data)
    )

    async with FinlandSYKEConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "99"


# ------------------------------------------------------------------
# Station tests (fallback endpoint)
# ------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fallback_on_primary_failure():
    """Falls back to /sites when /stations returns an error."""
    respx.get(f"{SYKE_BASE}/stations").mock(
        return_value=httpx.Response(404)
    )
    respx.get(f"{SYKE_BASE}/sites").mock(
        return_value=httpx.Response(200, json=MOCK_SITES_RESPONSE)
    )

    async with FinlandSYKEConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    native_ids = {s.native_id for s in stations}
    assert native_ids == {"2001", "2002"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fallback_fields():
    """Station fields from /sites endpoint are correctly mapped."""
    respx.get(f"{SYKE_BASE}/stations").mock(
        return_value=httpx.Response(404)
    )
    respx.get(f"{SYKE_BASE}/sites").mock(
        return_value=httpx.Response(200, json=MOCK_SITES_RESPONSE)
    )

    async with FinlandSYKEConnector() as conn:
        stations = await conn.fetch_stations()

    vuoksi = next(s for s in stations if s.native_id == "2001")
    assert vuoksi.id == "finland_syke:2001"
    assert vuoksi.name == "Vuoksi - Imatra"
    assert vuoksi.latitude == 61.17
    assert vuoksi.longitude == 28.77
    assert vuoksi.catchment_area_km2 == 61061.0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_both_fail_raises():
    """ConnectorError is raised when both endpoints fail."""
    respx.get(f"{SYKE_BASE}/stations").mock(
        return_value=httpx.Response(500)
    )
    respx.get(f"{SYKE_BASE}/sites").mock(
        return_value=httpx.Response(500)
    )

    async with FinlandSYKEConnector() as conn:
        with pytest.raises(ConnectorError, match="finland_syke"):
            await conn.fetch_stations()


# ------------------------------------------------------------------
# Observation tests (primary endpoint)
# ------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_filters_by_date_range():
    """Only observations within [start, end] are returned."""
    respx.get(f"{SYKE_BASE}/observations").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE)
    )

    async with FinlandSYKEConnector() as conn:
        chunk = await conn.fetch_observations(
            "finland_syke:1001",
            start=datetime(2024, 6, 1, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 1, 2, 0, tzinfo=UTC),
        )

    # The fourth observation (June 15) should be filtered out
    assert len(chunk.observations) == 3
    assert chunk.station_id == "finland_syke:1001"
    assert chunk.provider == "finland_syke"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_quality_mapping():
    """SYKE quality codes are correctly mapped to CSFS quality flags."""
    respx.get(f"{SYKE_BASE}/observations").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE)
    )

    async with FinlandSYKEConnector() as conn:
        chunk = await conn.fetch_observations(
            "finland_syke:1001",
            start=datetime(2024, 6, 1, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 1, 2, 0, tzinfo=UTC),
        )

    # "good" -> GOOD
    assert chunk.observations[0].quality == QualityFlag.GOOD
    # "verified" -> GOOD
    assert chunk.observations[1].quality == QualityFlag.GOOD
    # "suspect" -> SUSPECT
    assert chunk.observations[2].quality == QualityFlag.SUSPECT


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_discharge_values():
    """Discharge values are parsed as floats in m3/s."""
    respx.get(f"{SYKE_BASE}/observations").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE)
    )

    async with FinlandSYKEConnector() as conn:
        chunk = await conn.fetch_observations(
            "finland_syke:1001",
            start=datetime(2024, 6, 1, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 1, 2, 0, tzinfo=UTC),
        )

    assert chunk.observations[0].discharge_m3s == pytest.approx(120.5)
    assert chunk.observations[1].discharge_m3s == pytest.approx(121.3)
    assert chunk.observations[2].discharge_m3s == pytest.approx(119.0)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_null_value():
    """A null discharge value results in MISSING quality."""
    data = [
        {"time": "2024-06-01T00:00:00Z", "value": None, "quality": "good"},
    ]
    respx.get(f"{SYKE_BASE}/observations").mock(
        return_value=httpx.Response(200, json=data)
    )

    async with FinlandSYKEConnector() as conn:
        chunk = await conn.fetch_observations(
            "finland_syke:1001",
            start=datetime(2024, 6, 1, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 2, 0, 0, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[0].quality == QualityFlag.MISSING


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty():
    """An empty observation array returns zero observations."""
    respx.get(f"{SYKE_BASE}/observations").mock(
        return_value=httpx.Response(200, json=[])
    )

    async with FinlandSYKEConnector() as conn:
        chunk = await conn.fetch_observations(
            "finland_syke:1001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_strips_prefix():
    """The connector correctly strips its slug prefix from the station ID."""
    respx.get(f"{SYKE_BASE}/observations").mock(
        return_value=httpx.Response(200, json=[])
    )

    async with FinlandSYKEConnector() as conn:
        chunk = await conn.fetch_observations(
            "finland_syke:42",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert chunk.station_id == "finland_syke:42"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_naive_datetimes():
    """Naive start/end datetimes are treated as UTC for filtering."""
    respx.get(f"{SYKE_BASE}/observations").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE)
    )

    async with FinlandSYKEConnector() as conn:
        chunk = await conn.fetch_observations(
            "finland_syke:1001",
            start=datetime(2024, 6, 1, 0, 0),  # naive
            end=datetime(2024, 6, 1, 1, 30),    # naive
        )

    # Should include 00:00 and 01:00 but not 02:00
    assert len(chunk.observations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_invalid_timestamp():
    """Invalid timestamps raise DataFormatError."""
    data = [
        {"time": "not-a-date", "value": 10.0, "quality": "good"},
    ]
    respx.get(f"{SYKE_BASE}/observations").mock(
        return_value=httpx.Response(200, json=data)
    )

    async with FinlandSYKEConnector() as conn:
        with pytest.raises(DataFormatError, match="Invalid timestamp"):
            await conn.fetch_observations(
                "finland_syke:1001",
                start=datetime(2024, 6, 1, tzinfo=UTC),
                end=datetime(2024, 6, 2, tzinfo=UTC),
            )


# ------------------------------------------------------------------
# Observation tests (fallback endpoint)
# ------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_fallback_on_primary_failure():
    """Falls back to /values when /observations returns an error."""
    respx.get(f"{SYKE_BASE}/observations").mock(
        return_value=httpx.Response(404)
    )
    respx.get(f"{SYKE_BASE}/values").mock(
        return_value=httpx.Response(200, json=MOCK_VALUES_RESPONSE)
    )

    async with FinlandSYKEConnector() as conn:
        chunk = await conn.fetch_observations(
            "finland_syke:1001",
            start=datetime(2024, 6, 1, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 1, 12, 0, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(55.0)
    assert chunk.observations[1].discharge_m3s == pytest.approx(56.2)
    assert chunk.observations[1].quality == QualityFlag.ESTIMATED


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_both_fail_raises():
    """ConnectorError is raised when both observation endpoints fail."""
    respx.get(f"{SYKE_BASE}/observations").mock(
        return_value=httpx.Response(500)
    )
    respx.get(f"{SYKE_BASE}/values").mock(
        return_value=httpx.Response(500)
    )

    async with FinlandSYKEConnector() as conn:
        with pytest.raises(ConnectorError, match="finland_syke"):
            await conn.fetch_observations(
                "finland_syke:1001",
                start=datetime(2024, 6, 1, tzinfo=UTC),
                end=datetime(2024, 6, 2, tzinfo=UTC),
            )


# ------------------------------------------------------------------
# Quality mapping unit tests
# ------------------------------------------------------------------


def test_quality_from_syke_good():
    assert _quality_from_syke("good") == QualityFlag.GOOD


def test_quality_from_syke_verified():
    assert _quality_from_syke("verified") == QualityFlag.GOOD


def test_quality_from_syke_approved():
    assert _quality_from_syke("approved") == QualityFlag.GOOD


def test_quality_from_syke_numeric_good():
    assert _quality_from_syke("2") == QualityFlag.GOOD


def test_quality_from_syke_suspect():
    assert _quality_from_syke("suspect") == QualityFlag.SUSPECT


def test_quality_from_syke_numeric_suspect():
    assert _quality_from_syke("1") == QualityFlag.SUSPECT


def test_quality_from_syke_estimated():
    assert _quality_from_syke("estimated") == QualityFlag.ESTIMATED


def test_quality_from_syke_none():
    assert _quality_from_syke(None) == QualityFlag.RAW


def test_quality_from_syke_empty():
    assert _quality_from_syke("") == QualityFlag.RAW


def test_quality_from_syke_unknown():
    assert _quality_from_syke("SomeOtherCode") == QualityFlag.RAW


def test_quality_from_syke_case_insensitive():
    assert _quality_from_syke("Good") == QualityFlag.GOOD
    assert _quality_from_syke("VERIFIED") == QualityFlag.GOOD
    assert _quality_from_syke("Suspect") == QualityFlag.SUSPECT


def test_quality_from_syke_strips_whitespace():
    assert _quality_from_syke("  good  ") == QualityFlag.GOOD


# ------------------------------------------------------------------
# Registration test
# ------------------------------------------------------------------


def test_connector_is_registered():
    """The connector registers itself under the expected slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("finland_syke")
    assert cls is FinlandSYKEConnector


def test_connector_metadata():
    """Verify class-level attributes."""
    assert FinlandSYKEConnector.slug == "finland_syke"
    assert FinlandSYKEConnector.country_codes == ["FI"]
    assert "ymparisto" in FinlandSYKEConnector.base_url
