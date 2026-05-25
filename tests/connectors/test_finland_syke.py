"""Tests for the SYKE (Finland) hydrology connector with mocked HTTP responses.

All tests use the confirmed OData API at:
  https://rajapinnat.ymparisto.fi/api/Hydrologiarajapinta/1.0/odata
"""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.finland_syke import FinlandSYKEConnector, _dms_to_decimal, _quality_from_syke
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import QualityFlag

SYKE_BASE = "https://rajapinnat.ymparisto.fi/api/Hydrologiarajapinta/1.0/odata"

# OData station response (Paikka endpoint)
MOCK_STATIONS_RESPONSE = {
    "value": [
        {
            "Paikka_Id": 1001,
            "Nimi": "Ounasjoki - Kittila",
            "KoordLat": "673900",
            "KoordLong": "245436",
        },
        {
            "Paikka_Id": 1002,
            "Nimi": "Kemijoki - Rovaniemi",
            "KoordLat": "663000",
            "KoordLong": "254648",
        },
        {
            "Paikka_Id": 1003,
            "Nimi": "Tornionjoki - Pello",
            "KoordLat": "",
            "KoordLong": "",
        },
    ]
}

# OData discharge response (Virtaama endpoint)
# Observations: 2024-06-01T00:00Z, 01:00Z, 02:00Z, and one outside range (June 15)
MOCK_OBSERVATIONS_RESPONSE = {
    "value": [
        {
            "Aika": "2024-06-01T00:00:00Z",
            "Arvo": 120.5,
            "Laatu": "good",
        },
        {
            "Aika": "2024-06-01T01:00:00Z",
            "Arvo": 121.3,
            "Laatu": "verified",
        },
        {
            "Aika": "2024-06-01T02:00:00Z",
            "Arvo": 119.0,
            "Laatu": "suspect",
        },
        {
            "Aika": "2024-06-15T00:00:00Z",
            "Arvo": 200.0,
            "Laatu": "good",
        },
    ]
}


# ------------------------------------------------------------------
# Station tests (/Paikka endpoint)
# ------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_all():
    """All stations in the OData response are returned from /Paikka."""
    respx.get(f"{SYKE_BASE}/Paikka").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with FinlandSYKEConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 3
    native_ids = {s.native_id for s in stations}
    assert native_ids == {"1001", "1002", "1003"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fields_dms():
    """Station fields are correctly mapped from OData with DDMMSS coordinates."""
    respx.get(f"{SYKE_BASE}/Paikka").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with FinlandSYKEConnector() as conn:
        stations = await conn.fetch_stations()

    ounasjoki = next(s for s in stations if s.native_id == "1001")
    assert ounasjoki.id == "finland_syke:1001"
    assert ounasjoki.provider == "finland_syke"
    assert ounasjoki.name == "Ounasjoki - Kittila"
    assert ounasjoki.latitude == pytest.approx(67.65, abs=0.01)
    assert ounasjoki.longitude == pytest.approx(24.91, abs=0.01)
    assert ounasjoki.country_code == "FI"
    assert ounasjoki.is_active is True


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fields_dms_second():
    """Station coordinates from DDMMSS strings are parsed correctly."""
    respx.get(f"{SYKE_BASE}/Paikka").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with FinlandSYKEConnector() as conn:
        stations = await conn.fetch_stations()

    kemijoki = next(s for s in stations if s.native_id == "1002")
    assert kemijoki.latitude == pytest.approx(66.50, abs=0.01)
    assert kemijoki.longitude == pytest.approx(25.78, abs=0.01)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_empty_coordinates():
    """A station with empty coordinate strings gets (0.0, 0.0) coordinates."""
    respx.get(f"{SYKE_BASE}/Paikka").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with FinlandSYKEConnector() as conn:
        stations = await conn.fetch_stations()

    pello = next(s for s in stations if s.native_id == "1003")
    assert pello.latitude == 0.0
    assert pello.longitude == 0.0


def test_dms_to_decimal():
    """DDMMSS conversion works for typical Finnish coordinates."""
    assert _dms_to_decimal("622536") == pytest.approx(62.4267, abs=0.001)
    assert _dms_to_decimal("302642") == pytest.approx(30.4450, abs=0.001)
    assert _dms_to_decimal("") == 0.0
    assert _dms_to_decimal("None") == 0.0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty OData value array returns no stations."""
    respx.get(f"{SYKE_BASE}/Paikka").mock(
        return_value=httpx.Response(200, json={"value": []})
    )

    async with FinlandSYKEConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_missing_id():
    """Entries without Paikka_Id are silently skipped."""
    data = {
        "value": [
            {"Nimi": "No ID", "KoordinaattiPiste": None},
            {"Paikka_Id": 99, "Nimi": "Has ID", "KoordinaattiPiste": None},
        ]
    }
    respx.get(f"{SYKE_BASE}/Paikka").mock(
        return_value=httpx.Response(200, json=data)
    )

    async with FinlandSYKEConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "99"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_error_raises_connector_error():
    """ConnectorError is raised when /Paikka returns an HTTP error."""
    respx.get(f"{SYKE_BASE}/Paikka").mock(
        return_value=httpx.Response(500)
    )

    async with FinlandSYKEConnector() as conn:
        with pytest.raises(ConnectorError, match="finland_syke"):
            await conn.fetch_stations()


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_bad_format_raises():
    """DataFormatError is raised when response has no 'value' array."""
    respx.get(f"{SYKE_BASE}/Paikka").mock(
        return_value=httpx.Response(200, json={"error": "bad"})
    )

    async with FinlandSYKEConnector() as conn:
        # Should return empty list since data.get("value", []) returns []
        stations = await conn.fetch_stations()
        assert len(stations) == 0


# ------------------------------------------------------------------
# Observation tests (/Virtaama endpoint)
# ------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_filters_by_date_range():
    """Only observations within [start, end] are returned."""
    respx.get(f"{SYKE_BASE}/Virtaama").mock(
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
    respx.get(f"{SYKE_BASE}/Virtaama").mock(
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
    respx.get(f"{SYKE_BASE}/Virtaama").mock(
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
    """A null discharge value (Arvo) results in MISSING quality."""
    data = {
        "value": [
            {"Aika": "2024-06-01T00:00:00Z", "Arvo": None, "Laatu": "good"},
        ]
    }
    respx.get(f"{SYKE_BASE}/Virtaama").mock(
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
    """An empty OData value array returns zero observations."""
    respx.get(f"{SYKE_BASE}/Virtaama").mock(
        return_value=httpx.Response(200, json={"value": []})
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
    respx.get(f"{SYKE_BASE}/Virtaama").mock(
        return_value=httpx.Response(200, json={"value": []})
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
    respx.get(f"{SYKE_BASE}/Virtaama").mock(
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
    data = {
        "value": [
            {"Aika": "not-a-date", "Arvo": 10.0, "Laatu": "good"},
        ]
    }
    respx.get(f"{SYKE_BASE}/Virtaama").mock(
        return_value=httpx.Response(200, json=data)
    )

    async with FinlandSYKEConnector() as conn:
        with pytest.raises(DataFormatError, match="Invalid timestamp"):
            await conn.fetch_observations(
                "finland_syke:1001",
                start=datetime(2024, 6, 1, tzinfo=UTC),
                end=datetime(2024, 6, 2, tzinfo=UTC),
            )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_error_raises_connector_error():
    """ConnectorError is raised when /Virtaama returns an HTTP error."""
    respx.get(f"{SYKE_BASE}/Virtaama").mock(
        return_value=httpx.Response(500)
    )

    async with FinlandSYKEConnector() as conn:
        with pytest.raises(ConnectorError, match="finland_syke"):
            await conn.fetch_observations(
                "finland_syke:1001",
                start=datetime(2024, 6, 1, tzinfo=UTC),
                end=datetime(2024, 6, 2, tzinfo=UTC),
            )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_no_quality_field():
    """Missing Laatu field results in RAW quality."""
    data = {
        "value": [
            {"Aika": "2024-06-01T00:00:00Z", "Arvo": 50.0},
        ]
    }
    respx.get(f"{SYKE_BASE}/Virtaama").mock(
        return_value=httpx.Response(200, json=data)
    )

    async with FinlandSYKEConnector() as conn:
        chunk = await conn.fetch_observations(
            "finland_syke:1001",
            start=datetime(2024, 6, 1, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 2, 0, 0, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].quality == QualityFlag.RAW


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
    assert "Hydrologiarajapinta" in FinlandSYKEConnector.base_url
    assert "odata" in FinlandSYKEConnector.base_url
