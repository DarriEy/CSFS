"""Tests for the Bosnia FHMZ connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.bosnia_fhmz import (
    _SEED_STATIONS,
    BosniaFhmzConnector,
)

BASE_URL = "https://www.fhmzbih.gov.ba"

MOCK_OBSERVATIONS = [
    {"datum": "2024-06-01T06:00:00", "protok": 15.3},
    {"datum": "2024-06-01T12:00:00", "protok": 18.7},
    {"datum": "2024-06-01T18:00:00", "protok": None},
    {"datum": "2024-06-02T06:00:00", "protok": 16.0},
]


@pytest.mark.asyncio
async def test_fetch_stations_returns_seed_list():
    """Seed stations are always returned."""
    async with BosniaFhmzConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    native_ids = {s.native_id for s in stations}
    assert "1001" in native_ids  # Sarajevo
    assert "1011" in native_ids  # Mostar


@pytest.mark.asyncio
async def test_fetch_stations_seed_fields():
    """Seed stations have correct metadata."""
    async with BosniaFhmzConnector() as conn:
        stations = await conn.fetch_stations()

    sarajevo = next(s for s in stations if s.native_id == "1001")
    assert sarajevo.id == "bosnia_fhmz:1001"
    assert sarajevo.provider == "bosnia_fhmz"
    assert sarajevo.name == "Sarajevo - Bentbaša"
    assert sarajevo.country_code == "BA"
    assert sarajevo.river == "Miljacka"
    assert sarajevo.latitude == pytest.approx(43.86)
    assert sarajevo.longitude == pytest.approx(18.435)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_json():
    """Observations are correctly parsed with date filtering."""
    respx.get(f"{BASE_URL}/latinica/HIDRO/api/podaci").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS),
    )

    async with BosniaFhmzConnector() as conn:
        chunk = await conn.fetch_observations(
            "bosnia_fhmz:1001",
            start=datetime(2024, 6, 1, 10, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 1, 20, 0, 0, tzinfo=UTC),
        )

    assert chunk.provider == "bosnia_fhmz"
    assert chunk.station_id == "bosnia_fhmz:1001"
    # Only 12:00 and 18:00 within range
    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(18.7)
    assert chunk.observations[1].discharge_m3s is None
    assert chunk.observations[1].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_server_error():
    """Server errors return an empty chunk instead of raising."""
    respx.get(f"{BASE_URL}/latinica/HIDRO/api/podaci").mock(
        return_value=httpx.Response(500, text="Server Error"),
    )

    async with BosniaFhmzConnector() as conn:
        chunk = await conn.fetch_observations(
            "bosnia_fhmz:1001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0
    assert chunk.provider == "bosnia_fhmz"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty():
    """An empty observation list returns zero observations."""
    respx.get(f"{BASE_URL}/latinica/HIDRO/api/podaci").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with BosniaFhmzConnector() as conn:
        chunk = await conn.fetch_observations(
            "bosnia_fhmz:1001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_wrapped_response():
    """Observations wrapped in a 'podaci' key are parsed correctly."""
    wrapped = {"podaci": MOCK_OBSERVATIONS[:2]}
    respx.get(f"{BASE_URL}/latinica/HIDRO/api/podaci").mock(
        return_value=httpx.Response(200, json=wrapped),
    )

    async with BosniaFhmzConnector() as conn:
        chunk = await conn.fetch_observations(
            "bosnia_fhmz:1001",
            start=datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 2, 0, 0, 0, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_all_in_range():
    """All observations in range are returned."""
    respx.get(f"{BASE_URL}/latinica/HIDRO/api/podaci").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS),
    )

    async with BosniaFhmzConnector() as conn:
        chunk = await conn.fetch_observations(
            "bosnia_fhmz:1001",
            start=datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 2, 23, 59, 59, tzinfo=UTC),
        )

    assert len(chunk.observations) == 4


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_generic_exception_returns_empty():
    """Generic exceptions return an empty chunk."""
    respx.get(f"{BASE_URL}/latinica/HIDRO/api/podaci").mock(
        side_effect=RuntimeError("connection failed"),
    )

    async with BosniaFhmzConnector() as conn:
        chunk = await conn.fetch_observations(
            "bosnia_fhmz:1001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0
    assert chunk.provider == "bosnia_fhmz"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest_delegates():
    """fetch_latest calls fetch_observations for last 24h."""
    respx.get(f"{BASE_URL}/latinica/HIDRO/api/podaci").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with BosniaFhmzConnector() as conn:
        chunk = await conn.fetch_latest("bosnia_fhmz:1001")

    assert chunk.provider == "bosnia_fhmz"
    assert chunk.station_id == "bosnia_fhmz:1001"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_unexpected_type_raises():
    """Non-list/dict response raises DataFormatError."""
    from csfs.core.exceptions import DataFormatError

    respx.get(f"{BASE_URL}/latinica/HIDRO/api/podaci").mock(
        return_value=httpx.Response(200, text='"just a string"'),
    )

    async with BosniaFhmzConnector() as conn:
        with pytest.raises(DataFormatError, match="Unexpected response type"):
            await conn.fetch_observations(
                "bosnia_fhmz:1001",
                start=datetime(2024, 6, 1, tzinfo=UTC),
                end=datetime(2024, 6, 2, tzinfo=UTC),
            )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_invalid_timestamp_raises():
    """Invalid timestamp in observation raises DataFormatError."""
    from csfs.core.exceptions import DataFormatError

    bad_data = [
        {"datum": "not-a-date", "protok": 15.3},
    ]
    respx.get(f"{BASE_URL}/latinica/HIDRO/api/podaci").mock(
        return_value=httpx.Response(200, json=bad_data),
    )

    async with BosniaFhmzConnector() as conn:
        with pytest.raises(DataFormatError, match="Invalid or missing timestamp"):
            await conn.fetch_observations(
                "bosnia_fhmz:1001",
                start=datetime(2024, 6, 1, tzinfo=UTC),
                end=datetime(2024, 6, 2, tzinfo=UTC),
            )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_data_key_response():
    """Observations wrapped in a 'data' key (not 'podaci') are parsed."""
    wrapped = {"data": MOCK_OBSERVATIONS[:2]}
    respx.get(f"{BASE_URL}/latinica/HIDRO/api/podaci").mock(
        return_value=httpx.Response(200, json=wrapped),
    )

    async with BosniaFhmzConnector() as conn:
        chunk = await conn.fetch_observations(
            "bosnia_fhmz:1001",
            start=datetime(2024, 6, 1, 0, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 2, 0, 0, 0, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2


def test_connector_is_registered():
    """The connector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("bosnia_fhmz")
    assert cls is BosniaFhmzConnector
