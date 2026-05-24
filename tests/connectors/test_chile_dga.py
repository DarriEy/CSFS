"""Tests for Chile DGA connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.chile_dga import ChileDGAConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import QualityFlag

# ── BNA-style mock payloads ──────────────────────────────────────────

MOCK_STATIONS_BNA = [
    {
        "codigo": "05410001",
        "nombre": "RIO MAIPO EN EL MANZANO",
        "latitud": -33.59,
        "longitud": -70.37,
        "rio": "MAIPO",
        "cuenca": "MAIPO",
        "area_drenaje": 4968.0,
        "vigente": True,
    },
    {
        "codigo": "05410002",
        "nombre": "RIO MAPOCHO EN LOS ALMENDROS",
        "latitud": -33.37,
        "longitud": -70.45,
        "rio": "MAPOCHO",
        "cuenca": "MAIPO",
        "area_drenaje": 620.0,
        "vigente": False,
    },
    {
        "codigo": "05410003",
        "nombre": "INCOMPLETE STATION",
        "latitud": None,
        "longitud": None,
        "rio": "X",
        "cuenca": "X",
        "area_drenaje": None,
        "vigente": True,
    },
]

MOCK_OBSERVATIONS_BNA = {
    "datos": [
        {
            "fecha": "2024-06-01T12:00:00",
            "valor": 45.2,
            "calidad": "bueno",
        },
        {
            "fecha": "2024-06-02T12:00:00",
            "valor": 38.7,
            "calidad": "dudoso",
        },
        {
            "fecha": "2024-06-03T12:00:00",
            "valor": 50.1,
            "calidad": "estimado",
        },
        {
            "fecha": "2024-06-04T12:00:00",
            "valor": None,
            "calidad": "bueno",
        },
    ],
}

# ── V1-style mock payloads ───────────────────────────────────────────

MOCK_STATIONS_V1 = [
    {
        "id": "05410001",
        "name": "RIO MAIPO EN EL MANZANO",
        "latitude": -33.59,
        "longitude": -70.37,
        "river": "MAIPO",
        "catchment_area_km2": 4968.0,
        "active": True,
    },
]

MOCK_OBSERVATIONS_V1 = {
    "measurements": [
        {
            "timestamp": "2024-06-01T12:00:00",
            "value": 45.2,
            "quality": "bueno",
        },
    ],
}

# ── Base URLs ────────────────────────────────────────────────────────

_BNA_BASE = "https://snia.mop.gob.cl/BNAConsultas/reportes"
_V1_BASE = "https://snia.mop.gob.cl/api/v1"


# =====================================================================
# Station tests
# =====================================================================


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_bna():
    """Stations are parsed correctly from BNA JSON array response."""
    respx.get(f"{_BNA_BASE}/consultaEstaciones").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_BNA),
    )

    async with ChileDGAConnector() as conn:
        stations = await conn.fetch_stations()

    # Third station has None lat/lon and should be skipped
    assert len(stations) == 2

    s0 = stations[0]
    assert s0.id == "chile_dga:05410001"
    assert s0.native_id == "05410001"
    assert s0.name == "RIO MAIPO EN EL MANZANO"
    assert s0.latitude == pytest.approx(-33.59)
    assert s0.longitude == pytest.approx(-70.37)
    assert s0.river == "MAIPO"
    assert s0.catchment_area_km2 == pytest.approx(4968.0)
    assert s0.is_active is True
    assert s0.country_code == "CL"
    assert s0.provider == "chile_dga"

    assert stations[1].native_id == "05410002"
    assert stations[1].is_active is False


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fallback_to_v1():
    """When BNA station endpoint fails, connector falls back to v1 API."""
    respx.get(f"{_BNA_BASE}/consultaEstaciones").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{_V1_BASE}/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_V1),
    )

    async with ChileDGAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].id == "chile_dga:05410001"
    assert stations[0].name == "RIO MAIPO EN EL MANZANO"


# =====================================================================
# Observation tests
# =====================================================================


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_bna():
    """Observations are parsed correctly from BNA JSON response."""
    respx.get(f"{_BNA_BASE}/consultaDatos").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_BNA),
    )

    async with ChileDGAConnector() as conn:
        chunk = await conn.fetch_observations(
            "chile_dga:05410001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 4, tzinfo=UTC),
        )

    assert chunk.station_id == "chile_dga:05410001"
    assert chunk.provider == "chile_dga"
    assert len(chunk.observations) == 4

    assert chunk.observations[0].discharge_m3s == pytest.approx(45.2)
    assert chunk.observations[0].quality == QualityFlag.GOOD

    assert chunk.observations[1].discharge_m3s == pytest.approx(38.7)
    assert chunk.observations[1].quality == QualityFlag.SUSPECT

    assert chunk.observations[2].discharge_m3s == pytest.approx(50.1)
    assert chunk.observations[2].quality == QualityFlag.ESTIMATED

    # None discharge -> MISSING
    assert chunk.observations[3].discharge_m3s is None
    assert chunk.observations[3].quality == QualityFlag.MISSING


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_fallback_to_v1():
    """When BNA data endpoint fails, connector falls back to v1 API."""
    respx.get(f"{_BNA_BASE}/consultaDatos").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{_V1_BASE}/stations/05410001/measurements").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_V1),
    )

    async with ChileDGAConnector() as conn:
        chunk = await conn.fetch_observations(
            "chile_dga:05410001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 1, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(45.2)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty_datos():
    """Empty datos array produces zero observations."""
    respx.get(f"{_BNA_BASE}/consultaDatos").mock(
        return_value=httpx.Response(200, json={"datos": []}),
    )

    async with ChileDGAConnector() as conn:
        chunk = await conn.fetch_observations(
            "chile_dga:05410001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 1, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0
    assert chunk.station_id == "chile_dga:05410001"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_invalid_json():
    """Non-JSON body raises DataFormatError."""
    respx.get(f"{_BNA_BASE}/consultaDatos").mock(
        return_value=httpx.Response(200, text="<html>Error</html>"),
    )

    async with ChileDGAConnector() as conn:
        with pytest.raises(DataFormatError, match="Invalid JSON"):
            await conn.fetch_observations(
                "chile_dga:05410001",
                start=datetime(2024, 6, 1, tzinfo=UTC),
                end=datetime(2024, 6, 1, tzinfo=UTC),
            )


# =====================================================================
# Quality mapping tests
# =====================================================================


@pytest.mark.asyncio
@respx.mock
async def test_quality_flag_mapping():
    """DGA quality strings are mapped to correct QualityFlag values."""
    payload = {
        "datos": [
            {"fecha": "2024-01-01T00:00:00", "valor": 10.0, "calidad": "bueno"},
            {"fecha": "2024-01-02T00:00:00", "valor": 11.0, "calidad": "dudoso"},
            {"fecha": "2024-01-03T00:00:00", "valor": 12.0, "calidad": "estimado"},
            {"fecha": "2024-01-04T00:00:00", "valor": 13.0, "calidad": "unknown_value"},
            {"fecha": "2024-01-05T00:00:00", "valor": 14.0, "calidad": None},
        ],
    }
    respx.get(f"{_BNA_BASE}/consultaDatos").mock(
        return_value=httpx.Response(200, json=payload),
    )

    async with ChileDGAConnector() as conn:
        chunk = await conn.fetch_observations(
            "chile_dga:05410001",
            start=datetime(2024, 1, 1, tzinfo=UTC),
            end=datetime(2024, 1, 5, tzinfo=UTC),
        )

    assert chunk.observations[0].quality == QualityFlag.GOOD
    assert chunk.observations[1].quality == QualityFlag.SUSPECT
    assert chunk.observations[2].quality == QualityFlag.ESTIMATED
    assert chunk.observations[3].quality == QualityFlag.RAW  # unmapped -> RAW
    assert chunk.observations[4].quality == QualityFlag.RAW  # None -> RAW


# =====================================================================
# Date format in request
# =====================================================================


@pytest.mark.asyncio
@respx.mock
async def test_date_format_in_bna_request():
    """Verify date params use YYYY-MM-DD format in BNA requests."""
    route = respx.get(f"{_BNA_BASE}/consultaDatos").mock(
        return_value=httpx.Response(200, json={"datos": []}),
    )

    async with ChileDGAConnector() as conn:
        await conn.fetch_observations(
            "chile_dga:05410001",
            start=datetime(2024, 1, 15, tzinfo=UTC),
            end=datetime(2024, 12, 25, tzinfo=UTC),
        )

    assert route.called
    url_str = str(route.calls[0].request.url)
    assert "2024-01-15" in url_str
    assert "2024-12-25" in url_str


# =====================================================================
# Accept header
# =====================================================================


@pytest.mark.asyncio
@respx.mock
async def test_connector_sets_json_accept_header():
    """Verify the connector sets Accept: application/json header."""
    route = respx.get(f"{_BNA_BASE}/consultaEstaciones").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_BNA),
    )

    async with ChileDGAConnector() as conn:
        await conn.fetch_stations()

    request = route.calls[0].request
    assert request.headers["accept"] == "application/json"


# =====================================================================
# Timestamp parsing
# =====================================================================


@pytest.mark.asyncio
@respx.mock
async def test_various_timestamp_formats():
    """Parser handles ISO variants and date-only strings."""
    payload = {
        "datos": [
            {"fecha": "2024-06-01T12:00:00Z", "valor": 1.0, "calidad": "bueno"},
            {"fecha": "2024-06-02 08:30:00", "valor": 2.0, "calidad": "bueno"},
            {"fecha": "2024-06-03", "valor": 3.0, "calidad": "bueno"},
            {"fecha": "", "valor": 4.0, "calidad": "bueno"},
        ],
    }
    respx.get(f"{_BNA_BASE}/consultaDatos").mock(
        return_value=httpx.Response(200, json=payload),
    )

    async with ChileDGAConnector() as conn:
        chunk = await conn.fetch_observations(
            "chile_dga:05410001",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 3, tzinfo=UTC),
        )

    # Empty fecha is skipped
    assert len(chunk.observations) == 3
    assert chunk.observations[0].timestamp.year == 2024
    assert chunk.observations[1].timestamp.hour == 8
    assert chunk.observations[2].timestamp.day == 3


# =====================================================================
# Registration
# =====================================================================


def test_connector_registered():
    """ChileDGAConnector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("chile_dga")
    assert cls is ChileDGAConnector


def test_connector_class_attributes():
    """Verify slug, display_name, and country_codes."""
    assert ChileDGAConnector.slug == "chile_dga"
    assert ChileDGAConnector.country_codes == ["CL"]
    assert "DGA" in ChileDGAConnector.display_name


# =====================================================================
# fetch_latest
# =====================================================================


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest_uses_last_24h():
    """fetch_latest delegates to fetch_observations with a 24 h window."""
    route = respx.get(f"{_BNA_BASE}/consultaDatos").mock(
        return_value=httpx.Response(200, json={"datos": []}),
    )

    async with ChileDGAConnector() as conn:
        chunk = await conn.fetch_latest("chile_dga:05410001")

    assert route.called
    assert chunk.station_id == "chile_dga:05410001"
    assert len(chunk.observations) == 0


# =====================================================================
# Stations with missing codigo
# =====================================================================


@pytest.mark.asyncio
@respx.mock
async def test_stations_skip_missing_codigo():
    """Stations with empty or missing codigo are skipped."""
    records = [
        {"codigo": "", "nombre": "NO ID", "latitud": -33.0, "longitud": -70.0, "vigente": True},
        {"nombre": "ALSO NO ID", "latitud": -33.0, "longitud": -70.0, "vigente": True},
    ]
    respx.get(f"{_BNA_BASE}/consultaEstaciones").mock(
        return_value=httpx.Response(200, json=records),
    )

    async with ChileDGAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0
