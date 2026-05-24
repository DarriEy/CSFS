"""Tests for Peru SENAMHI connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.peru_senamhi import PeruSENAMHIConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import QualityFlag

# ── Mock payloads ───────────────────────────────────────────────────

MOCK_STATIONS = [
    {
        "codigo": "230601",
        "nombre": "CHOSICA",
        "latitud": -11.95,
        "longitud": -76.69,
        "departamento": "Lima",
        "rio": "RIMAC",
        "cuenca": "RIMAC",
    },
    {
        "codigo": "230602",
        "nombre": "SHEQUE",
        "latitud": -11.75,
        "longitud": -76.50,
        "departamento": "Lima",
        "rio": "RIMAC",
        "cuenca": "RIMAC",
    },
    {
        "codigo": "230603",
        "nombre": "INCOMPLETE STATION",
        "latitud": None,
        "longitud": None,
        "departamento": "Lima",
        "rio": "X",
        "cuenca": "X",
    },
]

MOCK_OBSERVATIONS = {
    "datos": [
        {
            "fecha": "2024-06-01T12:00:00",
            "valor": 12.5,
            "calidad": "bueno",
        },
        {
            "fecha": "2024-06-02T12:00:00",
            "valor": 15.3,
            "calidad": "dudoso",
        },
        {
            "fecha": "2024-06-03T12:00:00",
            "valor": 18.1,
            "calidad": "estimado",
        },
        {
            "fecha": "2024-06-04T12:00:00",
            "valor": None,
            "calidad": "bueno",
        },
    ],
}

# ── Base URLs ───────────────────────────────────────────────────────

_BASE = "https://www.senamhi.gob.pe"


# =====================================================================
# Station tests
# =====================================================================


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_sea():
    """Stations are parsed correctly from SEA JSON array response."""
    respx.get(f"{_BASE}/site/sea/www/estaciones").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS),
    )

    async with PeruSENAMHIConnector() as conn:
        stations = await conn.fetch_stations()

    # Third station has None lat/lon and should be skipped
    assert len(stations) == 2

    s0 = stations[0]
    assert s0.id == "peru_senamhi:230601"
    assert s0.native_id == "230601"
    assert s0.name == "CHOSICA"
    assert s0.latitude == pytest.approx(-11.95)
    assert s0.longitude == pytest.approx(-76.69)
    assert s0.river == "RIMAC"
    assert s0.country_code == "PE"
    assert s0.provider == "peru_senamhi"

    assert stations[1].native_id == "230602"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fallback_to_mapas():
    """When SEA station endpoint fails, connector falls back to mapas."""
    respx.get(f"{_BASE}/site/sea/www/estaciones").mock(
        return_value=httpx.Response(500),
    )
    respx.get(f"{_BASE}/mapas/mapa-estaciones/_dato_esta_tipo.php").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS[:2]),
    )

    async with PeruSENAMHIConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    assert stations[0].id == "peru_senamhi:230601"
    assert stations[0].name == "CHOSICA"


# =====================================================================
# Observation tests
# =====================================================================


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_sea():
    """Observations are parsed correctly from SEA JSON response."""
    respx.get(f"{_BASE}/site/sea/www/datos").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS),
    )

    async with PeruSENAMHIConnector() as conn:
        chunk = await conn.fetch_observations(
            "peru_senamhi:230601",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 4, tzinfo=UTC),
        )

    assert chunk.station_id == "peru_senamhi:230601"
    assert chunk.provider == "peru_senamhi"
    assert len(chunk.observations) == 4

    assert chunk.observations[0].discharge_m3s == pytest.approx(12.5)
    assert chunk.observations[0].quality == QualityFlag.GOOD

    assert chunk.observations[1].discharge_m3s == pytest.approx(15.3)
    assert chunk.observations[1].quality == QualityFlag.SUSPECT

    assert chunk.observations[2].discharge_m3s == pytest.approx(18.1)
    assert chunk.observations[2].quality == QualityFlag.ESTIMATED

    # None discharge -> MISSING
    assert chunk.observations[3].discharge_m3s is None
    assert chunk.observations[3].quality == QualityFlag.MISSING


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_fallback_to_mapas():
    """When SEA data endpoint fails, connector falls back to mapas."""
    respx.get(f"{_BASE}/site/sea/www/datos").mock(
        return_value=httpx.Response(500),
    )
    mapas_payload = {
        "datos": [
            {
                "fecha": "2024-06-01T12:00:00",
                "valor": 12.5,
                "calidad": "bueno",
            },
        ],
    }
    respx.get(
        f"{_BASE}/mapas/mapa-estaciones/_dato_esta_datos.php",
    ).mock(
        return_value=httpx.Response(200, json=mapas_payload),
    )

    async with PeruSENAMHIConnector() as conn:
        chunk = await conn.fetch_observations(
            "peru_senamhi:230601",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 1, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(12.5)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty():
    """Empty datos array produces zero observations."""
    respx.get(f"{_BASE}/site/sea/www/datos").mock(
        return_value=httpx.Response(200, json={"datos": []}),
    )

    async with PeruSENAMHIConnector() as conn:
        chunk = await conn.fetch_observations(
            "peru_senamhi:230601",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 1, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0
    assert chunk.station_id == "peru_senamhi:230601"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_invalid_json():
    """Non-JSON body raises DataFormatError."""
    respx.get(f"{_BASE}/site/sea/www/datos").mock(
        return_value=httpx.Response(200, text="<html>Error</html>"),
    )

    async with PeruSENAMHIConnector() as conn:
        with pytest.raises(DataFormatError, match="Invalid JSON"):
            await conn.fetch_observations(
                "peru_senamhi:230601",
                start=datetime(2024, 6, 1, tzinfo=UTC),
                end=datetime(2024, 6, 1, tzinfo=UTC),
            )


@pytest.mark.asyncio
@respx.mock
async def test_date_format_in_sea_request():
    """Verify date params use YYYY/MM/DD format in SEA requests."""
    route = respx.get(f"{_BASE}/site/sea/www/datos").mock(
        return_value=httpx.Response(200, json={"datos": []}),
    )

    async with PeruSENAMHIConnector() as conn:
        await conn.fetch_observations(
            "peru_senamhi:230601",
            start=datetime(2024, 1, 15, tzinfo=UTC),
            end=datetime(2024, 12, 25, tzinfo=UTC),
        )

    assert route.called
    url_str = str(route.calls[0].request.url)
    assert "2024%2F01%2F15" in url_str or "2024/01/15" in url_str
    assert "2024%2F12%2F25" in url_str or "2024/12/25" in url_str


# =====================================================================
# Registration and class attributes
# =====================================================================


def test_connector_registered():
    """PeruSENAMHIConnector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("peru_senamhi")
    assert cls is PeruSENAMHIConnector


def test_connector_class_attributes():
    """Verify slug, display_name, and country_codes."""
    assert PeruSENAMHIConnector.slug == "peru_senamhi"
    assert PeruSENAMHIConnector.country_codes == ["PE"]
    assert "SENAMHI" in PeruSENAMHIConnector.display_name


# =====================================================================
# Stations with missing codigo
# =====================================================================


@pytest.mark.asyncio
@respx.mock
async def test_stations_skip_missing_codigo():
    """Stations with empty or missing codigo are skipped."""
    records = [
        {
            "codigo": "",
            "nombre": "NO ID",
            "latitud": -12.0,
            "longitud": -77.0,
        },
        {
            "nombre": "ALSO NO ID",
            "latitud": -12.0,
            "longitud": -77.0,
        },
    ]
    respx.get(f"{_BASE}/site/sea/www/estaciones").mock(
        return_value=httpx.Response(200, json=records),
    )

    async with PeruSENAMHIConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0
