"""Tests for the Colombia IDEAM Socrata connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.colombia_ideam import ColombiaIDEAMConnector
from csfs.core.exceptions import DataFormatError

IDEAM_BASE = "https://www.datos.gov.co/resource"

MOCK_STATIONS_RESPONSE = [
    {
        "codigoestacion": "21017010",
        "nombreestacion": "PUENTE LLERAS",
        "latitud": "7.065",
        "longitud": "-73.854",
        "corriente": "RIO MAGDALENA",
        "areacuenca": "18500",
    },
    {
        "codigoestacion": "23037030",
        "nombreestacion": "ARRANCAPLUMAS",
        "latitud": "5.208",
        "longitud": "-74.735",
        "corriente": "RIO BOGOTA",
        "areacuenca": "5671",
    },
    {
        "codigoestacion": "28017040",
        "nombreestacion": "LA VIRGINIA",
        "latitud": "4.899",
        "longitud": "-75.882",
        "corriente": "RIO CAUCA",
    },
]

MOCK_OBSERVATIONS_RESPONSE = [
    {
        "codigoestacion": "21017010",
        "fechaobservacion": "2024-06-01T06:00:00",
        "valorobservado": "450.3",
    },
    {
        "codigoestacion": "21017010",
        "fechaobservacion": "2024-06-01T12:00:00",
        "valorobservado": "463.8",
    },
    {
        "codigoestacion": "21017010",
        "fechaobservacion": "2024-06-01T18:00:00",
        "valorobservado": None,
    },
]

MOCK_EMPTY_OBSERVATIONS: list[dict] = []


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_all():
    """All stations in the Socrata response are returned."""
    respx.get(f"{IDEAM_BASE}/hp9r-jxuu.json").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with ColombiaIDEAMConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 3
    native_ids = {s.native_id for s in stations}
    assert native_ids == {"21017010", "23037030", "28017040"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_field_mapping():
    """Station fields are correctly mapped from the Socrata response."""
    respx.get(f"{IDEAM_BASE}/hp9r-jxuu.json").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with ColombiaIDEAMConnector() as conn:
        stations = await conn.fetch_stations()

    puente = next(s for s in stations if s.native_id == "21017010")
    assert puente.id == "colombia_ideam:21017010"
    assert puente.provider == "colombia_ideam"
    assert puente.name == "PUENTE LLERAS"
    assert puente.latitude == pytest.approx(7.065)
    assert puente.longitude == pytest.approx(-73.854)
    assert puente.country_code == "CO"
    assert puente.river == "RIO MAGDALENA"
    assert puente.catchment_area_km2 == pytest.approx(18500.0)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty station list returns no stations."""
    respx.get(f"{IDEAM_BASE}/hp9r-jxuu.json").mock(
        return_value=httpx.Response(200, json=[])
    )

    async with ColombiaIDEAMConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_missing_code():
    """Stations without a 'codigoestacion' field are silently skipped."""
    data = [
        {"nombreestacion": "NO CODE", "latitud": "5.0", "longitud": "-74.0"},
        {
            "codigoestacion": "99999",
            "nombreestacion": "VALID",
            "latitud": "5.1",
            "longitud": "-74.1",
        },
    ]
    respx.get(f"{IDEAM_BASE}/hp9r-jxuu.json").mock(
        return_value=httpx.Response(200, json=data)
    )

    async with ColombiaIDEAMConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "99999"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_json():
    """Observations are correctly parsed from the Socrata response."""
    respx.get(f"{IDEAM_BASE}/sbwg-7ju4.json").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE)
    )

    async with ColombiaIDEAMConnector() as conn:
        chunk = await conn.fetch_observations(
            "colombia_ideam:21017010",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.station_id == "colombia_ideam:21017010"
    assert chunk.provider == "colombia_ideam"
    assert len(chunk.observations) == 3

    assert chunk.observations[0].discharge_m3s == pytest.approx(450.3)
    assert chunk.observations[0].quality.value == "raw"

    assert chunk.observations[1].discharge_m3s == pytest.approx(463.8)

    # Third observation has None value => MISSING
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty_response():
    """An empty observation array returns zero observations."""
    respx.get(f"{IDEAM_BASE}/sbwg-7ju4.json").mock(
        return_value=httpx.Response(200, json=MOCK_EMPTY_OBSERVATIONS)
    )

    async with ColombiaIDEAMConnector() as conn:
        chunk = await conn.fetch_observations(
            "colombia_ideam:21017010",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0
    assert chunk.station_id == "colombia_ideam:21017010"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_invalid_timestamp():
    """Invalid timestamps raise DataFormatError."""
    data = [
        {
            "codigoestacion": "21017010",
            "fechaobservacion": "not-a-date",
            "valorobservado": "100",
        },
    ]
    respx.get(f"{IDEAM_BASE}/sbwg-7ju4.json").mock(
        return_value=httpx.Response(200, json=data)
    )

    async with ColombiaIDEAMConnector() as conn:
        with pytest.raises(DataFormatError, match="Invalid timestamp"):
            await conn.fetch_observations(
                "colombia_ideam:21017010",
                start=datetime(2024, 6, 1),
                end=datetime(2024, 6, 2),
            )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_sends_soda_params():
    """Verify the SODA query parameters sent for observations."""
    route = respx.get(f"{IDEAM_BASE}/sbwg-7ju4.json").mock(
        return_value=httpx.Response(200, json=MOCK_EMPTY_OBSERVATIONS)
    )

    async with ColombiaIDEAMConnector() as conn:
        await conn.fetch_observations(
            "colombia_ideam:21017010",
            start=datetime(2024, 1, 15, 0, 0),
            end=datetime(2024, 12, 25, 23, 59),
        )

    assert route.called
    url = str(route.calls[0].request.url)
    assert "codigoestacion" in url
    assert "21017010" in url
    assert "fechaobservacion" in url
    assert "%24order" in url or "$order" in url


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_with_app_token():
    """When app_token is configured, it is sent as $$app_token."""
    route = respx.get(f"{IDEAM_BASE}/hp9r-jxuu.json").mock(
        return_value=httpx.Response(200, json=[])
    )

    async with ColombiaIDEAMConnector(config={"app_token": "my-token"}) as conn:
        await conn.fetch_stations()

    assert route.called
    url = str(route.calls[0].request.url)
    assert "app_token" in url
    assert "my-token" in url


def test_connector_is_registered():
    """The connector registers itself under the expected slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("colombia_ideam")
    assert cls is ColombiaIDEAMConnector


def test_connector_metadata():
    """Verify class-level attributes."""
    assert ColombiaIDEAMConnector.slug == "colombia_ideam"
    assert ColombiaIDEAMConnector.country_codes == ["CO"]
    assert "datos.gov.co" in ColombiaIDEAMConnector.base_url
