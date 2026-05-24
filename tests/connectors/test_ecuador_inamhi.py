"""Tests for Ecuador INAMHI (GEOGloWS) connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.ecuador_inamhi import EcuadorINAMHIConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import QualityFlag

# ── Mock payloads ───────────────────────────────────────────────────

MOCK_HISTORIC = {
    "data": [
        {
            "datetime": "2024-06-01T00:00:00",
            "streamflow_m3s": 350.2,
        },
        {
            "datetime": "2024-06-02T00:00:00",
            "streamflow_m3s": 375.8,
        },
        {
            "datetime": "2024-06-03T00:00:00",
            "streamflow_m3s": 400.1,
        },
        {
            "datetime": "2024-05-30T00:00:00",
            "streamflow_m3s": 310.0,
        },
    ],
}

MOCK_FORECAST = {
    "data": [
        {
            "datetime": "2024-06-10T00:00:00",
            "flow_avg": 420.5,
        },
        {
            "datetime": "2024-06-11T00:00:00",
            "flow_avg": 415.0,
        },
    ],
}

# ── Base URLs ───────────────────────────────────────────────────────

_BASE = "https://geoglows.ecmwf.int/api"
_TETHYS_BASE = "https://inamhi.geoglows.org/api"


# =====================================================================
# Station tests
# =====================================================================


@pytest.mark.asyncio
async def test_fetch_stations_returns_seed_list():
    """fetch_stations returns the curated seed list of Ecuadorian reaches."""
    async with EcuadorINAMHIConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 20

    s0 = stations[0]
    assert s0.id == "ecuador_inamhi:9027406"
    assert s0.native_id == "9027406"
    assert s0.name == "Guayas at Daule"
    assert s0.latitude == pytest.approx(-1.86)
    assert s0.longitude == pytest.approx(-79.97)
    assert s0.river == "Guayas"
    assert s0.country_code == "EC"
    assert s0.provider == "ecuador_inamhi"
    assert s0.is_active is True


@pytest.mark.asyncio
async def test_fetch_stations_all_have_valid_ids():
    """Every seed station has a non-empty COMID and valid coordinates."""
    async with EcuadorINAMHIConnector() as conn:
        stations = await conn.fetch_stations()

    for s in stations:
        assert s.native_id
        assert s.latitude is not None
        assert s.longitude is not None
        assert s.id.startswith("ecuador_inamhi:")


# =====================================================================
# Historic observation tests
# =====================================================================


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_geoglows():
    """Observations are parsed and filtered from GEOGloWS response."""
    respx.get(f"{_BASE}/HistoricSimulation/").mock(
        return_value=httpx.Response(200, json=MOCK_HISTORIC),
    )

    async with EcuadorINAMHIConnector() as conn:
        chunk = await conn.fetch_observations(
            "ecuador_inamhi:9027406",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 3, tzinfo=UTC),
        )

    assert chunk.station_id == "ecuador_inamhi:9027406"
    assert chunk.provider == "ecuador_inamhi"
    # Only 3 obs in range (2024-05-30 is out of range)
    assert len(chunk.observations) == 3

    assert chunk.observations[0].discharge_m3s == pytest.approx(350.2)
    assert chunk.observations[0].quality == QualityFlag.ESTIMATED

    assert chunk.observations[1].discharge_m3s == pytest.approx(375.8)
    assert chunk.observations[2].discharge_m3s == pytest.approx(400.1)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_fallback_to_tethys():
    """When GEOGloWS fails, connector falls back to Tethys portal."""
    respx.get(f"{_BASE}/HistoricSimulation/").mock(
        return_value=httpx.Response(500),
    )
    tethys_payload = {
        "data": [
            {
                "datetime": "2024-06-01T00:00:00",
                "streamflow_m3s": 350.2,
            },
        ],
    }
    respx.get(f"{_TETHYS_BASE}/HistoricSimulation/").mock(
        return_value=httpx.Response(200, json=tethys_payload),
    )

    async with EcuadorINAMHIConnector() as conn:
        chunk = await conn.fetch_observations(
            "ecuador_inamhi:9027406",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 1, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(350.2)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty():
    """Empty data array produces zero observations."""
    respx.get(f"{_BASE}/HistoricSimulation/").mock(
        return_value=httpx.Response(200, json={"data": []}),
    )

    async with EcuadorINAMHIConnector() as conn:
        chunk = await conn.fetch_observations(
            "ecuador_inamhi:9027406",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0
    assert chunk.station_id == "ecuador_inamhi:9027406"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_invalid_json():
    """Non-JSON body raises DataFormatError."""
    respx.get(f"{_BASE}/HistoricSimulation/").mock(
        return_value=httpx.Response(200, text="<html>Error</html>"),
    )

    async with EcuadorINAMHIConnector() as conn:
        with pytest.raises(DataFormatError, match="Invalid JSON"):
            await conn.fetch_observations(
                "ecuador_inamhi:9027406",
                start=datetime(2024, 6, 1, tzinfo=UTC),
                end=datetime(2024, 6, 1, tzinfo=UTC),
            )


# =====================================================================
# Forecast tests
# =====================================================================


@pytest.mark.asyncio
@respx.mock
async def test_fetch_forecast():
    """Forecast stats are parsed correctly."""
    respx.get(f"{_BASE}/ForecastStats/").mock(
        return_value=httpx.Response(200, json=MOCK_FORECAST),
    )

    async with EcuadorINAMHIConnector() as conn:
        chunk = await conn.fetch_forecast("ecuador_inamhi:9027406")

    assert chunk.station_id == "ecuador_inamhi:9027406"
    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(420.5)
    assert chunk.observations[1].discharge_m3s == pytest.approx(415.0)
    assert chunk.observations[0].quality == QualityFlag.ESTIMATED


# =====================================================================
# Registration and class attributes
# =====================================================================


def test_connector_registered():
    """EcuadorINAMHIConnector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("ecuador_inamhi")
    assert cls is EcuadorINAMHIConnector


def test_connector_class_attributes():
    """Verify slug, display_name, and country_codes."""
    assert EcuadorINAMHIConnector.slug == "ecuador_inamhi"
    assert EcuadorINAMHIConnector.country_codes == ["EC"]
    assert "GEOGloWS" in EcuadorINAMHIConnector.display_name


# =====================================================================
# Reach ID param in request
# =====================================================================


@pytest.mark.asyncio
@respx.mock
async def test_reach_id_in_request():
    """Verify reach_id is passed as a query parameter."""
    route = respx.get(f"{_BASE}/HistoricSimulation/").mock(
        return_value=httpx.Response(200, json={"data": []}),
    )

    async with EcuadorINAMHIConnector() as conn:
        await conn.fetch_observations(
            "ecuador_inamhi:9027406",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 3, tzinfo=UTC),
        )

    assert route.called
    url_str = str(route.calls[0].request.url)
    assert "reach_id=9027406" in url_str
    assert "return_format=json" in url_str
