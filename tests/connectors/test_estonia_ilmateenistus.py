"""Tests for the Estonia Ilmateenistus hydrology connector with mocked HTTP."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.estonia_ilmateenistus import EstoniaIlmateenistusConnector
from csfs.core.exceptions import ConnectorError

ILMA_BASE = "https://www.ilmateenistus.ee"

MOCK_STATIONS_JSON = [
    {
        "code": "EE001",
        "name": "Tartu - Emajogi",
        "latitude": 58.378,
        "longitude": 26.729,
        "waterBody": "Emajogi",
    },
    {
        "code": "EE002",
        "name": "Narva - Narva",
        "latitude": 59.379,
        "longitude": 28.190,
        "waterBody": "Narva",
    },
]

MOCK_STATIONS_HTML = """
<html><body>
<div class="stations-list">
  <a data-code="EE101" data-name="Parnu jogi" data-lat="58.385"
     data-lon="24.497" data-water="Parnu">Parnu jogi</a>
  <a data-code="EE102" data-name="Kasari" data-lat="58.741"
     data-lon="24.244" data-water="Kasari">Kasari</a>
</div>
</body></html>
"""

MOCK_OBS_CSV = """date,flow,water_level
2024-06-01,12.5,1.32
2024-06-02,13.1,1.35
2024-06-03,,1.28
2024-06-04,11.8,1.30
"""

MOCK_OBS_CSV_NO_HEADER = """2024-06-01,12.5,1.32
2024-06-02,13.1,1.35
"""

MOCK_OBS_JSON = {
    "observations": [
        {"date": "2024-06-01T00:00:00+00:00", "flow": 12.5},
        {"date": "2024-06-02T00:00:00+00:00", "flow": 13.1},
        {"date": "2024-06-03T00:00:00+00:00", "flow": None},
    ],
}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_json():
    """Stations parsed from a JSON endpoint."""
    respx.get(
        f"{ILMA_BASE}/ilm/ilmavaatlused/vaatlusandmed/json/hydro",
    ).mock(return_value=httpx.Response(200, json=MOCK_STATIONS_JSON))

    async with EstoniaIlmateenistusConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"EE001", "EE002"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_field_mapping():
    """Station fields are correctly mapped from a JSON response."""
    respx.get(
        f"{ILMA_BASE}/ilm/ilmavaatlused/vaatlusandmed/json/hydro",
    ).mock(return_value=httpx.Response(200, json=MOCK_STATIONS_JSON))

    async with EstoniaIlmateenistusConnector() as conn:
        stations = await conn.fetch_stations()

    tartu = next(s for s in stations if s.native_id == "EE001")
    assert tartu.id == "estonia_ilmateenistus:EE001"
    assert tartu.provider == "estonia_ilmateenistus"
    assert tartu.name == "Tartu - Emajogi"
    assert tartu.latitude == pytest.approx(58.378)
    assert tartu.longitude == pytest.approx(26.729)
    assert tartu.country_code == "EE"
    assert tartu.river == "Emajogi"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_html_fallback():
    """Stations are scraped from HTML when JSON endpoints fail."""
    # All JSON endpoints return 404
    for path in EstoniaIlmateenistusConnector._STATION_JSON_PATHS:
        respx.get(f"{ILMA_BASE}{path}").mock(
            return_value=httpx.Response(404),
        )

    respx.get(f"{ILMA_BASE}/siseveed/").mock(
        return_value=httpx.Response(200, text=MOCK_STATIONS_HTML),
    )

    async with EstoniaIlmateenistusConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    parnu = next(s for s in stations if s.native_id == "EE101")
    assert parnu.name == "Parnu jogi"
    assert parnu.latitude == pytest.approx(58.385)
    assert parnu.river == "Parnu"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_all_fail_raises():
    """ConnectorError is raised when all station endpoints fail."""
    for path in EstoniaIlmateenistusConnector._STATION_JSON_PATHS:
        respx.get(f"{ILMA_BASE}{path}").mock(
            return_value=httpx.Response(500),
        )
    respx.get(f"{ILMA_BASE}/siseveed/").mock(
        return_value=httpx.Response(500),
    )

    async with EstoniaIlmateenistusConnector() as conn:
        with pytest.raises(ConnectorError, match="estonia_ilmateenistus"):
            await conn.fetch_stations()


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_csv():
    """Observations parsed from CSV with header row."""
    respx.get(f"{ILMA_BASE}/siseveed/data/EE001").mock(
        return_value=httpx.Response(
            200,
            text=MOCK_OBS_CSV,
            headers={"content-type": "text/csv"},
        ),
    )

    async with EstoniaIlmateenistusConnector() as conn:
        chunk = await conn.fetch_observations(
            "estonia_ilmateenistus:EE001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 4),
        )

    assert chunk.station_id == "estonia_ilmateenistus:EE001"
    assert chunk.provider == "estonia_ilmateenistus"
    assert len(chunk.observations) == 4

    # First obs has flow=12.5
    assert chunk.observations[0].discharge_m3s == pytest.approx(12.5)
    assert chunk.observations[0].quality.value == "raw"

    # Third obs has no flow but has level -> falls back to level
    assert chunk.observations[2].discharge_m3s == pytest.approx(1.28)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_json_response():
    """Observations parsed from JSON when content-type is application/json."""
    respx.get(f"{ILMA_BASE}/siseveed/data/EE001").mock(
        return_value=httpx.Response(
            200,
            json=MOCK_OBS_JSON,
            headers={"content-type": "application/json"},
        ),
    )

    async with EstoniaIlmateenistusConnector() as conn:
        chunk = await conn.fetch_observations(
            "estonia_ilmateenistus:EE001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 3),
        )

    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(12.5)
    # Third obs has flow=None -> MISSING
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_all_fail_raises():
    """ConnectorError when all observation endpoints fail."""
    for path_tpl in EstoniaIlmateenistusConnector._OBS_CSV_PATHS:
        path = path_tpl.format(station_code="EE999")
        respx.get(f"{ILMA_BASE}{path}").mock(
            return_value=httpx.Response(500),
        )

    async with EstoniaIlmateenistusConnector() as conn:
        with pytest.raises(ConnectorError, match="EE999"):
            await conn.fetch_observations(
                "estonia_ilmateenistus:EE999",
                start=datetime(2024, 6, 1),
                end=datetime(2024, 6, 2),
            )


def test_connector_is_registered():
    """The connector registers itself under the expected slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("estonia_ilmateenistus")
    assert cls is EstoniaIlmateenistusConnector


def test_connector_metadata():
    """Verify class-level attributes."""
    assert EstoniaIlmateenistusConnector.slug == "estonia_ilmateenistus"
    assert EstoniaIlmateenistusConnector.country_codes == ["EE"]
    assert "ilmateenistus" in EstoniaIlmateenistusConnector.base_url
