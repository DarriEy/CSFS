"""Tests for the France Hub'Eau Hydrométrie connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.france_hubeau import FranceHubEauConnector

MOCK_STATIONS_RESPONSE = {
    "data": [
        {
            "code_station": "V7144010",
            "libelle_station": "Le Rhône à Beaucaire",
            "latitude_station": 43.8,
            "longitude_station": 4.65,
            "libelle_cours_eau": "Le Rhône",
        },
        {
            "code_station": "O9724010",
            "libelle_station": "La Garonne à Tonneins",
            "latitude_station": 44.39,
            "longitude_station": 0.31,
            "libelle_cours_eau": "La Garonne",
        },
        {
            "code_station": "",
            "latitude_station": 48.0,
            "longitude_station": 2.0,
        },
    ],
}

MOCK_STATIONS_PAGE_1 = {
    "data": [
        {
            "code_station": f"S{i:07d}",
            "libelle_station": f"Station {i}",
            "latitude_station": 43.0 + i * 0.01,
            "longitude_station": 2.0 + i * 0.01,
        }
        for i in range(200)
    ],
}

MOCK_STATIONS_PAGE_2 = {
    "data": [
        {
            "code_station": "S9999999",
            "libelle_station": "Last Station",
            "latitude_station": 48.0,
            "longitude_station": 3.0,
        },
    ],
}

MOCK_OBS_RESPONSE = {
    "data": [
        {
            "date_obs": "2024-06-01T06:00:00Z",
            "resultat_obs": 1250.0,
            "code_qualification_obs": 16,
        },
        {
            "date_obs": "2024-06-01T07:00:00Z",
            "resultat_obs": 1300.0,
            "code_qualification_obs": 12,
        },
        {
            "date_obs": "2024-06-01T08:00:00Z",
            "resultat_obs": None,
            "code_qualification_obs": 4,
        },
    ],
    "next": None,
}

MOCK_OBS_PAGINATED_P1 = {
    "data": [
        {
            "date_obs": "2024-06-01T06:00:00Z",
            "resultat_obs": 1250.0,
            "code_qualification_obs": 16,
        },
    ],
    "next": "cursor-abc123",
}

MOCK_OBS_PAGINATED_P2 = {
    "data": [
        {
            "date_obs": "2024-06-01T07:00:00Z",
            "resultat_obs": 1300.0,
            "code_qualification_obs": 8,
        },
    ],
    "next": None,
}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_correctly():
    respx.get("https://hubeau.eaufrance.fr/api/v2/hydrometrie/referentiel/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with FranceHubEauConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    rhone = next(s for s in stations if s.native_id == "V7144010")
    assert rhone.id == "france_hubeau:V7144010"
    assert rhone.provider == "france_hubeau"
    assert rhone.name == "Le Rhône à Beaucaire"
    assert rhone.country_code == "FR"
    assert rhone.river == "Le Rhône"
    assert rhone.latitude == pytest.approx(43.8)
    assert rhone.longitude == pytest.approx(4.65)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_incomplete():
    respx.get("https://hubeau.eaufrance.fr/api/v2/hydrometrie/referentiel/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with FranceHubEauConnector() as conn:
        stations = await conn.fetch_stations()

    assert not any(s.native_id == "" for s in stations)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_pagination():
    route = respx.get("https://hubeau.eaufrance.fr/api/v2/hydrometrie/referentiel/stations")
    route.side_effect = [
        httpx.Response(200, json=MOCK_STATIONS_PAGE_1),
        httpx.Response(200, json=MOCK_STATIONS_PAGE_2),
    ]

    async with FranceHubEauConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 201
    assert route.call_count == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_empty():
    respx.get("https://hubeau.eaufrance.fr/api/v2/hydrometrie/referentiel/stations").mock(
        return_value=httpx.Response(200, json={"data": []})
    )

    async with FranceHubEauConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_with_quality():
    respx.get("https://hubeau.eaufrance.fr/api/v2/hydrometrie/observations_tr").mock(
        return_value=httpx.Response(200, json=MOCK_OBS_RESPONSE)
    )

    async with FranceHubEauConnector() as conn:
        chunk = await conn.fetch_observations(
            "france_hubeau:V7144010",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "france_hubeau"
    assert chunk.station_id == "france_hubeau:V7144010"
    assert len(chunk.observations) == 3

    assert chunk.observations[0].discharge_m3s == pytest.approx(1.25)
    assert chunk.observations[0].quality.value == "good"

    assert chunk.observations[1].discharge_m3s == pytest.approx(1.3)
    assert chunk.observations[1].quality.value == "suspect"

    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_cursor_pagination():
    route = respx.get("https://hubeau.eaufrance.fr/api/v2/hydrometrie/observations_tr")
    route.side_effect = [
        httpx.Response(200, json=MOCK_OBS_PAGINATED_P1),
        httpx.Response(200, json=MOCK_OBS_PAGINATED_P2),
    ]

    async with FranceHubEauConnector() as conn:
        chunk = await conn.fetch_observations(
            "france_hubeau:V7144010",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 2
    assert route.call_count == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty():
    respx.get("https://hubeau.eaufrance.fr/api/v2/hydrometrie/observations_tr").mock(
        return_value=httpx.Response(200, json={"data": [], "next": None})
    )

    async with FranceHubEauConnector() as conn:
        chunk = await conn.fetch_observations(
            "france_hubeau:V7144010",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_litre_to_m3s_conversion():
    """Hub'Eau returns L/s — connector divides by 1000 to get m3/s."""
    respx.get("https://hubeau.eaufrance.fr/api/v2/hydrometrie/observations_tr").mock(
        return_value=httpx.Response(200, json={
            "data": [{
                "date_obs": "2024-06-01T06:00:00Z",
                "resultat_obs": 5000.0,
                "code_qualification_obs": 20,
            }],
            "next": None,
        })
    )

    async with FranceHubEauConnector() as conn:
        chunk = await conn.fetch_observations(
            "france_hubeau:V7144010",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.observations[0].discharge_m3s == pytest.approx(5.0)
