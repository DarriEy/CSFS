"""Tests for the France Hub'Eau connector."""
from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx

from csfs.connectors.france_hubeau import FranceHubEauConnector
from csfs.core.models import QualityFlag

BASE = "https://hubeau.eaufrance.fr/api/v2/hydrometrie"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_and_paginates():
    page1 = {
        "data": [
            {
                "code_station": "A001",
                "libelle_station": "Station One",
                "latitude_station": 48.0,
                "longitude_station": 2.0,
                "libelle_cours_eau": "Seine",
            },
        ],
    }
    page2: dict = {"data": []}
    route = respx.get(f"{BASE}/referentiel/stations")
    route.side_effect = [
        httpx.Response(200, json=page1),
        httpx.Response(200, json=page2),
    ]

    async with FranceHubEauConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].id == "france_hubeau:A001"
    assert stations[0].name == "Station One"
    assert stations[0].latitude == 48.0
    assert stations[0].river == "Seine"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_recent_uses_realtime():
    """A window within the real-time depth is served from observations_tr."""
    obs_page = {
        "data": [
            {
                "date_obs": "2024-01-01T00:00:00Z",
                "resultat_obs": 1500.0,
                "code_qualification_obs": 16,
            },
            {
                "date_obs": "2024-01-01T00:05:00Z",
                "resultat_obs": 1600.0,
                "code_qualification_obs": 20,
            },
        ],
        "next": None,
    }
    tr_route = respx.get(f"{BASE}/observations_tr").mock(
        return_value=httpx.Response(200, json=obs_page),
    )
    elab_route = respx.get(f"{BASE}/obs_elab")

    now = datetime.now(UTC)
    async with FranceHubEauConnector() as conn:
        chunk = await conn.fetch_observations(
            "france_hubeau:A001",
            start=now - timedelta(days=2),
            end=now,
        )

    assert tr_route.called
    assert not elab_route.called  # recent window must not hit the historical API
    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == 1.5
    assert chunk.observations[0].quality == QualityFlag.GOOD
    assert chunk.observations[1].discharge_m3s == 1.6


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_old_uses_elaborated():
    """A window older than the real-time depth is served from obs_elab.

    Uses the obs_elab field names (resultat_obs_elab, date_obs_elab,
    code_qualification) and daily granularity.
    """
    elab_page = {
        "data": [
            {
                "date_obs_elab": "2020-06-01",
                "resultat_obs_elab": 28858.0,
                "code_qualification": 20,
                "grandeur_hydro_elab": "QmnJ",
            },
            {
                "date_obs_elab": "2020-06-02",
                "resultat_obs_elab": 27000.0,
                "code_qualification": 12,
                "grandeur_hydro_elab": "QmnJ",
            },
        ],
        "next": None,
    }
    elab_route = respx.get(f"{BASE}/obs_elab").mock(
        return_value=httpx.Response(200, json=elab_page),
    )
    tr_route = respx.get(f"{BASE}/observations_tr")

    async with FranceHubEauConnector() as conn:
        chunk = await conn.fetch_observations(
            "france_hubeau:A001",
            start=datetime(2020, 6, 1, tzinfo=UTC),
            end=datetime(2020, 6, 30, tzinfo=UTC),
        )

    assert elab_route.called
    assert not tr_route.called  # old window must not hit the real-time API
    assert len(chunk.observations) == 2
    # resultat is in L/s; connector converts to m3/s.
    assert chunk.observations[0].discharge_m3s == pytest.approx(28.858)
    assert chunk.observations[0].quality == QualityFlag.GOOD
    assert chunk.observations[1].quality == QualityFlag.SUSPECT


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_spanning_window_hits_both_endpoints():
    """A window straddling the cutoff queries both endpoints and merges them."""
    elab_page = {
        "data": [
            {
                "date_obs_elab": "2024-01-01",
                "resultat_obs_elab": 1000.0,
                "code_qualification": 20,
            },
        ],
        "next": None,
    }
    now = datetime.now(UTC)
    tr_page = {
        "data": [
            {
                "date_obs": now.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "resultat_obs": 2000.0,
                "code_qualification_obs": 16,
            },
        ],
        "next": None,
    }
    elab_route = respx.get(f"{BASE}/obs_elab").mock(
        return_value=httpx.Response(200, json=elab_page),
    )
    tr_route = respx.get(f"{BASE}/observations_tr").mock(
        return_value=httpx.Response(200, json=tr_page),
    )

    async with FranceHubEauConnector() as conn:
        chunk = await conn.fetch_observations(
            "france_hubeau:A001",
            start=datetime(2024, 1, 1, tzinfo=UTC),
            end=now,
        )

    assert elab_route.called
    assert tr_route.called
    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(1.0)
    assert chunk.observations[1].discharge_m3s == pytest.approx(2.0)
