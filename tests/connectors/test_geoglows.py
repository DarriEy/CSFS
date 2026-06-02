# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Tests for the GEOGLOWS connector (GEOGLOWS V2 REST API, respx-mocked)."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.geoglows import GEOGloWSConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import QualityFlag

_BASE = "https://geoglows.ecmwf.int/api/v2"

# Shape of a GEOGLOWS V2 retrospectivedaily response: discharge keyed by the
# river_id string, parallel to a "datetime" array; m3/s ("cms").
RETRO_RESPONSE = {
    "621130084": [205000.0, 210500.5, 198750.2],
    "datetime": [
        "2026-05-01T00:00:00+00:00",
        "2026-05-02T00:00:00+00:00",
        "2026-05-03T00:00:00+00:00",
    ],
    "metadata": {
        "river_id": 621130084,
        "units": {"long": "cubic meters per second", "short": "cms"},
    },
}

# Shape of a GEOGLOWS V2 forecast response: ensemble "flow_median" series.
FORECAST_RESPONSE = {
    "datetime": [
        "2026-06-02T00:00:00+00:00",
        "2026-06-02T03:00:00+00:00",
    ],
    "flow_median": [2375.8, 2372.9],
    "flow_uncertainty_lower": [2300.0, 2295.0],
    "flow_uncertainty_upper": [2450.0, 2448.0],
    "metadata": {"river_id": 621130084},
}


@pytest.mark.asyncio
async def test_fetch_stations_default_reaches():
    async with GEOGloWSConnector() as conn:
        stations = await conn.fetch_stations()

    # Built-in reaches are returned without any network call.
    assert len(stations) >= 1
    amazon = next(s for s in stations if s.native_id == "621130084")
    assert amazon.provider == "geoglows"
    assert amazon.id == "geoglows:621130084"
    assert amazon.river == "Amazon"
    assert amazon.country_code == "BR"


@pytest.mark.asyncio
async def test_fetch_stations_config_override():
    cfg = {"virtual_stations": [{"id": "123456789"}]}
    async with GEOGloWSConnector(config=cfg) as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "123456789"
    assert stations[0].id == "geoglows:123456789"
    assert stations[0].name == "Reach 123456789"  # falls back when name absent
    assert stations[0].country_code == "global"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_retrospective():
    route = respx.get(f"{_BASE}/retrospectivedaily/621130084").mock(
        return_value=httpx.Response(200, json=RETRO_RESPONSE)
    )

    async with GEOGloWSConnector() as conn:
        chunk = await conn.fetch_observations(
            "geoglows:621130084",
            start=datetime(2026, 5, 1, tzinfo=UTC),
            end=datetime(2026, 5, 3, tzinfo=UTC),
        )

    assert route.call_count == 1
    req = route.calls[0].request
    assert req.url.params["start_date"] == "20260501"
    assert req.url.params["end_date"] == "20260503"

    assert chunk.station_id == "geoglows:621130084"
    assert chunk.provider == "geoglows"
    assert len(chunk.observations) == 3
    obs = chunk.observations[0]
    # Big river -> large discharge, in m3/s.
    assert obs.discharge_m3s == pytest.approx(205000.0)
    assert obs.timestamp == datetime(2026, 5, 1, tzinfo=UTC)
    # GEOGLOWS is model output, not a gauge reading.
    assert obs.quality == QualityFlag.ESTIMATED


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_forecast():
    # A window in the future routes to the forecast endpoint (flow_median).
    route = respx.get(f"{_BASE}/forecast/621130084").mock(
        return_value=httpx.Response(200, json=FORECAST_RESPONSE)
    )

    start = datetime(2026, 6, 2, tzinfo=UTC)
    end = datetime(2030, 1, 1, tzinfo=UTC)
    async with GEOGloWSConnector() as conn:
        chunk = await conn.fetch_observations("geoglows:621130084", start, end)

    assert route.call_count == 1
    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(2375.8)
    assert chunk.observations[0].quality == QualityFlag.ESTIMATED


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_bad_payload():
    respx.get(f"{_BASE}/retrospectivedaily/621130084").mock(
        return_value=httpx.Response(200, json={"error": "River ID not found"})
    )

    async with GEOGloWSConnector() as conn:
        with pytest.raises(DataFormatError):
            await conn.fetch_observations(
                "geoglows:621130084",
                start=datetime(2026, 5, 1, tzinfo=UTC),
                end=datetime(2026, 5, 3, tzinfo=UTC),
            )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_nulls_and_window():
    payload = {
        "621130084": [None, 1000.0, 2000.0],
        "datetime": [
            "2026-05-01T00:00:00+00:00",
            "2026-05-02T00:00:00+00:00",
            "2026-05-20T00:00:00+00:00",  # outside requested window
        ],
        "metadata": {},
    }
    respx.get(f"{_BASE}/retrospectivedaily/621130084").mock(
        return_value=httpx.Response(200, json=payload)
    )

    async with GEOGloWSConnector() as conn:
        chunk = await conn.fetch_observations(
            "geoglows:621130084",
            start=datetime(2026, 5, 1, tzinfo=UTC),
            end=datetime(2026, 5, 3, tzinfo=UTC),
        )

    # Third point is outside the window and is dropped; null is preserved.
    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[1].discharge_m3s == pytest.approx(1000.0)


def test_connector_is_registered():
    from csfs.core.registry import get_connector

    assert get_connector("geoglows") is GEOGloWSConnector
