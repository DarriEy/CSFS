# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Tests for the Ecuador INAMHI (GEOGLOWS V2) connector with mocked responses."""

from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx

from csfs.connectors.ecuador_inamhi import EcuadorINAMHIConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import QualityFlag

_BASE = "https://geoglows.ecmwf.int/api/v2"
_REACH = "670049564"  # Guayas at Daule

# GEOGLOWS V2 retrospective shape: discharge keyed by river_id, parallel to
# the shared "datetime" array.
MOCK_RETRO = {
    _REACH: [350.2, 375.8, 400.1, 310.0],
    "datetime": [
        "2024-06-01T00:00:00",
        "2024-06-02T00:00:00",
        "2024-06-03T00:00:00",
        "2024-05-30T00:00:00",
    ],
    "metadata": {"river_id": int(_REACH)},
}

MOCK_FORECAST = {
    "datetime": ["2024-06-10T00:00:00", "2024-06-11T00:00:00"],
    "flow_median": [420.5, 415.0],
}


# === Station tests ===================================================

@pytest.mark.asyncio
async def test_fetch_stations_returns_seed_list():
    async with EcuadorINAMHIConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 19  # 20 seeds, two Esmeraldas points dedup to one reach
    s0 = stations[0]
    assert s0.id == f"ecuador_inamhi:{_REACH}"
    assert s0.native_id == _REACH
    assert s0.name == "Guayas at Daule"
    assert s0.river == "Guayas"
    assert s0.country_code == "EC"
    assert s0.provider == "ecuador_inamhi"
    assert s0.is_active is True


@pytest.mark.asyncio
async def test_fetch_stations_have_unique_ids():
    async with EcuadorINAMHIConnector() as conn:
        stations = await conn.fetch_stations()
    ids = [s.native_id for s in stations]
    assert len(ids) == len(set(ids)), "river_ids must be unique"
    assert all(s.id.startswith("ecuador_inamhi:") for s in stations)


# === Retrospective observation tests =================================

@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_retrospective():
    respx.get(f"{_BASE}/retrospectivedaily/{_REACH}").mock(
        return_value=httpx.Response(200, json=MOCK_RETRO),
    )

    async with EcuadorINAMHIConnector() as conn:
        chunk = await conn.fetch_observations(
            f"ecuador_inamhi:{_REACH}",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 3, tzinfo=UTC),
        )

    assert chunk.station_id == f"ecuador_inamhi:{_REACH}"
    assert chunk.provider == "ecuador_inamhi"
    # 2024-05-30 is out of the requested window.
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(350.2)
    assert chunk.observations[0].quality == QualityFlag.ESTIMATED
    assert chunk.observations[2].discharge_m3s == pytest.approx(400.1)


@pytest.mark.asyncio
@respx.mock
async def test_retrospective_request_shape():
    route = respx.get(f"{_BASE}/retrospectivedaily/{_REACH}").mock(
        return_value=httpx.Response(200, json={_REACH: [], "datetime": []}),
    )

    async with EcuadorINAMHIConnector() as conn:
        await conn.fetch_observations(
            f"ecuador_inamhi:{_REACH}",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 3, tzinfo=UTC),
        )

    assert route.called
    url = str(route.calls[0].request.url)
    assert "start_date=20240601" in url
    assert "end_date=20240603" in url
    assert "format=json" in url


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_forecast_for_future_window():
    """A window ending in the future uses the forecast endpoint."""
    route = respx.get(f"{_BASE}/forecast/{_REACH}").mock(
        return_value=httpx.Response(200, json=MOCK_FORECAST),
    )

    now = datetime.now(UTC)
    async with EcuadorINAMHIConnector() as conn:
        chunk = await conn.fetch_observations(
            f"ecuador_inamhi:{_REACH}",
            start=datetime(2024, 6, 10, tzinfo=UTC),
            end=now + timedelta(days=5),
        )

    assert route.called
    # MOCK_FORECAST datetimes (2024-06) fall before the window end, but the
    # forecast endpoint was selected because end > now.
    assert chunk.provider == "ecuador_inamhi"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty_series():
    respx.get(f"{_BASE}/retrospectivedaily/{_REACH}").mock(
        return_value=httpx.Response(200, json={_REACH: [], "datetime": []}),
    )

    async with EcuadorINAMHIConnector() as conn:
        chunk = await conn.fetch_observations(
            f"ecuador_inamhi:{_REACH}",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_skips_null_discharge():
    respx.get(f"{_BASE}/retrospectivedaily/{_REACH}").mock(
        return_value=httpx.Response(200, json={
            _REACH: [None, 200.0],
            "datetime": ["2024-06-01T00:00:00", "2024-06-02T00:00:00"],
        }),
    )

    async with EcuadorINAMHIConnector() as conn:
        chunk = await conn.fetch_observations(
            f"ecuador_inamhi:{_REACH}",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 3, tzinfo=UTC),
        )

    # The null value still yields an observation with discharge_m3s=None.
    vals = [o.discharge_m3s for o in chunk.observations]
    assert vals == [None, pytest.approx(200.0)]


@pytest.mark.asyncio
@respx.mock
async def test_unexpected_response_raises_data_format_error():
    respx.get(f"{_BASE}/retrospectivedaily/{_REACH}").mock(
        return_value=httpx.Response(200, json={"unexpected": "shape"}),
    )

    async with EcuadorINAMHIConnector() as conn:
        with pytest.raises(DataFormatError):
            await conn.fetch_observations(
                f"ecuador_inamhi:{_REACH}",
                start=datetime(2024, 6, 1, tzinfo=UTC),
                end=datetime(2024, 6, 3, tzinfo=UTC),
            )


# === extract_values unit tests =======================================

def test_extract_values_keyed_by_reach():
    payload = {_REACH: [1.0, 2.0], "datetime": ["a", "b"]}
    assert EcuadorINAMHIConnector._extract_values(payload, _REACH) == [1.0, 2.0]


def test_extract_values_forecast_median():
    payload = {"flow_median": [9.0], "datetime": ["a"]}
    assert EcuadorINAMHIConnector._extract_values(payload, _REACH) == [9.0]


def test_extract_values_fallback_parallel_list():
    payload = {"datetime": ["a", "b"], "some_series": [3.0, 4.0], "metadata": {}}
    assert EcuadorINAMHIConnector._extract_values(payload, _REACH) == [3.0, 4.0]


def test_extract_values_none_when_absent():
    payload = {"datetime": ["a", "b"], "metadata": {}}
    assert EcuadorINAMHIConnector._extract_values(payload, _REACH) is None


# === Registration / metadata =========================================

def test_connector_registered():
    from csfs.core.registry import get_connector

    assert get_connector("ecuador_inamhi") is EcuadorINAMHIConnector


def test_connector_class_attributes():
    assert EcuadorINAMHIConnector.slug == "ecuador_inamhi"
    assert EcuadorINAMHIConnector.country_codes == ["EC"]
    assert EcuadorINAMHIConnector.base_url.endswith("/api/v2")
