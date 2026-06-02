# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Tests for the GloFAS connector (Open-Meteo Flood API, respx-mocked)."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.glofas import GloFASConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import QualityFlag

_FLOOD_URL = "https://flood-api.open-meteo.com/v1/flood"

MOCK_RESPONSE = {
    "latitude": -1.95,
    "longitude": -55.51,
    "daily_units": {"time": "iso8601", "river_discharge": "m³/s"},
    "daily": {
        "time": ["2026-05-01", "2026-05-02", "2026-05-03"],
        "river_discharge": [302690.3, 300606.8, 297850.6],
    },
}


@pytest.mark.asyncio
async def test_fetch_stations_default_reporting_points():
    async with GloFASConnector() as conn:
        stations = await conn.fetch_stations()

    # Built-in reporting points are returned without any network call.
    assert len(stations) >= 10
    amazon = next(s for s in stations if s.native_id == "amazon_obidos")
    assert amazon.provider == "glofas"
    assert amazon.id == "glofas:amazon_obidos"
    assert amazon.river == "Amazon"
    assert amazon.country_code == "BR"
    assert amazon.latitude == pytest.approx(-1.95)


@pytest.mark.asyncio
async def test_fetch_stations_config_override():
    cfg = {"virtual_stations": [{"id": "x1", "lat": 10.0, "lon": 20.0}]}
    async with GloFASConnector(config=cfg) as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "x1"
    assert stations[0].name == "x1"  # falls back to id when name missing
    assert stations[0].country_code == "global"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations():
    route = respx.get(_FLOOD_URL).mock(
        return_value=httpx.Response(200, json=MOCK_RESPONSE)
    )

    async with GloFASConnector() as conn:
        chunk = await conn.fetch_observations(
            "glofas:amazon_obidos",
            start=datetime(2026, 5, 1, tzinfo=UTC),
            end=datetime(2026, 5, 3, tzinfo=UTC),
        )

    assert route.call_count == 1
    # Verify the request carried the reporting point's coordinates + variable.
    req = route.calls[0].request
    assert req.url.params["daily"] == "river_discharge"
    assert req.url.params["latitude"] == "-1.95"

    assert chunk.station_id == "glofas:amazon_obidos"
    assert len(chunk.observations) == 3
    obs = chunk.observations[0]
    assert obs.discharge_m3s == pytest.approx(302690.3)
    assert obs.timestamp == datetime(2026, 5, 1, tzinfo=UTC)
    # GloFAS is model output, not a gauge reading.
    assert obs.quality == QualityFlag.ESTIMATED


@pytest.mark.asyncio
async def test_fetch_observations_unknown_station():
    async with GloFASConnector() as conn:
        with pytest.raises(ConnectorError):
            await conn.fetch_observations(
                "glofas:does_not_exist",
                start=datetime(2026, 5, 1, tzinfo=UTC),
                end=datetime(2026, 5, 3, tzinfo=UTC),
            )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_bad_payload():
    respx.get(_FLOOD_URL).mock(return_value=httpx.Response(200, json={"foo": "bar"}))

    async with GloFASConnector() as conn:
        with pytest.raises(DataFormatError):
            await conn.fetch_observations(
                "glofas:amazon_obidos",
                start=datetime(2026, 5, 1, tzinfo=UTC),
                end=datetime(2026, 5, 3, tzinfo=UTC),
            )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_null_values():
    payload = {
        "daily": {
            "time": ["2026-05-01", "2026-05-02"],
            "river_discharge": [None, 123.4],
        }
    }
    respx.get(_FLOOD_URL).mock(return_value=httpx.Response(200, json=payload))

    async with GloFASConnector() as conn:
        chunk = await conn.fetch_observations(
            "glofas:amazon_obidos",
            start=datetime(2026, 5, 1, tzinfo=UTC),
            end=datetime(2026, 5, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[1].discharge_m3s == pytest.approx(123.4)


def test_connector_is_registered():
    from csfs.core.registry import get_connector

    assert get_connector("glofas") is GloFASConnector
