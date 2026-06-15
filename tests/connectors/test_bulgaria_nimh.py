# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Tests for the Bulgaria NIMH connector (per-day runoff HTML table)."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.bulgaria_nimh import BulgariaNimhConnector
from csfs.core.models import QualityFlag
from csfs.core.registry import get_connector

_URL = "https://info.meteo.bg/openData/river-runoff/"


def _table(q_value: str) -> str:
    return f"""
    <table class="nimh-table">
      <thead><tr>
        <th>№</th><th>Река</th><th>Местност</th><th>Qmin</th><th>Qср</th>
        <th>Qmax</th><th>H</th><th>Q</th><th>ΔH</th>
      </tr></thead>
      <tbody>
        <tr><td>14840</td><td>Лом</td><td>с.Василковци</td><td>0,079</td>
            <td>4,898</td><td>233,000</td><td>39</td><td>{q_value}</td><td>3</td></tr>
        <tr><td>16800</td><td>Огоста</td><td>с.Кобиляк</td><td>0,5</td>
            <td>10,0</td><td>120,0</td><td>52</td><td>21,964</td><td>1</td></tr>
      </tbody>
    </table>
    """


def test_connector_is_registered():
    assert get_connector("bulgaria_nimh") is BulgariaNimhConnector


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_table():
    respx.post(_URL).mock(return_value=httpx.Response(200, text=_table("5,140")))

    async with BulgariaNimhConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    s = stations[0]
    assert s.native_id == "14840"
    assert s.river == "Лом"
    assert s.name == "Лом (с.Василковци)"
    assert s.country_code == "BG"
    assert s.provider == "bulgaria_nimh"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_discharge():
    # Same table for every day in the (short) window.
    respx.post(_URL).mock(return_value=httpx.Response(200, text=_table("5,140")))

    async with BulgariaNimhConnector() as conn:
        chunk = await conn.fetch_observations(
            "bulgaria_nimh:14840",
            start=datetime(2026, 6, 1, tzinfo=UTC),
            end=datetime(2026, 6, 3, tzinfo=UTC),
        )

    # One observation per day in [1, 3].
    assert len(chunk.observations) == 3
    obs = chunk.observations[0]
    assert obs.discharge_m3s == pytest.approx(5.14)  # Bulgarian comma -> dot
    assert obs.quality == QualityFlag.RAW
    assert obs.timestamp == datetime(2026, 6, 1, tzinfo=UTC)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_missing_value_is_none():
    respx.post(_URL).mock(return_value=httpx.Response(200, text=_table("-")))

    async with BulgariaNimhConnector() as conn:
        chunk = await conn.fetch_observations(
            "bulgaria_nimh:14840",
            start=datetime(2026, 6, 1, tzinfo=UTC),
            end=datetime(2026, 6, 1, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[0].quality == QualityFlag.MISSING


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_unknown_station_empty():
    respx.post(_URL).mock(return_value=httpx.Response(200, text=_table("5,140")))

    async with BulgariaNimhConnector() as conn:
        chunk = await conn.fetch_observations(
            "bulgaria_nimh:99999",
            start=datetime(2026, 6, 1, tzinfo=UTC),
            end=datetime(2026, 6, 1, tzinfo=UTC),
        )
    assert chunk.observations == []
