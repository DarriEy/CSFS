# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Tests for the Peru SENAMHI connector.

VERDICT (2026-06): NOT FIXABLE -- SENAMHI's open real-time hydro feed
publishes water level only ("Nivel promedio diario (m)"), not discharge
(caudal, m3/s), so this connector intentionally yields no observations.
These tests pin that graceful-empty contract plus registration; the autouse
network guard in tests/conftest.py keeps them hermetic.
"""

from __future__ import annotations

from datetime import UTC, datetime

import pytest

from csfs.connectors.peru_senamhi import PeruSenamhiConnector
from csfs.core.models import Station, TimeSeriesChunk
from csfs.core.registry import discover, get_connector


@pytest.mark.asyncio
async def test_fetch_stations_returns_empty_no_fake_seed():
    """No open discharge feed -> no stations (and no fabricated seed)."""
    async with PeruSenamhiConnector() as conn:
        stations = await conn.fetch_stations()

    assert stations == []
    assert isinstance(stations, list)


@pytest.mark.asyncio
async def test_fetch_observations_returns_empty_chunk_gracefully():
    """Observations return an empty chunk rather than raising, so bulk
    acquisition is not disrupted by a level-only upstream."""
    start = datetime(2026, 5, 1, tzinfo=UTC)
    end = datetime(2026, 6, 1, tzinfo=UTC)

    async with PeruSenamhiConnector() as conn:
        chunk = await conn.fetch_observations("peru_senamhi:230503", start, end)

    assert isinstance(chunk, TimeSeriesChunk)
    assert chunk.provider == "peru_senamhi"
    assert chunk.station_id == "peru_senamhi:230503"
    assert chunk.observations == []


@pytest.mark.asyncio
async def test_no_discharge_emitted():
    """Belt-and-suspenders: even if observations existed, none would carry
    discharge, since SENAMHI serves water level (m) only. Assert the chunk
    yields zero discharge values."""
    async with PeruSenamhiConnector() as conn:
        chunk = await conn.fetch_observations(
            "peru_senamhi:250303",
            datetime(2026, 5, 1, tzinfo=UTC),
            datetime(2026, 6, 1, tzinfo=UTC),
        )

    discharges = [o.discharge_m3s for o in chunk.observations if o.discharge_m3s]
    assert discharges == []


def test_connector_metadata():
    assert PeruSenamhiConnector.slug == "peru_senamhi"
    assert PeruSenamhiConnector.country_codes == ["PE"]
    assert PeruSenamhiConnector.base_url == "https://www.senamhi.gob.pe"


def test_registration():
    """Connector stays registered/importable under its slug."""
    discover()
    cls = get_connector("peru_senamhi")
    assert cls is PeruSenamhiConnector
    assert issubclass(cls, PeruSenamhiConnector.__mro__[1])  # BaseConnector


def test_station_id_roundtrip():
    """The slug:native_id convention round-trips, so the connector stays
    compatible with the rest of the pipeline if a feed ever appears."""
    conn = PeruSenamhiConnector()
    sid = conn._station_id("230503")
    assert sid == "peru_senamhi:230503"
    assert sid.removeprefix("peru_senamhi:") == "230503"

    # Smoke: a Station built with this id validates against the model.
    st = Station(
        id=sid,
        provider="peru_senamhi",
        native_id="230503",
        name="Puente Cunyac",
        latitude=-13.563,
        longitude=-72.5752,
        country_code="PE",
    )
    assert st.id == "peru_senamhi:230503"
