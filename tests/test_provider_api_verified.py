"""Live round-trip verification for the posture-only provider_api drop-ins.

The 19 national/regional streamflow APIs in
``csfs.integrations.symfluence.NATIONAL_PROVIDER_APIS`` have NO native SYMFLUENCE
handler to parity-grade against, so they are admitted by the framework's
posture-only gate (open/attribution source license). "Verified" for these means
a real round-trip returns observed discharge — proven here.

These tests reach real upstream APIs, so they are marked ``network`` and
deselected in CI (``-m 'not network'``). They are the standing evidence behind
each provider's drop-in claim; run them with ``-m network`` to re-verify.

Every connector is tried against two windows (a fixed 2024-Q1 archive window and
a recent ~6-week window) to cover both archive and realtime-only providers — the
same methodology used to qualify the roster. A provider passes if ANY tried
station returns at least one real discharge value in EITHER window.
"""
from __future__ import annotations

import asyncio
from datetime import UTC, datetime

import pytest

import csfs.integrations.symfluence as integration
from csfs.core.registry import discover, get_connector

pytestmark = [pytest.mark.network, pytest.mark.asyncio]

discover()  # populate the connector registry so get_connector(slug) resolves

_WINDOWS = (
    (datetime(2024, 1, 1, tzinfo=UTC), datetime(2024, 3, 1, tzinfo=UTC)),   # archive
    (datetime(2026, 5, 1, tzinfo=UTC), datetime(2026, 6, 15, tzinfo=UTC)),  # realtime
)
_MAX_STATIONS = 5
_TIMEOUT = 90


async def _first_real_discharge(slug: str) -> tuple[int, str | None]:
    """Return (real_obs_count, sample_station) for the first station with data."""
    conn_cls = get_connector(slug)
    async with conn_cls(config={}) as conn:
        stations = await asyncio.wait_for(conn.fetch_stations(), timeout=_TIMEOUT)
        for station in stations[:_MAX_STATIONS]:
            for start, end in _WINDOWS:
                try:
                    chunk = await asyncio.wait_for(
                        conn.fetch_observations(station.id, start, end), timeout=_TIMEOUT
                    )
                except Exception:  # noqa: BLE001 - try the next window/station
                    continue
                real = sum(1 for o in chunk.observations if o.discharge_m3s is not None)
                if real > 0:
                    return real, station.native_id
    return 0, None


@pytest.mark.parametrize(
    "api", integration.NATIONAL_PROVIDER_APIS, ids=[a.slug for a in integration.NATIONAL_PROVIDER_APIS]
)
async def test_national_provider_returns_real_discharge(api):
    """Each posture-only drop-in must serve observed discharge from its live API."""
    real, sample = await _first_real_discharge(api.slug)
    assert real > 0, (
        f"{api.slug}: no real discharge returned from the live API in either window; "
        "the posture-only drop-in claim is unverified"
    )
