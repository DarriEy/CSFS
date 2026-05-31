"""Shared test fixtures."""

from __future__ import annotations

import socket
from datetime import datetime

import pytest

from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk

# ---------------------------------------------------------------------------
# Hermetic network guard
#
# Connector tests must mock their HTTP (respx); none should touch a real
# upstream, or the suite becomes slow and flaky. This autouse guard blocks DNS
# resolution for any non-local host, so an unmocked call fails fast and
# deterministically instead of hanging until a socket timeout. Opt a test out
# with @pytest.mark.network (those are deselected in CI via `-m "not network"`).
# ---------------------------------------------------------------------------

_ALLOWED_HOSTS = {"127.0.0.1", "::1", "localhost", "", None}
_real_getaddrinfo = socket.getaddrinfo


def _guarded_getaddrinfo(host, *args, **kwargs):
    if host not in _ALLOWED_HOSTS:
        raise RuntimeError(
            f"Blocked network access to {host!r} during tests. Mock the HTTP "
            "call with respx, or mark the test with @pytest.mark.network."
        )
    return _real_getaddrinfo(host, *args, **kwargs)


@pytest.fixture(autouse=True)
def _block_network(request, monkeypatch):
    if request.node.get_closest_marker("network"):
        return  # explicitly allowed to reach the real upstream
    monkeypatch.setattr(socket, "getaddrinfo", _guarded_getaddrinfo)


@pytest.fixture
def sample_station() -> Station:
    return Station(
        id="usgs:01646500",
        provider="usgs",
        native_id="01646500",
        name="Potomac River near Washington DC",
        latitude=38.9497,
        longitude=-77.1278,
        country_code="US",
        river="Potomac",
        catchment_area_km2=29940.0,
    )


@pytest.fixture
def sample_chunk() -> TimeSeriesChunk:
    return TimeSeriesChunk(
        station_id="usgs:01646500",
        provider="usgs",
        observations=[
            Observation(
                station_id="usgs:01646500",
                timestamp=datetime(2024, 6, 1, 0, 0),
                discharge_m3s=150.5,
                quality=QualityFlag.GOOD,
            ),
            Observation(
                station_id="usgs:01646500",
                timestamp=datetime(2024, 6, 2, 0, 0),
                discharge_m3s=145.2,
                quality=QualityFlag.GOOD,
            ),
        ],
        fetched_at=datetime(2024, 6, 2, 12, 0),
    )
