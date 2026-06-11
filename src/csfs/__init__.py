# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""CSFS — Community Streamflow Service.

This module is the blessed public Python API. Everything re-exported here is
stable across minor releases; deeper imports (``csfs.connectors.*``,
``csfs.store.*``, ...) are internal and may change without notice.

Typical usage::

    import csfs

    async with csfs.open_store("csfs.duckdb") as store:
        df = await store.get_observations_df("usgs:01646500")

Connector modules are *not* imported here; call :func:`discover` (or use
:func:`fetch_observations` / :func:`run_acquisition`, which do it for you)
to populate the provider registry.
"""

from __future__ import annotations

import asyncio
from datetime import datetime
from pathlib import Path

from csfs.core.config import load_config
from csfs.core.models import (
    OBSERVATION_SCHEMA,
    STATION_SCHEMA,
    Observation,
    QualityFlag,
    Station,
    TimeSeriesChunk,
)
from csfs.core.registry import discover, get_connector, list_providers
from csfs.scheduler.runner import run_acquisition
from csfs.store.duckdb_store import DuckDBStore

__version__ = "0.2.0"

__all__ = [
    "OBSERVATION_SCHEMA",
    "STATION_SCHEMA",
    "DuckDBStore",
    "Observation",
    "QualityFlag",
    "Station",
    "TimeSeriesChunk",
    "__version__",
    "discover",
    "fetch_observations",
    "fetch_observations_sync",
    "get_connector",
    "list_providers",
    "load_config",
    "open_store",
    "run_acquisition",
]


def open_store(db_path: str | Path = "csfs.duckdb", read_only: bool = True) -> DuckDBStore:
    """Open a CSFS observation store (thin alias for :class:`DuckDBStore`).

    Returns an *async context manager*; the database is opened on
    ``__aenter__``. Defaults to read-only, the safe mode for analysis and
    model-calibration workloads — pass ``read_only=False`` to create or
    write to the database (e.g. before :func:`run_acquisition`).

    Example::

        async with csfs.open_store("csfs.duckdb") as store:
            stations = await store.get_stations(provider="usgs")
    """
    return DuckDBStore(db_path, read_only=read_only)


async def fetch_observations(
    provider_slug: str,
    station_id: str,
    start: datetime,
    end: datetime,
    config: dict | None = None,
) -> TimeSeriesChunk:
    """Fetch one station's observations directly from a provider, no store needed.

    One-shot wrapper around ``discover()`` + ``get_connector(slug)`` + the
    connector's async context manager. ``station_id`` is the canonical CSFS
    ID (``"<provider>:<native_id>"``). ``config`` carries provider-specific
    settings such as API keys; most providers need none.
    """
    discover()
    connector_cls = get_connector(provider_slug)
    async with connector_cls(config=config or {}) as connector:
        return await connector.fetch_observations(station_id, start, end)


def fetch_observations_sync(
    provider_slug: str,
    station_id: str,
    start: datetime,
    end: datetime,
    config: dict | None = None,
) -> TimeSeriesChunk:
    """Synchronous convenience wrapper around :func:`fetch_observations`.

    Runs the fetch in a fresh event loop via :func:`asyncio.run`. Must be
    called from synchronous code; raises :class:`RuntimeError` if an event
    loop is already running (use ``await csfs.fetch_observations(...)``
    there instead).
    """
    try:
        asyncio.get_running_loop()
    except RuntimeError:
        return asyncio.run(fetch_observations(provider_slug, station_id, start, end, config))
    raise RuntimeError(
        "fetch_observations_sync() cannot be called while an event loop is "
        "running; use 'await csfs.fetch_observations(...)' instead."
    )
