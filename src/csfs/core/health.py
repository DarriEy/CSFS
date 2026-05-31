# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Shared connector-health helpers used by both the API and the CLI.

The store's :meth:`get_connector_health` reports only providers that have
appeared in the stations table or the acquisition log. These helpers layer on
the *registered roster* (so connectors that have never run still surface as
``data_health == "none"``) and provide the summary / degraded-filtering logic
that the API endpoint and the ``csfs health`` command share.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from csfs.store.base import BaseStore

# data_health values that mean "stored data is not fresh/usable".
DEGRADED_DATA_HEALTH = ("none", "empty", "stale")
# Acquisition-log statuses that mean "the last run did not fully succeed".
DEGRADED_RUN_STATUS = ("error", "degraded")


def _empty_row(slug: str) -> dict:
    """A health row for a registered connector with no data and no logged run."""
    return {
        "provider": slug,
        "stations": 0,
        "observations": 0,
        "latest_observation": None,
        "last_fetch_at": None,
        "staleness_hours": None,
        "data_health": "none",
        "last_run": None,
        "last_status": None,
        "last_error": None,
        "last_ok_at": None,
        "total_runs": 0,
        "ok_runs": 0,
        "success_rate": None,
    }


async def gather_connector_health(
    store: BaseStore,
    *,
    stale_after_hours: float = 168.0,
    include_registered: bool = True,
) -> list[dict]:
    """Per-provider health, optionally padded with the full registered roster.

    Returns the same row shape as :meth:`BaseStore.get_connector_health`,
    sorted by provider slug when ``include_registered`` is set.
    """
    rows = await store.get_connector_health(stale_after_hours=stale_after_hours)

    if include_registered:
        from csfs.core.registry import discover
        from csfs.core.registry import list_providers as _list_providers

        discover()
        seen = {r["provider"] for r in rows}
        for slug in _list_providers():
            if slug not in seen:
                rows.append(_empty_row(slug))
        rows.sort(key=lambda r: r["provider"])

    return rows


def summarize_health(rows: list[dict]) -> dict[str, int]:
    """Count connectors per ``data_health`` bucket."""
    summary: dict[str, int] = {}
    for r in rows:
        summary[r["data_health"]] = summary.get(r["data_health"], 0) + 1
    return summary


def is_degraded(
    row: dict,
    *,
    data_health: tuple[str, ...] = DEGRADED_DATA_HEALTH,
    run_status: tuple[str, ...] = DEGRADED_RUN_STATUS,
) -> bool:
    """Whether a health row should trip an alert.

    A connector is degraded if its stored data is in a flagged ``data_health``
    bucket, or its most recent acquisition run ended in a flagged status.
    """
    if row.get("data_health") in data_health:
        return True
    return row.get("last_status") in run_status


def degraded_connectors(
    rows: list[dict],
    *,
    data_health: tuple[str, ...] = DEGRADED_DATA_HEALTH,
    run_status: tuple[str, ...] = DEGRADED_RUN_STATUS,
) -> list[dict]:
    """Subset of ``rows`` that are degraded per :func:`is_degraded`."""
    return [
        r for r in rows
        if is_degraded(r, data_health=data_health, run_status=run_status)
    ]
