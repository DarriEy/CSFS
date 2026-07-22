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

from datetime import UTC, datetime
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from csfs.store.base import BaseStore

# data_health values that mean "stored data is not fresh/usable".
DEGRADED_DATA_HEALTH = ("none", "empty", "stale")
# Acquisition-log statuses that mean "the last run did not fully succeed".
DEGRADED_RUN_STATUS = ("error", "degraded")


def classify_root_cause(row: dict) -> str | None:
    """Return a stable, operational root-cause category for a health row."""
    lifecycle = row.get("lifecycle", "active")
    if lifecycle in {"deprecated", "retired"}:
        return f"lifecycle:{lifecycle}"
    status = row.get("last_status")
    error = str(row.get("last_error") or "").lower()
    if status in DEGRADED_RUN_STATUS:
        if any(token in error for token in ("401", "403", "credential", "unauthor", "forbidden")):
            return "outage:authentication"
        # Check transport status before generic words such as "format". URLs
        # commonly contain ``format=json`` even when the actual failure is a
        # 502/503 response from the provider.
        if any(token in error for token in ("timeout", "timed out", "connection", "dns", "503", "502")):
            return "outage:upstream"
        if any(token in error for token in ("schema", "column", "parse", "format", "decode")):
            return "outage:schema-drift"
        if any(token in error for token in ("404", "410", "retired", "deprecated", "not found")):
            return "outage:product-unavailable"
        return f"run:{status}"
    data_health = row.get("data_health")
    if data_health in DEGRADED_DATA_HEALTH:
        return f"data:{data_health}"
    return None


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
        "lifecycle": "active",
    }


async def gather_connector_health(
    store: BaseStore,
    *,
    stale_after_hours: float = 168.0,
    include_registered: bool = True,
    tier_aware: bool = True,
) -> list[dict]:
    """Per-provider health, optionally padded with the full registered roster.

    Returns the same row shape as :meth:`BaseStore.get_connector_health`,
    sorted by provider slug when ``include_registered`` is set.

    With ``tier_aware`` (the default), per-provider staleness expectations
    from the scheduler roster apply (weekly-tier archives tolerate years of
    lag; see ``TIER_STALE_AFTER`` / ``PROVIDER_STALE_AFTER`` in
    ``csfs.scheduler.cron``); ``stale_after_hours`` remains the threshold
    for providers without a roster entry.
    """
    overrides: dict[str, float] | None = None
    if tier_aware:
        from csfs.scheduler.cron import stale_after_by_provider

        overrides = stale_after_by_provider()
    rows = await store.get_connector_health(
        stale_after_hours=stale_after_hours,
        stale_after_by_provider=overrides,
    )

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


def health_status_document(
    rows: list[dict],
    *,
    stale_threshold_hours: float,
    tier: str | None = None,
    data_health: tuple[str, ...] = DEGRADED_DATA_HEALTH,
    run_status: tuple[str, ...] = DEGRADED_RUN_STATUS,
) -> dict:
    """Build a versioned status document with stable alert root causes."""
    flagged = degraded_connectors(rows, data_health=data_health, run_status=run_status)
    reasons: dict[str, list[str]] = {}
    for row in flagged:
        reason = classify_root_cause(row) or "unknown"
        reasons.setdefault(reason, []).append(str(row["provider"]))
    now = datetime.now(UTC)
    connectors: list[dict] = []
    for source in rows:
        row = dict(source)
        last_run = row.get("last_run")
        if last_run is not None:
            if last_run.tzinfo is None:
                last_run = last_run.replace(tzinfo=UTC)
            row["hours_since_last_run"] = max(0.0, (now - last_run).total_seconds() / 3600)
        row.setdefault("lifecycle", "active")
        row["root_cause"] = classify_root_cause(row)
        connectors.append(row)
    return {
        "schema_version": 1,
        "generated_at": datetime.now(UTC).isoformat(),
        "tier": tier,
        "status": "degraded" if flagged else "healthy",
        "stale_threshold_hours": stale_threshold_hours,
        "summary": summarize_health(rows),
        "degraded": [r["provider"] for r in flagged],
        "reasons": reasons,
        "connectors": connectors,
    }
