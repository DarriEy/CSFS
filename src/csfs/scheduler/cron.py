# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Cron-based scheduler for periodic acquisition cycles."""

from __future__ import annotations

import asyncio
import signal
from datetime import UTC, datetime

import structlog
from croniter import croniter

from csfs.scheduler.runner import run_acquisition
from csfs.store.duckdb_store import DuckDBStore

logger = structlog.get_logger()

DEFAULT_SCHEDULES: dict[str, str] = {
    "realtime": "*/15 * * * *",   # every 15 min
    "hourly": "5 * * * *",        # every hour at :05
    "daily": "30 2 * * *",        # daily at 02:30
    "weekly": "0 3 * * 0",        # Sunday at 03:00
}

PROVIDER_TIERS: dict[str, list[str]] = {
    "realtime": [
        "usgs", "uk_ea", "france_hubeau", "germany_pegelonline",
        "environment_canada", "poland_imgw",
    ],
    "hourly": [
        "norway_nve", "sweden_smhi", "switzerland_bafu",
        "australia_bom", "finland_syke", "greece_openhi",
        "colombia_ideam", "taiwan_wra",
    ],
    "daily": [
        "japan_mlit", "south_africa_dws", "china_mwr",
        "bosnia_fhmz", "iceland_lamahice", "ecuador_inamhi",
        "elsalvador_marn", "jamaica_wra", "iran_iwrmc",
        "kazakhstan_kazhydromet", "malaysia_did",
        "philippines_dpwh", "pakistan_wapda", "nepal_icimod",
        "panama_stri", "vietnam_mekong", "afghanistan_usgs",
        "bolivia_ine", "bulgaria_nimh", "danube_his",
        "israel_caravan",
    ],
    "weekly": [
        "grdc", "estreams", "ca_discharge", "caravan", "gsim",
        "lamah_ce", "sierem", "adhi", "russia_arcticnet",
    ],
}

TIER_LOOKBACK: dict[str, int] = {
    "realtime": 4,
    "hourly": 48,
    "daily": 168,
    "weekly": 720,
}


async def run_scheduled_cycle(
    db_path: str,
    tier: str | None = None,
    providers: list[str] | None = None,
    max_stations: int | None = None,
    concurrency: int = 10,
) -> dict[str, dict]:
    """Run one acquisition cycle for a tier or specific providers."""
    if tier and not providers:
        providers = PROVIDER_TIERS.get(tier, [])
    lookback = TIER_LOOKBACK.get(tier or "daily", 168)

    async with DuckDBStore(db_path) as store:
        return await run_acquisition(
            store,
            providers=providers,
            lookback_hours=lookback,
            max_stations=max_stations,
            concurrency=concurrency,
        )


async def run_daemon(
    db_path: str,
    schedule: str = "daily",
    tier: str | None = None,
    max_stations: int | None = None,
) -> None:
    """Run as a long-lived daemon, executing on a cron schedule."""
    cron_expr = DEFAULT_SCHEDULES.get(schedule, schedule)
    cron = croniter(cron_expr, datetime.now(UTC))
    stop = asyncio.Event()

    def _handle_signal(*_):
        logger.info("shutdown_signal_received")
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, _handle_signal)

    logger.info("daemon_started", schedule=cron_expr, tier=tier or "all")

    while not stop.is_set():
        next_run = cron.get_next(datetime)
        now = datetime.now(UTC)
        delay = max(0, (next_run - now).total_seconds())
        logger.info("next_run_scheduled", next_run=next_run.isoformat(), delay_s=int(delay))

        try:
            await asyncio.wait_for(stop.wait(), timeout=delay)
            break  # stop was set
        except TimeoutError:
            pass  # time to run

        logger.info("cycle_starting", tier=tier or "all")
        try:
            results = await run_scheduled_cycle(
                db_path, tier=tier, max_stations=max_stations,
            )
            ok = [s for s, r in results.items() if r.get("status") == "ok"]
            degraded = [s for s, r in results.items() if r.get("status") == "degraded"]
            errored = [s for s, r in results.items() if r.get("status") == "error"]
            total_obs = sum(r.get("observations", 0) for r in results.values())

            logger.info(
                "cycle_complete",
                ok=len(ok),
                degraded=len(degraded),
                errors=len(errored),
                observations=total_obs,
            )
            if degraded:
                logger.warning("degraded_providers", providers=degraded)
            if errored:
                logger.error("failed_providers", providers=errored)
        except Exception as e:
            logger.error("cycle_failed", error=str(e))
