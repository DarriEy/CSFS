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
        "belgium_waterinfo", "netherlands_rws", "thailand_hii",
        "slovenia_arso", "denmark_dmihyd", "croatia_dhz",
        "germany_bw", "germany_bavaria", "italy_emilia",
        "wmo_whos_plata",
        "bulgaria_eaemdr",
    ],
    "hourly": [
        "norway_nve", "sweden_smhi", "switzerland_bafu",
        "australia_bom", "finland_syke", "greece_openhi",
        "newzealand_hilltop",
        "lithuania_lhmt", "austria_ehyd",
        "camelsh", "germany_berlin",
    ],
    "daily": [
        "argentina_snih",
        "japan_mlit",
        "iceland_lamahice", "ecuador_inamhi",
        "pakistan_wapda",
        "panama_stri",
        "bolivia_ine", "bulgaria_nimh",
        "israel_caravan", "czechia_chmu",
        "scotland_sepa", "belgium_spw", "taiwan_wra",
        "glofas", "wmo_whos", "geoglows",
        "southafrica_dws",
    ],
    "weekly": [
        "grdc", "estreams", "ca_discharge", "caravan", "gsim",
        "lamah_ce", "sierem", "adhi", "russia_arcticnet",
        "spain_cedex", "brazil_ana", "uk_nrfa", "ireland_epa",
        "camels_de", "camels_in", "camels_co",
        "camels_dk", "camels_br", "camels_cl", "camels_ind", "camels_ch",
        "camels_aus", "camels_us", "camels_gb", "camels_se", "camels_fr",
        "camels_nz", "camels_fi", "camels_lux", "hysets", "camels_pe",
        "camels_col", "camels_spat",
        "robin", "caravan_grdc",
        "germany_nrw",
        "chile_cr2", "portugal_snirh", "vietnam_mekong",
    ],
}

TIER_LOOKBACK: dict[str, int] = {
    "realtime": 4,
    "hourly": 48,
    "daily": 168,
    "weekly": 720,
}

# Freshness expectations for health classification ("stale" threshold in
# hours). The weekly tier hosts archive-class sources that publish annually
# (uk_nrfa: gauged daily flow lands ~once a year) or consolidate with
# months of lag (brazil_ana) — flagging those against a 7-day horizon is
# permanent noise, not signal. Live tiers keep the historical 7-day default.
TIER_STALE_AFTER: dict[str, float] = {
    "realtime": 168.0,
    "hourly": 168.0,
    "daily": 168.0,
    "weekly": 26280.0,  # 3 years — tolerates annual archives + consolidation lag
}

# Per-provider overrides (take precedence over the tier value).
PROVIDER_STALE_AFTER: dict[str, float] = {
    # CR2 explorador re-serves a DGA snapshot frozen at 2020-06-05; the
    # connector delivers everything the source offers, so "fresh" here
    # means "the archive is loaded", not "recent data exists".
    "chile_cr2": 87600.0,  # 10 years
    # EIDC Mekong ratings archive is static and ends 2017-09.
    "vietnam_mekong": 105120.0,  # 12 years
}


def stale_after_by_provider() -> dict[str, float]:
    """Per-provider staleness thresholds derived from tiers + overrides."""
    thresholds: dict[str, float] = {}
    for tier, slugs in PROVIDER_TIERS.items():
        tier_hours = TIER_STALE_AFTER.get(tier, 168.0)
        for slug in slugs:
            thresholds[slug] = tier_hours
    thresholds.update(PROVIDER_STALE_AFTER)
    return thresholds


async def run_scheduled_cycle(
    db_path: str,
    tier: str | None = None,
    providers: list[str] | None = None,
    max_stations: int | None = None,
    concurrency: int = 10,
    provider_configs: dict[str, dict] | None = None,
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
            provider_configs=provider_configs,
        )


async def run_daemon(
    db_path: str,
    schedule: str = "daily",
    tier: str | None = None,
    max_stations: int | None = None,
    provider_configs: dict[str, dict] | None = None,
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
                provider_configs=provider_configs,
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
