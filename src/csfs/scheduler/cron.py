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
        "australia_bom", "spain_saih", "netherlands_rws",
        "ireland_epa", "denmark_dmihyd", "belgium_waterinfo",
        "belgium_spw", "hungary_ovf", "romania_inhga",
        "thailand_hii", "bangladesh_ffwc", "south_korea_wamis",
        "taiwan_wra", "lithuania_lhmt",
    ],
    "daily": [
        "finland_syke", "austria_ehyd", "czechia_chmu",
        "slovakia_shmu", "croatia_dhz", "slovenia_arso",
        "greece_openhi", "portugal_snirh", "estonia_ilmateenistus",
        "brazil_ana", "chile_dga", "argentina_snih",
        "colombia_ideam", "peru_senamhi", "mexico_conagua",
        "india_cwc", "japan_mlit", "south_africa_dws",
        "turkey_dsi", "newzealand_hilltop", "china_mwr",
        "italy_ispra", "italy_emilia", "italy_piedmont",
        "italy_tuscany", "germany_bavaria", "germany_bw",
        "germany_nrw", "uk_nrfa", "bosnia_fhmz",
        "iceland_lamahice", "ecuador_inamhi",
    ],
    "weekly": [
        "grdc", "estreams", "ca_discharge", "caravan", "gsim",
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
            ok = sum(1 for r in results.values() if r.get("status") == "ok")
            err = sum(1 for r in results.values() if r.get("status") == "error")
            total_obs = sum(r.get("observations", 0) for r in results.values())
            logger.info("cycle_complete", ok=ok, errors=err, observations=total_obs)
        except Exception as e:
            logger.error("cycle_failed", error=str(e))
