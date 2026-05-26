# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Acquisition runner — orchestrates fetching across providers."""

from __future__ import annotations

import asyncio
import time
from datetime import UTC, datetime, timedelta

import structlog

from csfs.core.registry import discover, get_connector, list_providers
from csfs.store.base import BaseStore

logger = structlog.get_logger()

DEFAULT_CONCURRENCY = 10


async def run_acquisition(
    store: BaseStore,
    providers: list[str] | None = None,
    lookback_hours: int = 48,
    max_stations: int | None = None,
    concurrency: int = DEFAULT_CONCURRENCY,
) -> dict[str, dict]:
    """Run one acquisition cycle across selected (or all) providers."""
    discover()
    slugs = providers or list_providers()
    results: dict[str, dict] = {}

    for slug in slugs:
        log = logger.bind(provider=slug)
        t0 = time.monotonic()
        started_at = datetime.now(UTC)
        try:
            connector_cls = get_connector(slug)
            async with connector_cls() as conn:
                log.info("fetching_stations")
                stations = await conn.fetch_stations()
                if not stations:
                    log.warning("no_stations_discovered")
                n_stations = await store.upsert_stations(stations)
                log.info("stations_synced", count=n_stations)

                total_obs = 0
                failed = 0
                failed_stations: list = []
                end = datetime.now(UTC)
                start = end - timedelta(hours=lookback_hours)
                limit = max_stations or len(stations)
                target_stations = stations[:limit]

                sem = asyncio.Semaphore(concurrency)

                async def _fetch_one(station):
                    async with sem:
                        latest = await store.get_latest_timestamp(station.id)
                        fetch_start = latest if latest else start
                        return await conn.fetch_observations(
                            station.id, fetch_start, end,
                        )

                batch_size = max(concurrency * 5, 50)
                for batch_start in range(0, len(target_stations), batch_size):
                    batch = target_stations[batch_start:batch_start + batch_size]
                    tasks = [_fetch_one(s) for s in batch]
                    batch_results = await asyncio.gather(
                        *tasks, return_exceptions=True,
                    )

                    for station, result in zip(batch, batch_results):
                        if isinstance(result, BaseException):
                            failed += 1
                            failed_stations.append(station)
                            if failed <= 5:
                                log.warning(
                                    "station_fetch_failed",
                                    station=station.id,
                                    error=str(result)[:80],
                                )
                        else:
                            n_obs = await store.append_observations(result)
                            total_obs += n_obs

                    done = min(batch_start + len(batch), len(target_stations))
                    log.info(
                        "progress",
                        fetched=done,
                        total=len(target_stations),
                        obs=total_obs,
                        failed=failed,
                    )

                retried = 0
                recovered = 0
                if failed_stations:
                    retried = len(failed_stations)
                    log.info("retrying_failed_stations", count=retried)
                    retry_sem = asyncio.Semaphore(max(concurrency // 2, 2))

                    async def _retry_one(station, _sem=retry_sem, _start=start, _end=end):
                        async with _sem:
                            latest = await store.get_latest_timestamp(station.id)
                            fetch_start = latest if latest else _start
                            return await conn.fetch_observations(
                                station.id, fetch_start, _end,
                            )

                    retry_results = await asyncio.gather(
                        *[_retry_one(s) for s in failed_stations],
                        return_exceptions=True,
                    )
                    for station, result in zip(failed_stations, retry_results):
                        if isinstance(result, BaseException):
                            log.warning(
                                "station_retry_failed",
                                station=station.id,
                                error=str(result)[:80],
                            )
                        else:
                            recovered += 1
                            failed -= 1
                            n_obs = await store.append_observations(result)
                            total_obs += n_obs

                    if recovered:
                        log.info("retry_recovered", recovered=recovered, still_failed=failed)

                fetched = len(target_stations)

                if failed > 5:
                    log.warning("station_failures_summary", failed=failed, fetched=fetched)

                if n_stations > 0 and total_obs == 0 and failed < fetched:
                    log.warning("zero_observations", stations=n_stations, fetched=fetched)

                if fetched > 0 and failed == fetched:
                    status = "error"
                elif failed > 0 or (n_stations > 0 and total_obs == 0):
                    status = "degraded"
                else:
                    status = "ok"

                duration_s = time.monotonic() - t0
                log.info(
                    "acquisition_complete",
                    stations=n_stations,
                    observations=total_obs,
                    fetched=fetched,
                    failed=failed,
                    retried=retried,
                    recovered=recovered,
                    duration_s=round(duration_s, 1),
                    status=status,
                )
                results[slug] = {
                    "stations": n_stations,
                    "observations": total_obs,
                    "fetched": fetched,
                    "failed": failed,
                    "retried": retried,
                    "recovered": recovered,
                    "status": status,
                }

                try:
                    await store.record_acquisition(
                        provider=slug,
                        started_at=started_at,
                        duration_s=duration_s,
                        status=status,
                        stations=n_stations,
                        observations=total_obs,
                        fetched=fetched,
                        failed=failed,
                        retried=retried,
                        recovered=recovered,
                    )
                except Exception:
                    log.warning("acquisition_log_write_failed", exc_info=True)

        except Exception as e:
            duration_s = time.monotonic() - t0
            log.error("acquisition_failed", error=str(e))
            results[slug] = {"status": "error", "error": str(e)}
            try:
                await store.record_acquisition(
                    provider=slug,
                    started_at=started_at,
                    duration_s=duration_s,
                    status="error",
                    error_message=str(e)[:500],
                )
            except Exception:
                log.warning("acquisition_log_write_failed", exc_info=True)

    return results
