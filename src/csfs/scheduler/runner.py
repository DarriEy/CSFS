"""Acquisition runner — orchestrates fetching across providers."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import structlog

from csfs.core.registry import discover, get_connector, list_providers
from csfs.store.base import BaseStore

logger = structlog.get_logger()


async def run_acquisition(
    store: BaseStore,
    providers: list[str] | None = None,
    lookback_hours: int = 48,
    max_stations: int | None = None,
) -> dict[str, dict]:
    """Run one acquisition cycle across selected (or all) providers."""
    discover()
    slugs = providers or list_providers()
    results: dict[str, dict] = {}

    for slug in slugs:
        log = logger.bind(provider=slug)
        try:
            connector_cls = get_connector(slug)
            async with connector_cls() as conn:
                log.info("fetching_stations")
                stations = await conn.fetch_stations()
                n_stations = await store.upsert_stations(stations)
                log.info("stations_synced", count=n_stations)

                total_obs = 0
                failed = 0
                end = datetime.now(UTC)
                start = end - timedelta(hours=lookback_hours)
                limit = max_stations or len(stations)

                for i, station in enumerate(stations[:limit]):
                    try:
                        latest = await store.get_latest_timestamp(station.id)
                        fetch_start = latest if latest else start
                        chunk = await conn.fetch_observations(station.id, fetch_start, end)
                        n_obs = await store.append_observations(chunk)
                        total_obs += n_obs
                    except Exception as e:
                        failed += 1
                        if failed <= 5:
                            log.warning("station_fetch_failed", station=station.id, error=str(e)[:80])
                        continue

                    if (i + 1) % 100 == 0:
                        log.info("progress", fetched=i + 1, total=limit, obs=total_obs, failed=failed)

                log.info(
                    "acquisition_complete",
                    stations=n_stations,
                    observations=total_obs,
                    fetched=min(limit, len(stations)),
                    failed=failed,
                )
                results[slug] = {
                    "stations": n_stations,
                    "observations": total_obs,
                    "fetched": min(limit, len(stations)),
                    "failed": failed,
                    "status": "ok",
                }

        except Exception as e:
            log.error("acquisition_failed", error=str(e))
            results[slug] = {"status": "error", "error": str(e)}

    return results
