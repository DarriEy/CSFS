# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""CSFS command-line interface."""

from __future__ import annotations

import asyncio
from pathlib import Path

import click
import structlog

structlog.configure(
    processors=[
        structlog.dev.ConsoleRenderer(),
    ],
)


@click.group()
@click.option("--db", default="csfs.duckdb", help="Path to DuckDB database file")
@click.pass_context
def cli(ctx: click.Context, db: str) -> None:
    """CSFS — Community Streamflow Service."""
    ctx.ensure_object(dict)
    ctx.obj["db_path"] = Path(db)


@cli.command()
@click.option("--provider", "-p", multiple=True, help="Provider slug(s) to fetch. Omit for all.")
@click.option("--lookback", default=168, help="Hours of data to fetch per station (default: 168)")
@click.option(
    "--max-stations", "-n", default=None, type=int,
    help="Max stations to fetch obs for (default: all)",
)
@click.option("--tier", "-t", default=None, help="Fetch a predefined tier: realtime, hourly, daily, weekly")
@click.option("--concurrency", "-j", default=10, type=int, help="Max parallel station fetches (default: 10)")
@click.pass_context
def fetch(
    ctx: click.Context,
    provider: tuple[str, ...],
    lookback: int,
    max_stations: int | None,
    tier: str | None,
    concurrency: int,
) -> None:
    """Run one acquisition cycle."""
    from csfs.scheduler.cron import PROVIDER_TIERS, TIER_LOOKBACK
    from csfs.scheduler.runner import run_acquisition
    from csfs.store.duckdb_store import DuckDBStore

    if tier:
        target_providers = PROVIDER_TIERS.get(tier, [])
        lookback = TIER_LOOKBACK.get(tier, lookback)
        click.echo(f"Tier '{tier}': {len(target_providers)} providers, {lookback}h lookback")
    elif provider:
        target_providers = list(provider)
    else:
        target_providers = None

    async def _run():
        async with DuckDBStore(ctx.obj["db_path"]) as store:
            results = await run_acquisition(
                store,
                providers=target_providers,
                lookback_hours=lookback,
                max_stations=max_stations,
                concurrency=concurrency,
            )
            total_stations = 0
            total_obs = 0
            for slug, info in results.items():
                status = info.get("status", "unknown")
                total_stations += info.get("stations", 0)
                total_obs += info.get("observations", 0)
                if status == "ok":
                    click.echo(
                        f"  {slug}: {info['stations']} stations, "
                        f"{info['observations']} obs "
                        f"({info['fetched']} queried, {info['failed']} failed)"
                    )
                elif status == "degraded":
                    click.echo(
                        f"  {slug}: DEGRADED — {info.get('observations', 0)} obs, "
                        f"{info.get('failed', 0)}/{info.get('fetched', 0)} failed"
                    )
                else:
                    click.echo(f"  {slug}: ERROR — {info.get('error', '?')}")
            click.echo(f"\nTotal: {total_stations} stations, {total_obs} observations")

    asyncio.run(_run())


@cli.command()
@click.option(
    "--schedule", "-s", default="daily",
    help="Cron schedule: realtime, hourly, daily, weekly, or a cron expression",
)
@click.option("--tier", "-t", default=None, help="Provider tier to run")
@click.option("--max-stations", "-n", default=None, type=int)
@click.pass_context
def daemon(ctx: click.Context, schedule: str, tier: str | None, max_stations: int | None) -> None:
    """Run as a long-lived daemon on a cron schedule."""
    from csfs.scheduler.cron import run_daemon

    asyncio.run(run_daemon(
        str(ctx.obj["db_path"]),
        schedule=schedule,
        tier=tier,
        max_stations=max_stations,
    ))


@cli.command()
@click.pass_context
def providers(ctx: click.Context) -> None:
    """List registered providers."""
    from csfs.core.registry import discover, list_providers
    from csfs.scheduler.cron import PROVIDER_TIERS

    discover()
    all_slugs = list_providers()

    tier_lookup = {}
    for tier_name, slugs in PROVIDER_TIERS.items():
        for s in slugs:
            tier_lookup[s] = tier_name

    click.echo(f"  {'PROVIDER':<30s}  {'TIER':<10s}")
    click.echo(f"  {'─' * 30}  {'─' * 10}")
    for slug in all_slugs:
        tier = tier_lookup.get(slug, "?")
        click.echo(f"  {slug:<30s}  {tier:<10s}")
    click.echo(f"\n  {len(all_slugs)} providers registered")


@cli.command()
@click.option("--provider", "-p", default=None)
@click.option("--country", "-c", default=None)
@click.pass_context
def stations(ctx: click.Context, provider: str | None, country: str | None) -> None:
    """List stations in the local database."""
    from csfs.store.duckdb_store import DuckDBStore

    async def _run():
        async with DuckDBStore(ctx.obj["db_path"]) as store:
            result = await store.get_stations(provider=provider, country_code=country)
            click.echo(f"Found {len(result)} stations")
            for s in result[:20]:
                click.echo(f"  {s.id:30s}  {s.name:40s}  {s.country_code}")
            if len(result) > 20:
                click.echo(f"  ... and {len(result) - 20} more")

    asyncio.run(_run())


@cli.command()
@click.option("--history", "-h", default=0, type=int, help="Show last N acquisition runs per provider")
@click.pass_context
def status(ctx: click.Context, history: int) -> None:
    """Show database status and provider coverage."""
    import duckdb

    db = str(ctx.obj["db_path"])
    try:
        conn = duckdb.connect(db, read_only=True)
    except Exception:
        click.echo(f"No database found at {db}")
        return

    r = conn.execute("SELECT COUNT(*) FROM stations").fetchone()
    total_stations = r[0] if r else 0
    r = conn.execute("SELECT COUNT(*) FROM observations").fetchone()
    total_obs = r[0] if r else 0
    click.echo(f"\n  Database: {db}")
    click.echo(f"  Stations: {total_stations:,}")
    click.echo(f"  Observations: {total_obs:,}")

    time_range = conn.execute("SELECT MIN(timestamp), MAX(timestamp) FROM observations").fetchone()
    if time_range and time_range[0]:
        click.echo(f"  Time range: {time_range[0]} → {time_range[1]}")

    click.echo(f"\n  {'PROVIDER':<25s}  {'STATIONS':>8s}  {'OBS':>10s}  {'LATEST':>20s}  {'STATUS'}")
    click.echo(f"  {'─' * 25}  {'─' * 8}  {'─' * 10}  {'─' * 20}  {'─' * 8}")
    now = conn.execute("SELECT CURRENT_TIMESTAMP AT TIME ZONE 'UTC'").fetchone()[0]
    for row in conn.execute("""
        SELECT s.provider, COUNT(DISTINCT s.id), COUNT(o.station_id),
               MAX(o.fetched_at)
        FROM stations s LEFT JOIN observations o ON o.station_id = s.id
        GROUP BY s.provider ORDER BY COUNT(o.station_id) DESC
    """).fetchall():
        provider, n_stations, n_obs, latest = row
        if latest:
            latest_naive = latest.replace(tzinfo=None) if hasattr(latest, 'replace') else latest
            now_naive = now.replace(tzinfo=None) if hasattr(now, 'replace') else now
            age_hours = (now_naive - latest_naive).total_seconds() / 3600
            age_str = str(latest)[:16]
            if age_hours > 168:
                health = "STALE"
            elif n_obs == 0:
                health = "EMPTY"
            else:
                health = "ok"
        else:
            age_str = "—"
            health = "EMPTY" if n_stations > 0 else "—"
        click.echo(f"  {provider:<25s}  {n_stations:>8,}  {n_obs:>10,}  {age_str:>20s}  {health}")

    r = conn.execute("SELECT COUNT(DISTINCT country_code) FROM stations").fetchone()
    countries = r[0] if r else 0
    click.echo(f"\n  {countries} countries represented")

    has_acq_log = conn.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_name = 'acquisition_log'"
    ).fetchone()[0]
    if not has_acq_log:
        conn.close()
        return

    acq_count = conn.execute("SELECT COUNT(*) FROM acquisition_log").fetchone()[0]
    if acq_count == 0:
        conn.close()
        return

    click.echo(f"\n  Acquisition health ({acq_count} runs logged)")
    click.echo(
        f"  {'PROVIDER':<25s}  {'LAST RUN':>10s}  {'STATUS':<10s}  "
        f"{'TREND':<12s}  {'SINCE OK':>10s}"
    )
    click.echo(
        f"  {'─' * 25}  {'─' * 10}  {'─' * 10}  "
        f"{'─' * 12}  {'─' * 10}"
    )

    now_naive = now.replace(tzinfo=None) if hasattr(now, 'replace') else now
    providers_acq = conn.execute(
        "SELECT DISTINCT provider FROM acquisition_log ORDER BY provider"
    ).fetchall()

    for (prov,) in providers_acq:
        rows = conn.execute(
            "SELECT status, failed, fetched, started_at FROM acquisition_log "
            "WHERE provider = ? ORDER BY started_at DESC LIMIT 5",
            [prov],
        ).fetchall()
        if not rows:
            continue

        last_status = rows[0][0]
        last_started = rows[0][3]
        last_naive = last_started.replace(tzinfo=None) if hasattr(last_started, 'replace') else last_started
        age_h = (now_naive - last_naive).total_seconds() / 3600
        if age_h < 1:
            last_ago = f"{int(age_h * 60)}m ago"
        elif age_h < 48:
            last_ago = f"{int(age_h)}h ago"
        else:
            last_ago = f"{int(age_h / 24)}d ago"

        last_ok = conn.execute(
            "SELECT started_at FROM acquisition_log "
            "WHERE provider = ? AND status = 'ok' ORDER BY started_at DESC LIMIT 1",
            [prov],
        ).fetchone()
        if last_ok:
            ok_naive = last_ok[0].replace(tzinfo=None) if hasattr(last_ok[0], 'replace') else last_ok[0]
            ok_h = (now_naive - ok_naive).total_seconds() / 3600
            if ok_h < 1:
                since_ok = f"{int(ok_h * 60)}m ago"
            elif ok_h < 48:
                since_ok = f"{int(ok_h)}h ago"
            else:
                since_ok = f"{int(ok_h / 24)}d ago"
        else:
            since_ok = "never"

        if len(rows) >= 3:
            fail_rates = [r[1] / max(r[2], 1) for r in rows]
            if fail_rates[0] > fail_rates[-1] + 0.1:
                trend = "worsening"
            elif fail_rates[0] < fail_rates[-1] - 0.1:
                trend = "improving"
            else:
                trend = "stable"
        else:
            trend = "—"

        click.echo(
            f"  {prov:<25s}  {last_ago:>10s}  {last_status:<10s}  "
            f"{trend:<12s}  {since_ok:>10s}"
        )

    if history > 0:
        click.echo(f"\n  Detailed history (last {history} per provider)")
        click.echo(
            f"  {'PROVIDER':<20s}  {'STARTED':<20s}  {'STATUS':<8s}  "
            f"{'DUR':>5s}  {'STA':>5s}  {'OBS':>8s}  "
            f"{'FAIL':>5s}  {'RETRY':>5s}  {'RECOV':>5s}  ERROR"
        )
        sep = f"  {'─' * 20}  {'─' * 20}  {'─' * 8}  {'─' * 5}  {'─' * 5}  {'─' * 8}"
        click.echo(f"{sep}  {'─' * 5}  {'─' * 5}  {'─' * 5}  {'─' * 20}")
        for (prov,) in providers_acq:
            rows = conn.execute(
                "SELECT provider, started_at, status, duration_s, stations, "
                "observations, failed, retried, recovered, error_message "
                "FROM acquisition_log WHERE provider = ? "
                "ORDER BY started_at DESC LIMIT ?",
                [prov, history],
            ).fetchall()
            for row in rows:
                p, sa, st, dur, sta, obs, fail, retr, recov, err = row
                sa_str = str(sa)[:16]
                dur_str = f"{dur:.0f}s"
                err_str = (err[:30] + "…") if err and len(err) > 30 else (err or "")
                click.echo(
                    f"  {p:<20s}  {sa_str:<20s}  {st:<8s}  "
                    f"{dur_str:>5s}  {sta:>5}  {obs:>8}  "
                    f"{fail:>5}  {retr:>5}  {recov:>5}  {err_str}"
                )

    conn.close()


@cli.command()
@click.option("--host", default="0.0.0.0")
@click.option("--port", default=8000, type=int)
@click.pass_context
def serve(ctx: click.Context, host: str, port: int) -> None:
    """Start the CSFS API server."""
    import uvicorn

    from csfs.api.app import create_app

    app = create_app(ctx.obj["db_path"])
    uvicorn.run(app, host=host, port=port)
