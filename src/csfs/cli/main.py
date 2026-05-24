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
@click.pass_context
def fetch(
    ctx: click.Context,
    provider: tuple[str, ...],
    lookback: int,
    max_stations: int | None,
    tier: str | None,
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
            )
            total_stations = 0
            total_obs = 0
            for slug, info in results.items():
                status = info.get("status", "unknown")
                if status == "ok":
                    total_stations += info["stations"]
                    total_obs += info["observations"]
                    click.echo(
                        f"  {slug}: {info['stations']} stations, "
                        f"{info['observations']} obs "
                        f"({info['fetched']} queried, {info['failed']} failed)"
                    )
                else:
                    click.echo(f"  {slug}: ERROR — {info.get('error', '?')}")
            click.echo(f"
Total: {total_stations} stations, {total_obs} observations")

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
    click.echo(f"
  {len(all_slugs)} providers registered")


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
@click.pass_context
def status(ctx: click.Context) -> None:
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
    click.echo(f"
  Database: {db}")
    click.echo(f"  Stations: {total_stations:,}")
    click.echo(f"  Observations: {total_obs:,}")

    time_range = conn.execute("SELECT MIN(timestamp), MAX(timestamp) FROM observations").fetchone()
    if time_range and time_range[0]:
        click.echo(f"  Time range: {time_range[0]} → {time_range[1]}")

    click.echo(f"
  {'PROVIDER':<25s}  {'STATIONS':>8s}  {'OBS':>10s}")
    click.echo(f"  {'─' * 25}  {'─' * 8}  {'─' * 10}")
    for row in conn.execute("""
        SELECT s.provider, COUNT(DISTINCT s.id), COUNT(o.station_id)
        FROM stations s LEFT JOIN observations o ON o.station_id = s.id
        GROUP BY s.provider ORDER BY COUNT(o.station_id) DESC
    """).fetchall():
        click.echo(f"  {row[0]:<25s}  {row[1]:>8,}  {row[2]:>10,}")

    r = conn.execute("SELECT COUNT(DISTINCT country_code) FROM stations").fetchone()
    countries = r[0] if r else 0
    click.echo(f"
  {countries} countries represented")
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
