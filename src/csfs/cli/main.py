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
@click.option("--lookback", default=48, help="Hours of data to fetch per station (default: 48)")
@click.option("--max-stations", "-n", default=None, type=int, help="Max stations to fetch obs for (default: all)")
@click.pass_context
def fetch(ctx: click.Context, provider: tuple[str, ...], lookback: int, max_stations: int | None) -> None:
    """Run one acquisition cycle."""
    from csfs.scheduler.runner import run_acquisition
    from csfs.store.duckdb_store import DuckDBStore

    async def _run():
        async with DuckDBStore(ctx.obj["db_path"]) as store:
            results = await run_acquisition(
                store,
                providers=list(provider) if provider else None,
                lookback_hours=lookback,
                max_stations=max_stations,
            )
            for slug, info in results.items():
                status = info.get("status", "unknown")
                if status == "ok":
                    click.echo(
                        f"  {slug}: {info['stations']} stations synced, "
                        f"{info['observations']} obs fetched "
                        f"({info['fetched']} stations queried, {info['failed']} failed)"
                    )
                else:
                    click.echo(f"  {slug}: ERROR — {info.get('error', '?')}")

    asyncio.run(_run())


@cli.command()
@click.pass_context
def providers(ctx: click.Context) -> None:
    """List registered providers."""
    from csfs.core.registry import discover, list_providers

    discover()
    for slug in list_providers():
        click.echo(f"  {slug}")


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
@click.option("--host", default="0.0.0.0")
@click.option("--port", default=8000, type=int)
@click.pass_context
def serve(ctx: click.Context, host: str, port: int) -> None:
    """Start the CSFS API server."""
    import uvicorn

    from csfs.api.app import create_app

    app = create_app(ctx.obj["db_path"])
    uvicorn.run(app, host=host, port=port)
