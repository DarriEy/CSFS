# API Reference

Generated from the source with mkdocstrings. The stable, user-facing surface
is shown here; connector internals are documented in their modules.

## Facade conveniences

Top-level helpers on the `import csfs` facade (which also re-exports
everything below — models, store, runner, registry).

::: csfs.open_store

::: csfs.fetch_observations

::: csfs.fetch_observations_sync

## Canonical models

::: csfs.core.models.Station

::: csfs.core.models.Observation

::: csfs.core.models.TimeSeriesChunk

::: csfs.core.models.QualityFlag

## Store

::: csfs.store.duckdb_store.DuckDBStore

## Acquisition

::: csfs.scheduler.runner.run_acquisition

::: csfs.scheduler.cron.run_scheduled_cycle

::: csfs.scheduler.cron.run_daemon

## Registry

::: csfs.core.registry.register

::: csfs.core.registry.get_connector

::: csfs.core.registry.list_providers

::: csfs.core.registry.discover

## Connector base class

::: csfs.connectors.base.BaseConnector

## Health

::: csfs.core.health
