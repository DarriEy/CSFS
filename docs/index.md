# CSFS — Community Streamflow Service

**Live acquisition and harmonization of global streamflow observations.**

CSFS connects to open streamflow data providers around the world — national
hydrological agencies, regional networks, research archives, and global model
products — harmonizes their observations into one canonical station/observation
schema (discharge in m³/s, timestamps in UTC), and maintains a near-real-time
DuckDB store with scheduled acquisition, health monitoring, a CLI, and a
FastAPI read layer.

## Statement of need

Despite decades of open-data progress, programmatic access to river discharge
observations remains fragmented. The community's workhorses are either *static
archives* — GRDC exports, Caravan, GSIM, EStreams, the CAMELS family — which
are invaluable for retrospective studies but frozen at publication time, or
*single-agency clients* such as USGS `dataretrieval` and `hydrofunctions`,
which cover one network each and leave the remaining hundred-plus agencies to
bespoke, per-project scraping code. A researcher who needs current discharge
for basins spanning, say, France, Brazil, and Japan must learn three APIs,
three formats, three unit conventions, and three quality-flag vocabularies.
CSFS addresses this gap with a single interface for *live, multi-provider*
streamflow acquisition: one connector per agency, every observation normalized
to a common schema, scheduled re-acquisition tuned to each provider's update
cadence, and per-connector health monitoring. Critically, the breadth of the
roster is kept honest mechanically: CI-enforced integrity tests require every
registered connector to have test coverage, a scheduler tier, and a truthful
inventory status, so the provider catalog cannot drift from what the code
actually does.

## What you get

- **86 provider connectors** in code — 78 carry inventory status
  `implemented`; 8 remain labeled `research` while their upstream data paths
  are validated. The full inventory catalogs 104 sources. See the
  [Provider Catalog](catalog.md) for the honest breakdown.
- **Canonical data model** — `Station`, `Observation`, `TimeSeriesChunk`
  (pydantic), discharge in m³/s, UTC timestamps, ISO country codes.
- **DuckDB store** — portable single-file analytics database with station
  upserts, deduplicated observation appends, and an acquisition log.
- **Scheduler tiers** — realtime / hourly / daily / weekly cron tiers with
  per-tier lookback windows and a long-lived daemon mode.
- **Health monitoring** — per-connector data freshness and run-history
  reporting, with a `--fail-on` mode for cron-driven alerting.
- **Three ways in** — the [`csfs` CLI](cli.md), the
  [Python API](python-api.md), and a FastAPI read layer (`csfs serve`).

## Where to next

- New here? Start with the [Quick Start](quickstart.md).
- Scripting or analyzing? See the [Python API](python-api.md) and the
  [API Reference](reference.md).
- Want the full provider list? See the [Provider Catalog](catalog.md).
- Curious how it holds together? Read the [Architecture](architecture.md) —
  including the roster-integrity guard system that keeps the catalog honest.

## License

GPL-3.0-or-later. Source on [GitHub](https://github.com/DarriEy/CSFS).
