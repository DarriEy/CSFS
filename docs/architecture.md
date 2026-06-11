# Architecture

CSFS is a pipeline from heterogeneous provider APIs to one queryable store:

```
provider APIs ──> connectors ──> canonical models ──> DuckDB store
                     │            (Station /            │
                     │             Observation)         │
                     │                                  │
              scheduler tiers ──────────────> health monitoring
              (realtime/hourly/                (csfs health,
               daily/weekly)                    acquisition log)
```

```
src/csfs/
  connectors/     One module per provider (86 registered), all subclassing BaseConnector
  core/           Canonical models, registry, config, exceptions, health logic
  store/          Persistence (DuckDBStore is the default backend)
  scheduler/      Acquisition runner + cron tiers + daemon
  api/            FastAPI read layer
  cli/            The csfs command
inventory/        providers.yaml — the documented catalog of 104 sources
tests/            Hermetic test suite + roster-integrity guards
```

## Connectors

A connector is one module in `src/csfs/connectors/`, subclassing
`BaseConnector` and registered under a slug:

```python
@register("usgs")
class USGSConnector(BaseConnector):
    slug = "usgs"
    display_name = "USGS NWIS"
    base_url = "https://waterservices.usgs.gov/nwis"
    country_codes = ["US"]
```

The base class provides the async HTTP client lifecycle, retry with
exponential backoff (rate limits, connection errors, timeouts), optional
per-host concurrency caps, and canonical station-ID construction
(`"<slug>:<native_id>"`). Subclasses implement two methods:
`fetch_stations()` and `fetch_observations(station_id, start, end)`.
`csfs.core.registry.discover()` imports every connector module so
registration is automatic.

## Canonical data model

All providers converge on three pydantic models (`csfs.core.models`):
`Station` (provider-agnostic metadata, ISO country codes), `Observation`
(**discharge in m³/s, timestamp in UTC**, a five-value quality flag), and
`TimeSeriesChunk` (one fetch's batch with `fetched_at` provenance). Unit
conversion (cfs, l/s, ...) and timezone normalization happen inside each
connector, so nothing downstream ever sees provider-native conventions.

## DuckDB store

`DuckDBStore` persists everything in a single portable file: a `stations`
table (upserted on every cycle), an `observations` table (append-only with
`(station_id, timestamp)` dedup via an anti-join staging step), and an
`acquisition_log` recording every run (status, counts, duration, error).
Being plain DuckDB, the file is directly queryable from SQL, pandas, Arrow,
or R without CSFS in the loop.

## Scheduler tiers

Every registered connector belongs to **exactly one** tier in
`PROVIDER_TIERS` (`src/csfs/scheduler/cron.py`), matching the provider's
update cadence:

| Tier | Schedule | Lookback | Typical members |
| --- | --- | --- | --- |
| `realtime` | every 15 min | 4 h | USGS, UK EA, Hub'Eau, PEGELONLINE, ... |
| `hourly` | hourly at :05 | 48 h | NVE, SMHI, BAFU, BoM, ... |
| `daily` | daily 02:30 | 168 h | agencies with daily updates, GloFAS, GEOGLOWS |
| `weekly` | Sunday 03:00 | 720 h | archive datasets: GRDC, Caravan, GSIM, EStreams, CAMELS, ... |

The acquisition runner (`run_acquisition`) fetches stations, then
observations concurrently in batches, retries failed stations once at lower
concurrency, fetches **incrementally** (from each station's latest stored
timestamp), and classifies the run `ok` / `degraded` / `error`. The daemon
(`csfs daemon`) wraps this in a croniter loop with clean signal handling.

## Health monitoring

Two views are merged per connector (`csfs.core.health`, surfaced as
`csfs health`): *data health* from the store (fresh / `stale` / `empty` /
`none`) and *run health* from the acquisition log (last status, success
rate, time since last OK). With `--fail-on stale,error` the CLI exits
non-zero, so a cron wrapper can alert when a connector goes dark. A
scheduled GitHub Actions workflow runs acquisition tiers and publishes a
database snapshot, using the same health gate.

## Roster-integrity guards

The signature feature of the codebase is that **the provider roster cannot
silently rot or overclaim**. `tests/test_connector_integrity.py` runs in CI
and mechanically enforces:

- **Every connector is tested.** Each registered slug must have its own
  `tests/connectors/test_<slug>.py` or be referenced by slug inside another
  connector test (covering variant connectors exercised by a shared parent
  test). A batch of connectors cannot sneak into the roster while the suite
  stays green.
- **Exactly one cron tier per connector.** An orphaned connector would never
  be acquired; a duplicated one would be acquired twice. Both fail CI, as do
  *ghost tiers* — tier entries naming a slug that is no longer registered.
- **Complete metadata.** Every connector class must expose a `slug` matching
  its registry key, a `display_name`, a `base_url`, and ISO 3166-1 alpha-2
  `country_codes` (or the `"global"` sentinel).
- **Documented in the inventory.** Every registered connector needs an entry
  in `inventory/providers.yaml` — no undocumented live sources.
- **No overclaiming.** An inventory entry may carry `status: implemented`
  *only* if its slug is actually registered; everything else must be honestly
  labeled `planned`, `research`, `manual`, or `fallback`. The
  [Provider Catalog](catalog.md) numbers are therefore trustworthy by
  construction.
- **Valid committed config.** Provider keys in the repo-root `csfs.yaml`
  must reference registered connectors, so a typo cannot silently no-op.

These guards complement the **hermetic test policy**: an autouse fixture in
`tests/conftest.py` blocks DNS resolution for non-local hosts, so every
connector test must mock its HTTP (respx) and the offline suite
(`pytest -m "not network"`) can never quietly depend on a live upstream.
Tests that intentionally reach a real API are marked
`@pytest.mark.network` and excluded in CI.

## API layer

`csfs serve` exposes the store over HTTP via FastAPI (read-only connection),
for dashboards or remote clients that should not touch the DuckDB file
directly.
