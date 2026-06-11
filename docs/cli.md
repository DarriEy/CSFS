# CLI

The `csfs` command drives acquisition, inspection, and serving. Global
options come *before* the subcommand:

| Option | Default | Meaning |
| --- | --- | --- |
| `--db PATH` | `csfs.duckdb` | DuckDB database file to read/write |
| `-c, --config PATH` | — | YAML config file (per-provider settings, API keys) |
| `--version` | — | Print the installed CSFS version |

```bash
csfs --db /data/streamflow.duckdb -c csfs.yaml fetch -p usgs
```

## `csfs fetch`

Run one acquisition cycle: discover stations, fetch observations, write to
the store, and record the run in the acquisition log.

| Option | Default | Meaning |
| --- | --- | --- |
| `-p, --provider SLUG` | all | Provider slug(s); repeatable |
| `-t, --tier NAME` | — | Fetch a whole tier: `realtime`, `hourly`, `daily`, `weekly` |
| `--lookback HOURS` | 168 | Hours of data to fetch per station |
| `-n, --max-stations N` | all | Cap stations per provider |
| `-j, --concurrency N` | 10 | Parallel station fetches |

```bash
csfs fetch -p usgs -p uk_ea --lookback 48 -n 100
csfs fetch --tier realtime          # tier lookback applied automatically
```

## `csfs daemon`

Long-lived scheduler: sleeps until the next cron tick, runs a cycle, repeats.
Stops cleanly on SIGINT/SIGTERM.

| Option | Default | Meaning |
| --- | --- | --- |
| `-s, --schedule` | `daily` | `realtime`, `hourly`, `daily`, `weekly`, or a raw cron expression |
| `-t, --tier NAME` | all | Restrict to one provider tier |
| `-n, --max-stations N` | all | Cap stations per provider |

```bash
csfs daemon -s realtime -t realtime
csfs daemon -s "0 */6 * * *"        # custom cron: every 6 hours
```

## `csfs providers`

List every registered connector and its scheduler tier.

```bash
csfs providers
```

## `csfs stations`

List stations in the local database (first 20 shown).

| Option | Meaning |
| --- | --- |
| `-p, --provider SLUG` | Filter by provider |
| `-c, --country CODE` | Filter by ISO 3166-1 alpha-2 country code |

```bash
csfs stations -p france_hubeau
csfs stations -c IS
```

## `csfs status`

Database overview: station/observation counts, time range, per-provider
coverage and freshness, and acquisition-run health.

| Option | Meaning |
| --- | --- |
| `-h, --history N` | Also show the last N acquisition runs per provider |

```bash
csfs status
csfs status -h 5
```

## `csfs health`

Per-connector health report combining stored-data freshness (`ok` / `stale` /
`empty` / `none`) with acquisition-log history (last run status, success
rate). Designed for cron-driven alerting: with `--fail-on`, the exit code is
non-zero when any connector matches.

| Option | Default | Meaning |
| --- | --- | --- |
| `--stale-hours H` | 168 | Data older than this counts as stale |
| `-p, --provider SLUG` | — | Show only this provider |
| `-t, --tier NAME` | — | Scope to one tier's providers |
| `--degraded-only` | off | Show only degraded connectors |
| `--json` | off | Machine-readable output |
| `--fail-on LIST` | — | Comma-separated states forcing exit 1, e.g. `stale,empty,error` |

```bash
csfs health
csfs health --tier realtime --degraded-only
csfs health --json --fail-on stale,error    # for cron alerts
```

## `csfs download-data`

Download the offline archive datasets used by local-file connectors (GRDC,
Caravan, GSIM, EStreams, CAMELS variants, ...). Some are auto-downloadable;
others require a manual step (registration or license acceptance) and the
command prints where to get them and where to put them.

| Option | Default | Meaning |
| --- | --- | --- |
| `-d, --dataset SLUG` | all | Dataset(s) to download; repeatable |
| `--dest DIR` | `data/datasets` | Base directory |
| `--list` | — | List available datasets, sizes, auto/manual mode |
| `--dry-run` | — | Show what would be downloaded |

```bash
csfs download-data --list
csfs download-data -d caravan -d gsim
```

## `csfs serve`

Start the FastAPI read layer over the local database
(requires `pip install "csfs[api]"`).

| Option | Default |
| --- | --- |
| `--host` | `0.0.0.0` |
| `--port` | `8000` |

```bash
csfs serve --port 8000
```
