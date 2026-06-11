# Quick Start

## Install

```bash
pip install csfs
```

Python 3.11+ is required. For the FastAPI read layer, install the extra:

```bash
pip install "csfs[api]"
```

From source:

```bash
git clone https://github.com/DarriEy/CSFS.git
cd CSFS
pip install -e ".[dev,api]"
```

## First acquisition (CLI)

```bash
# See every registered provider and its scheduler tier
csfs providers

# Fetch the last 7 days of USGS data (capped at 50 stations to start small)
csfs fetch -p usgs --lookback 168 -n 50

# Inspect what landed in the local DuckDB (csfs.duckdb by default)
csfs status
csfs stations -p usgs

# Check per-connector health
csfs health
```

Each command accepts `--db` to point at a different database file:

```bash
csfs --db /data/streamflow.duckdb fetch -p france_hubeau --lookback 48
```

!!! note "Live providers can wobble"
    `csfs fetch` talks to real agency APIs. A failed provider usually means a
    transient upstream outage, not a CSFS bug — re-run before filing an issue.

## First acquisition (Python)

Everything the CLI does is available programmatically. Run one acquisition
cycle and then query the store:

```python
import asyncio

from csfs.scheduler.runner import run_acquisition
from csfs.store.duckdb_store import DuckDBStore


async def main() -> None:
    async with DuckDBStore("csfs.duckdb") as store:
        # Acquire: last 48 h of data for two providers, 20 stations each
        results = await run_acquisition(
            store,
            providers=["usgs", "uk_ea"],
            lookback_hours=48,
            max_stations=20,
        )
        print(results["usgs"])
        # {'stations': ..., 'observations': ..., 'status': 'ok', ...}

        # Query stations back out of the store
        stations = await store.get_stations(provider="usgs", limit=5)
        for s in stations:
            print(s.id, s.name, s.latitude, s.longitude)

        # Query observations for one station
        obs = await store.get_observations(stations[0].id, limit=10)
        for row in obs:
            print(row["timestamp"], row["discharge_m3s"], row["quality"])


asyncio.run(main())
```

`get_stations` returns typed `Station` models; `get_observations` returns
dicts with `station_id`, `timestamp`, `discharge_m3s`, and `quality`. The
store is a plain DuckDB file, so any DuckDB/SQL/pandas/Arrow tooling works on
it directly too. See the [Python API](python-api.md) guide for more, including
direct connector access without a store.

## Keep it fresh

Run as a daemon on a cron tier (see [Architecture](architecture.md) for tiers):

```bash
csfs daemon --schedule realtime --tier realtime
```

## Offline archive datasets

Some connectors read research archives (GRDC, Caravan, GSIM, EStreams,
CAMELS variants, ...) from local files:

```bash
csfs download-data --list     # what's available, sizes, auto vs manual
csfs download-data -d caravan # fetch one dataset into data/datasets/
```

## API keys

Most connectors need no credentials. Exceptions:

- **Norway (`norway_nve`)** — free [NVE HydAPI](https://hydapi.nve.no/) key.
- **GloFAS (`glofas`)** — [Copernicus CDS](https://cds.climate.copernicus.eu/)
  Personal Access Token, stored in `~/.cdsapirc` (recommended) or under
  `providers.glofas.api_key` in `csfs.yaml`.

Keep keys out of any tracked config file.

## Serve over HTTP

```bash
pip install "csfs[api]"
csfs serve --port 8000
```

This starts a FastAPI read layer over the local database.
