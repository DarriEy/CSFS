# Python API

Everything the CLI does is a thin wrapper over the library. Three layers are
useful directly: the **store** (query what you have), the **runner**
(acquire data), and the **connectors** (talk to one provider without a
store). Full signatures are in the [API Reference](reference.md).

## Querying the store

`DuckDBStore` is an async context manager over a single-file DuckDB database.

```python
import asyncio
from datetime import UTC, datetime

from csfs.store.duckdb_store import DuckDBStore


async def main() -> None:
    async with DuckDBStore("csfs.duckdb", read_only=True) as store:
        # Stations: filter by provider, country, and/or bounding box
        stations = await store.get_stations(
            provider="usgs",
            bbox=(-115.0, 49.0, -101.0, 60.0),  # (min_lon, min_lat, max_lon, max_lat)
            limit=50,
        )
        for s in stations:
            print(s.id, s.name, s.river, s.catchment_area_km2)

        # Observations for one station over a time window
        obs = await store.get_observations(
            stations[0].id,
            start=datetime(2026, 1, 1, tzinfo=UTC),
            end=datetime(2026, 6, 1, tzinfo=UTC),
        )
        for row in obs[:5]:
            print(row["timestamp"], row["discharge_m3s"], row["quality"])

        # Acquisition history and per-connector health
        history = await store.get_acquisition_history(provider="usgs", limit=5)
        health = await store.get_connector_health(stale_after_hours=72)


asyncio.run(main())
```

Because the store is plain DuckDB, you can also bypass CSFS entirely for
analytics:

```python
import duckdb

conn = duckdb.connect("csfs.duckdb", read_only=True)
df = conn.execute("""
    SELECT s.name, o.timestamp, o.discharge_m3s
    FROM observations o JOIN stations s ON s.id = o.station_id
    WHERE s.provider = 'usgs'
    ORDER BY o.timestamp
""").df()
```

## Running acquisition

`run_acquisition` orchestrates a full cycle — station discovery, concurrent
observation fetches with retry, store writes, and acquisition logging —
and returns a per-provider result dict.

```python
import asyncio

from csfs.scheduler.runner import run_acquisition
from csfs.store.duckdb_store import DuckDBStore


async def main() -> None:
    async with DuckDBStore("csfs.duckdb") as store:
        results = await run_acquisition(
            store,
            providers=["usgs", "france_hubeau"],  # None = all registered
            lookback_hours=48,
            max_stations=100,       # None = all stations
            concurrency=10,
            provider_configs=None,  # e.g. {"norway_nve": {"api_key": "..."}}
        )
    for slug, info in results.items():
        print(slug, info["status"], info.get("observations"))


asyncio.run(main())
```

Incremental by design: for each station the runner asks the store for the
latest stored timestamp and fetches only from there forward, so repeated runs
do not re-download history.

## Direct connector access

To talk to one provider without a database, instantiate its connector. Every
connector is an async context manager exposing `fetch_stations()` and
`fetch_observations(station_id, start, end)`:

```python
import asyncio
from datetime import UTC, datetime, timedelta

from csfs.core.registry import discover, get_connector


async def main() -> None:
    discover()  # import all connector modules, populating the registry

    connector_cls = get_connector("usgs")
    async with connector_cls(config={}) as c:
        stations = await c.fetch_stations()
        print(len(stations), "stations")

        end = datetime.now(UTC)
        start = end - timedelta(hours=48)
        chunk = await c.fetch_observations(stations[0].id, start, end)
        for obs in chunk.observations[:5]:
            print(obs.timestamp, obs.discharge_m3s, obs.quality)


asyncio.run(main())
```

Notes:

- `fetch_observations` takes the **canonical** station ID
  (`"<slug>:<native_id>"`, i.e. `Station.id`) and returns a
  `TimeSeriesChunk` of `Observation`s — discharge already normalized to
  m³/s, timestamps to UTC.
- `config` carries provider-specific settings (e.g. `{"api_key": ...}` for
  `norway_nve`); most providers need none.
- HTTP retry/rate-limit handling is built into the base class; transient
  upstream failures are retried with exponential backoff.

## Canonical models

All data flows through three pydantic models in `csfs.core.models`:

- **`Station`** — `id`, `provider`, `native_id`, `name`, `latitude`,
  `longitude`, `country_code`, plus optional `river`,
  `catchment_area_km2`, `elevation_m`.
- **`Observation`** — `station_id`, `timestamp` (UTC), `discharge_m3s`,
  `quality` (`good` / `suspect` / `missing` / `estimated` / `raw`).
- **`TimeSeriesChunk`** — a batch of observations from one connector fetch,
  with `fetched_at` provenance.
