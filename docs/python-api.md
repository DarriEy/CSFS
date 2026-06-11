# Python API

`import csfs` is the blessed public surface — everything re-exported there
(see `csfs.__all__`) is stable across minor releases, while deeper module
paths are internal and may move. This facade is what downstream frameworks
(e.g. a SYMFLUENCE streamflow-observation adapter) should call. Full
signatures are in the [API Reference](reference.md).

## Quick start: store → DataFrame

`csfs.open_store()` opens the single-file DuckDB store (read-only by
default) as an async context manager. The `*_df` methods return pandas
DataFrames; observations come back indexed by ascending UTC `timestamp`
with `discharge_m3s` and `quality` columns.

```python
import asyncio

import csfs


async def main() -> None:
    async with csfs.open_store("csfs.duckdb") as store:
        # One gauge's series, ready for resampling/plotting/metrics
        df = await store.get_observations_df("usgs:01646500")
        print(df["discharge_m3s"].describe())

        # Many gauges at once (a station_id column is kept)
        multi = await store.get_observations_df(
            ["usgs:01646500", "usgs:01638500"],
        )

        # Station metadata as a frame
        stations = await store.get_stations_df(provider="usgs", limit=50)


asyncio.run(main())
```

!!! note "pandas is an optional extra"
    The DataFrame methods need pandas, which CSFS does not require by
    default. Install it with:

    ```bash
    pip install "community-streamflow-service[pandas]"
    ```

    Without it, the `*_df` methods raise an `ImportError` pointing at that
    command. The Arrow methods below need no extra.

## Zero-copy Arrow queries

`get_observations_arrow()` / `get_stations_arrow()` take the same filters
as their list-returning counterparts and return a `pyarrow.Table` straight
from DuckDB's native Arrow results — no Python-object round trip, ideal for
large pulls or handing off to polars/datafusion:

```python
table = await store.get_observations_arrow(
    ["usgs:01646500", "usgs:01638500"],
    start=datetime(2026, 1, 1, tzinfo=UTC),
    end=datetime(2026, 6, 1, tzinfo=UTC),
)
```

## One-shot provider fetch (no store)

To pull a single gauge's series directly from a provider — e.g. grabbing
observations for one calibration basin — use `fetch_observations` /
`fetch_observations_sync`. They handle connector discovery, instantiation,
and the HTTP session for you and return a `TimeSeriesChunk` with discharge
normalized to m³/s and timestamps to UTC:

```python
from datetime import UTC, datetime, timedelta

import csfs

end = datetime.now(UTC)
chunk = csfs.fetch_observations_sync(
    "usgs",
    "usgs:01646500",          # canonical "<provider>:<native_id>" ID
    start=end - timedelta(days=30),
    end=end,
    config=None,              # provider-specific settings, e.g. API keys
)
for obs in chunk.observations[:5]:
    print(obs.timestamp, obs.discharge_m3s, obs.quality)
```

`fetch_observations_sync` runs its own event loop, so it must be called
from synchronous code; inside async code (it raises `RuntimeError` there)
use the awaitable form:

```python
chunk = await csfs.fetch_observations("usgs", "usgs:01646500", start, end)
```

## Querying the store (lists of models/dicts)

The original query methods return pydantic `Station` models and plain
dicts — handy for JSON serialization and small lookups:

```python
import asyncio
from datetime import UTC, datetime

import csfs


async def main() -> None:
    async with csfs.open_store("csfs.duckdb") as store:
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

import csfs


async def main() -> None:
    async with csfs.open_store("csfs.duckdb", read_only=False) as store:
        results = await csfs.run_acquisition(
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

Provider configs can be loaded from `csfs.yaml` (or
`~/.config/csfs/config.yaml`) with `csfs.load_config()`.

## Direct connector access

For full control over one provider — station discovery, bulk fetches —
instantiate its connector class. Every connector is an async context
manager exposing `fetch_stations()` and
`fetch_observations(station_id, start, end)`:

```python
import asyncio
from datetime import UTC, datetime, timedelta

import csfs


async def main() -> None:
    csfs.discover()  # import all connector modules, populating the registry

    connector_cls = csfs.get_connector("usgs")
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

All data flows through three pydantic models, re-exported at top level:

- **`csfs.Station`** — `id`, `provider`, `native_id`, `name`, `latitude`,
  `longitude`, `country_code`, plus optional `river`,
  `catchment_area_km2`, `elevation_m`.
- **`csfs.Observation`** — `station_id`, `timestamp` (UTC),
  `discharge_m3s`, `quality` (`good` / `suspect` / `missing` /
  `estimated` / `raw`).
- **`csfs.TimeSeriesChunk`** — a batch of observations from one connector
  fetch, with `fetched_at` provenance.

The matching PyArrow schemas are `csfs.OBSERVATION_SCHEMA` and
`csfs.STATION_SCHEMA`.
