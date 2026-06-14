# CSFS — Community Streamflow Service

**Live acquisition and harmonization of global streamflow observations.**

[![CI](https://github.com/DarriEy/CSFS/actions/workflows/ci.yml/badge.svg)](https://github.com/DarriEy/CSFS/actions/workflows/ci.yml)
[![License: GPL v3](https://img.shields.io/badge/License-GPLv3-blue.svg)](https://www.gnu.org/licenses/gpl-3.0)

CSFS connects to open streamflow data providers worldwide — national
hydrological agencies, regional networks, research archives, and global model
products — harmonizes their observations into one canonical
station/observation schema (discharge in m³/s, timestamps in UTC), and
maintains a near-real-time DuckDB store with scheduled acquisition, health
monitoring, a CLI, and a FastAPI read layer.

**Documentation:** <https://darriey.github.io/CSFS/>

## Why CSFS?

Programmatic access to river discharge data is fragmented: the community
relies either on *static archives* (GRDC, Caravan, GSIM, EStreams, CAMELS)
that are frozen at publication time, or on *single-agency clients* (USGS
`dataretrieval`, `hydrofunctions`) that each cover one network. Getting
current discharge across, say, France, Brazil, and Japan means learning three
APIs, three formats, and three unit conventions. CSFS provides a single
interface for live, multi-provider acquisition — one connector per agency,
every observation normalized to a common schema, re-acquisition scheduled to
each provider's update cadence — and keeps its provider roster honest
mechanically, with CI-enforced integrity tests.

## Provider roster (the honest numbers)

- **104 sources cataloged** in [`inventory/providers.yaml`](inventory/providers.yaml),
  labeled by readiness: **78 implemented**, 17 research, 5 fallback,
  3 manual, 1 deprecated.
- **86 connectors registered in code** — the 78 `implemented` entries plus 8
  still labeled `research` while their upstream data paths are validated.
- **41 implemented providers are realtime/near-realtime**; the rest are
  recent/archive sources, including roughly a dozen offline research archives
  (GRDC, Caravan, GSIM, EStreams, LamaH, CAMELS variants, ROBIN, ADHI, SIEREM).

These statuses are **CI-enforced**: `tests/test_connector_integrity.py`
fails the build if a connector ships without tests, lacks a scheduler tier,
or if the inventory claims `implemented` for a connector that does not exist.
See the full [provider catalog](https://darriey.github.io/CSFS/catalog/).

> Note: live-provider commands talk to real agency APIs and can hit transient
> upstream outages — a failed fetch is usually them, not you.

## Install

```bash
pip install community-streamflow-service            # core
pip install "community-streamflow-service[pandas]"  # + DataFrame store queries
pip install "community-streamflow-service[api]"     # + FastAPI read layer
```

Requires Python 3.11+.

## Quick start (CLI)

```bash
csfs providers                          # list registered providers + tiers
csfs fetch -p usgs --lookback 168 -n 50 # fetch a week of USGS data
csfs status                             # what's in the local DuckDB
csfs health                             # per-connector freshness + run health
csfs serve                              # HTTP read layer (needs the api extra)
```

## Quick start (Python)

```python
import asyncio

import csfs


async def main() -> None:
    async with csfs.open_store("csfs.duckdb", read_only=False) as store:
        await csfs.run_acquisition(store, providers=["usgs"], lookback_hours=48, max_stations=20)

        stations = await store.get_stations(provider="usgs", limit=5)
        # pandas DataFrame indexed by timestamp (needs the [pandas] extra);
        # get_observations() / get_observations_arrow() need no extra.
        df = await store.get_observations_df(stations[0].id)
        print(df["discharge_m3s"].describe())


asyncio.run(main())
```

Or pull one gauge's series straight from a provider, no database involved:

```python
from datetime import UTC, datetime, timedelta

import csfs

end = datetime.now(UTC)
chunk = csfs.fetch_observations_sync("usgs", "usgs:01646500", start=end - timedelta(days=7), end=end)
```

The store is a plain DuckDB file — any SQL/pandas/Arrow tooling works on it
directly. The blessed, stable surface is what `import csfs` re-exports; see
the [Python API guide](https://darriey.github.io/CSFS/python-api/).

## SYMFLUENCE integration

CSFS doubles as a streamflow-observation plugin for
[SYMFLUENCE](https://github.com/DarriEy/SYMFLUENCE): install both packages and
SYMFLUENCE auto-discovers the handler, so an experiment YAML needs only

```yaml
ADDITIONAL_OBSERVATIONS: csfs
CSFS_STATION_ID: "usgs:01646500"
```

to calibrate against any CSFS-reachable gauge (live fetch, or offline from a
CSFS store via `CSFS_DB_PATH`). See the
[SYMFLUENCE integration guide](https://darriey.github.io/CSFS/symfluence/).

## API keys

Most connectors need no credentials. Exceptions: **`norway_nve`** (free
[NVE HydAPI](https://hydapi.nve.no/) key) and **`glofas`**
([Copernicus CDS](https://cds.climate.copernicus.eu/) token in `~/.cdsapirc`).
Keep keys out of tracked config files.

## Architecture

```
connectors/     Provider plugins (one per data source)
core/           Canonical data models, registry, health, exceptions
store/          Persistence layer (DuckDB default)
scheduler/      Acquisition runner, cron tiers, daemon
api/            FastAPI query layer
cli/            Command-line interface
inventory/      Global provider inventory (YAML)
```

Details — including the roster-integrity guard system and the hermetic test
policy — in the [architecture docs](https://darriey.github.io/CSFS/architecture/).

## Contributing

The most valuable contribution is a new provider connector. See
[CONTRIBUTING.md](CONTRIBUTING.md) for the walkthrough and the
roster-integrity requirements your PR must satisfy.

## Citing

See [CITATION.cff](CITATION.cff).

## License

GPL-3.0-or-later. See [LICENSE](LICENSE).
