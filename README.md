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
pip install csfs            # core
pip install "csfs[api]"     # + FastAPI read layer
```

Requires Python 3.11+.

## Quick start (CLI)

```bash
csfs providers                          # list registered providers + tiers
csfs fetch -p usgs --lookback 168 -n 50 # fetch a week of USGS data
csfs status                             # what's in the local DuckDB
csfs health                             # per-connector freshness + run health
csfs serve                              # HTTP read layer (needs csfs[api])
```

## Quick start (Python)

```python
import asyncio

from csfs.scheduler.runner import run_acquisition
from csfs.store.duckdb_store import DuckDBStore


async def main() -> None:
    async with DuckDBStore("csfs.duckdb") as store:
        await run_acquisition(store, providers=["usgs"], lookback_hours=48, max_stations=20)

        stations = await store.get_stations(provider="usgs", limit=5)
        obs = await store.get_observations(stations[0].id, limit=10)
        for row in obs:
            print(row["timestamp"], row["discharge_m3s"])


asyncio.run(main())
```

The store is a plain DuckDB file — any SQL/pandas/Arrow tooling works on it
directly. For direct single-provider access without a database, see the
[Python API guide](https://darriey.github.io/CSFS/python-api/).

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
