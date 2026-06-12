# SYMFLUENCE Integration

CSFS ships a [SYMFLUENCE](https://github.com/DarriEy/SYMFLUENCE) plugin with
two modes:

1. **Drop-in community backend** — CSFS replaces SYMFLUENCE's built-in
   primary streamflow acquisition for `STREAMFLOW_DATA_PROVIDER: USGS / WSC /
   SMHI` with a *single new config line* (`DATA_ACCESS: community`). Your
   existing station-id keys keep working; nothing else changes.
2. **Generic observation handler** — any of CSFS's 86 providers (live agency
   APIs or a pre-built CSFS store) as an additional observation source via
   `ADDITIONAL_OBSERVATIONS: csfs` and namespaced station ids.

## Integration tiers (how a request is routed)

Under `DATA_ACCESS: community`, SYMFLUENCE resolves primary streamflow
through three layered tiers, highest priority first:

1. **ObservationBackend tier** (SYMFLUENCE acquisition-backend protocol,
   contract 0.2.0): the plugin registers `CommunityObservationBackend` under
   `R.observation_backends`. SYMFLUENCE's selection layer matches the
   configured provider against the backend's declared capabilities (USGS,
   WSC, SMHI — parity-graded — plus the ungated generic `CSFS`), applies the
   parity gate, and hands the backend a formal `ObservationRequest`. The
   backend reuses the handler classes below internally, then additionally
   writes a per-station `*_obs_v1.csv` delivery (`datetime,value,quality_flag`,
   UTC, m³/s, trimmed to the half-open `[start, end)` window) and an
   `acquisition_manifest.json` sidecar.
2. **Registry-handler tier** (the original integration): the drop-in keys
   `usgs`/`wsc`/`smhi` plus the generic `csfs` key. This tier is the
   fallthrough when the backend tier declines — e.g. the parity gate refuses
   the ungated generic provider, or an older SYMFLUENCE without the backend
   registry is installed — and remains the only route for
   `ADDITIONAL_OBSERVATIONS: csfs`. Redundant under community mode but kept
   by design.
3. **Legacy tier**: SYMFLUENCE's native in-tree handlers — untouched, and
   the default outside community mode.

All tiers produce the identical processed calibration CSV; the backend tier
adds the protocol artifacts on top.

## Drop-in community backend (USGS / WSC / SMHI)

Take an existing experiment that calibrates against a USGS, WSC, or SMHI
gauge and add one line:

```yaml
STREAMFLOW_DATA_PROVIDER: USGS      # unchanged
STATION_ID: "06191500"              # unchanged (or USGS_SITE_CODE / STREAMFLOW_STATION_ID)
DATA_ACCESS: community              # <- the only new line
```

The plugin registers observation handlers under the existing provider names
(`usgs`, `wsc`, `smhi`). SYMFLUENCE's registry-first streamflow dispatch
(`process_streamflow_data()`) resolves the lowercased provider in the
observation-handler registry and routes acquisition + processing to CSFS
when `DATA_ACCESS` is `community`; with the default `DATA_ACCESS` the native
in-tree handlers run exactly as before (they live under separate registry
keys — `usgs_streamflow` etc. — so the plugin never shadows them).

Station ids are read from the **same config keys the native handlers use**,
in the same order:

| Provider | Keys (resolution order) | Accepted forms |
|----------|------------------------|----------------|
| `USGS` | `STATION_ID` (`evaluation.streamflow.station_id`), `USGS_SITE_CODE`, `STREAMFLOW_STATION_ID` | `06191500`, `6191500` (zero-padded to 8 digits like the native handler), `usgs:06191500` |
| `WSC` | `STATION_ID` | `05BB001`, `environment_canada:05BB001`, `wsc:05BB001` |
| `SMHI` | `STATION_ID` | `2357`, `sweden_smhi:2357`, `smhi:2357` |

`acquire()` does a direct connector fetch for the experiment window
(`EXPERIMENT_TIME_START`/`END`) and writes the raw CSV under
`observations/streamflow/raw_data/`; `process()` emits the identical
processed contract as the native handlers (`datetime` index, tz-naive UTC,
`discharge_cms` in m³/s, same resample + interpolation).

### Parity with the native handlers

Measured native-vs-CSFS on the same station/window (full results in the
community-services design notes):

| Provider | Parity | Notes |
|----------|--------|-------|
| USGS | **Bit-identical** | Same NWIS API. Required two fixes, both landed: SYMFLUENCE parsed NWIS local clock time and ignored `tz_cd` (fixed in symfluence#221 — native output moved to UTC), and CSFS used a truncated cfs→m³/s factor (now the exact `0.028316846592`). |
| WSC | **Value-identical** | Same GeoMet daily-mean collection; CSFS fetches only the experiment window instead of the full period of record. The native handler's unsorted GeoMet pagination could silently corrupt records on stations with >10k records (duplicates + missing rows, nondeterministic); CSFS's `environment_canada` connector never had that failure mode. |
| SMHI | **Product parity via 15-min** | The native handler downloads SMHI hydroobs parameter 2 (15-minute discharge); the CSFS default is the daily product. The drop-in handler therefore pins `resolution: "15min"` on the connector so both sides see the same product. Timestamps are epoch-ms UTC on both sides. |

The SMHI 15-min pin (and any other connector setting) can be overridden via
`CSFS_CONNECTOR_CONFIG` (e.g. `{resolution: daily}`), at the cost of parity
with the native product.

## Generic observation handler (`ADDITIONAL_OBSERVATIONS: csfs`)

Beyond the three drop-in providers, the generic handler exposes the whole
CSFS network — 80+ additional agencies and archives — as an additional
observation source. SYMFLUENCE experiments can calibrate and evaluate
against any gauge CSFS can reach — live agency APIs or a pre-built CSFS
store — with a two-line config change and zero framework modifications.

## How discovery works

CSFS registers a hook in the `symfluence.plugins` entry-point group
(`csfs = "csfs.integrations.symfluence:register"`). SYMFLUENCE's bootstrap
loads that group on `import symfluence` and calls the hook, which adds
`CSFSStreamflowHandler` to the observation-handler registry under the key
`csfs`, plus the three drop-in handlers under `usgs`, `wsc`, and `smhi`.
There is nothing to import, register, or configure manually:

```console
$ python -c "import symfluence
> from symfluence.core.registries import R
> print([k for k in ('csfs', 'usgs', 'wsc', 'smhi') if k in R.observation_handlers])"
['csfs', 'usgs', 'wsc', 'smhi']
```

The dependency is strictly one-way: CSFS does **not** depend on SYMFLUENCE.
`csfs.integrations.symfluence` imports the framework defensively, so
`import csfs` keeps working in environments without it (the handler class
simply degrades and refuses to run).

## Install

Both packages in the same environment, plus the pandas extra (the handler
reads/writes CSVs through pandas):

```bash
pip install symfluence "community-streamflow-service[pandas]"
```

## Configuration (generic handler)

Add the handler to your experiment YAML via the standard
`ADDITIONAL_OBSERVATIONS` mechanism:

```yaml
# --- streamflow observations via CSFS ---
ADDITIONAL_OBSERVATIONS: csfs
CSFS_STATION_ID: "usgs:01646500"     # canonical CSFS id: <provider>:<native_id>
```

That is the whole live-fetch setup. During the observation step SYMFLUENCE
instantiates the handler, `acquire()` pulls the series for the experiment
window (`EXPERIMENT_TIME_START`/`END`) with `csfs.fetch_observations_sync`,
and `process()` writes the framework's standard processed streamflow CSV.

Supported keys:

| Key | Required | Meaning |
|-----|----------|---------|
| `CSFS_STATION_ID` | yes¹ | One or more CSFS station ids — a single id, a comma-separated string, or a YAML list. The `<provider>:` prefix selects the connector (`csfs providers` lists slugs). Multiple stations are averaged per timestep in the processed output. |
| `CSFS_CONNECTOR_CONFIG` | no | Mapping of provider-specific settings (API keys etc.) passed to the connector on live fetches. |
| `CSFS_DB_PATH` | no | Path to an existing CSFS DuckDB store. When set, observations are read from the store instead of fetched live. |

¹ Falls back to the shared `STATION_ID` (`evaluation.streamflow.station_id`)
when unset — that value must then also be namespaced (`usgs:01646500`, not
`01646500`); un-namespaced ids fail with a pointed error.

### Live fetch vs. store mode

**Live fetch (default).** Each station is pulled straight from its provider
API for the experiment window. Best for one-off experiments and always-fresh
data; requires network access at workflow time.

**Store mode (`CSFS_DB_PATH`).** Point the handler at a DuckDB store built
with `csfs acquire`:

```yaml
ADDITIONAL_OBSERVATIONS: csfs
CSFS_STATION_ID: "usgs:01646500"
CSFS_DB_PATH: /data/csfs/csfs.duckdb
```

The handler queries the store read-only — fully offline, reproducible, and
fast for many-gauge or repeated-calibration workloads where you curate the
observation database once.

## What gets written

Following SYMFLUENCE's observation conventions under
`domain_{NAME}/data/observations/streamflow/`:

- `raw_data/csfs_{provider}_{native_id}_raw.csv` — one per station, the
  CSFS-native series (`timestamp`, `discharge_m3s`, `quality`). Existing raw
  files are reused unless `FORCE_DOWNLOAD: true`.
- `preprocessed/{DOMAIN_NAME}_streamflow_processed.csv` — the calibration
  pipeline's contract, identical to the USGS/WSC handlers: a `datetime`
  index column and a `discharge_cms` column, resampled to the configured
  model timestep (`FORCING_TIME_STEP_SIZE`) with small gaps interpolated.

## Units and timezone guarantees

CSFS harmonizes every provider to **discharge in m³/s** and **timestamps in
UTC**, so the handlers perform no unit conversion. Processed timestamps are
written tz-naive in UTC, matching the convention of SYMFLUENCE's built-in
streamflow handlers (which, for USGS, themselves moved to UTC with
symfluence#221 — earlier versions wrote gauge-local clock time with DST
discontinuities; aligning observations and forcing on UTC is what makes the
calibration comparison meaningful). This applies equally to the drop-in
provider handlers and the generic `csfs` handler.
