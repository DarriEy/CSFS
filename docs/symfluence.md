# SYMFLUENCE Integration

CSFS ships a [SYMFLUENCE](https://github.com/DarriEy/SYMFLUENCE) plugin that
exposes the whole CSFS provider network as a streamflow **observation
handler**. SYMFLUENCE experiments can calibrate and evaluate against any
gauge CSFS can reach — live agency APIs or a pre-built CSFS store — with a
two-line config change and zero framework modifications.

## How discovery works

CSFS registers a hook in the `symfluence.plugins` entry-point group
(`csfs = "csfs.integrations.symfluence:register"`). SYMFLUENCE's bootstrap
loads that group on `import symfluence` and calls the hook, which adds
`CSFSStreamflowHandler` to the observation-handler registry under the key
`csfs`. There is nothing to import, register, or configure manually:

```console
$ python -c "import symfluence
> from symfluence.data.observation.registry import ObservationRegistry
> print('csfs' in ObservationRegistry.list_observations())"
True
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

## Configuration

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
UTC**, so the handler performs no unit conversion. Processed timestamps are
written tz-naive in UTC, matching the convention of SYMFLUENCE's built-in
streamflow handlers.
