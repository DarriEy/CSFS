# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.3.0] — 2026-06-11

### Added

- Drop-in SYMFLUENCE community backend: the plugin now also registers
  per-provider observation handlers under SYMFLUENCE's existing streamflow
  provider names (`usgs`, `wsc`, `smhi`), built by a small factory on top of
  `CSFSStreamflowHandler`. With SYMFLUENCE's registry-first streamflow
  dispatch and `DATA_ACCESS: community`, a stock experiment
  (`STREAMFLOW_DATA_PROVIDER` and station-id keys unchanged) acquires its
  primary streamflow through CSFS; the in-tree handlers keep their separate
  `usgs_streamflow`-style registry keys and the default path is untouched.
  Station ids resolve from the same config keys the native handlers read
  (`STATION_ID`, plus `USGS_SITE_CODE`/`STREAMFLOW_STATION_ID` for USGS,
  with native zero-padding) and also accept CSFS-namespaced ids. The WSC
  handler maps to the `environment_canada` connector (value-identical to
  native, without the native GeoMet pagination corruption); the SMHI handler
  pins the connector to the 15-minute discharge product for parity with the
  native download (overridable via `CSFS_CONNECTOR_CONFIG`). Documented in
  `docs/symfluence.md` together with the measured parity table
  (USGS bit-identical after the timezone/conversion fixes, WSC
  value-identical, SMHI 15-min product parity).
- SYMFLUENCE integration plugin (`csfs.integrations.symfluence`): a
  `CSFSStreamflowHandler` observation handler that lets SYMFLUENCE pull
  calibration/evaluation streamflow from any CSFS provider connector
  (`CSFS_STATION_ID: "usgs:01646500"`, live fetch) or from a pre-built
  CSFS DuckDB store (`CSFS_DB_PATH`), writing the framework's standard
  raw and processed streamflow CSVs (`discharge_cms`, m³/s, UTC).
- Auto-discovery via the `symfluence.plugins` entry point: with both
  packages installed, `ADDITIONAL_OBSERVATIONS: csfs` works after a plain
  `import symfluence` — no framework changes, no registration code.
  SYMFLUENCE remains a non-dependency: the integration module imports the
  framework defensively and `import csfs` is unaffected without it.
- Documentation page (`docs/symfluence.md`) covering install, YAML
  configuration, live-fetch vs store mode, and the unit/timezone contract.
- SMHI 15-minute discharge product: `sweden_smhi` now accepts
  `config={"resolution": "15min"}` to use hydroobs parameter 2
  ("Vattenföring (15 min)") instead of the daily parameter 1
  ("Vattenföring (Dygn)", still the default — existing behavior is
  unchanged). The 15-min path keeps the same epoch-ms UTC timestamps and
  m³/s passthrough; because the API offers no date subsetting and the full
  15-min corrected archive is one ~73 MB JSON per station, windows starting
  within the last 24 h fetch the small `latest-day` file instead (the API
  has no `latest-months` period, unlike SMHI metobs), and full-archive
  downloads get an extended timeout.

### Fixed

- SMHI quality-code map now follows SMHI's documented legend: `G` (green,
  checked and approved) → good, `Y` (yellow, roughly checked) → suspect,
  and the previously unmapped `O` (orange, unchecked — seen on recent
  realtime data) is explicitly mapped to raw; unknown codes deliberately
  fall through to raw.
- `sweden_smhi.fetch_observations` docstring claimed the `latest-months`
  period while the code fetched `corrected-archive`; it now documents the
  actual window-dependent period selection.

## [0.2.0] — 2026-06-11

### Added

- Public Python API facade: `import csfs` now re-exports the blessed,
  stable surface — `DuckDBStore`, the canonical models (`Station`,
  `Observation`, `TimeSeriesChunk`, `QualityFlag`) and PyArrow schemas,
  `run_acquisition`, `load_config`, and the registry
  (`discover` / `get_connector` / `list_providers`) — with an explicit
  `__all__`. Connector modules stay lazy behind `discover()`.
- Arrow and pandas store queries: `DuckDBStore.get_observations_arrow()` /
  `get_stations_arrow()` return zero-copy `pyarrow.Table`s via DuckDB's
  native Arrow results; `get_observations_df()` / `get_stations_df()`
  return pandas DataFrames (observations indexed by ascending UTC
  `timestamp`). Observation queries accept a list of station IDs for
  multi-gauge (e.g. calibration) workloads.
- `pandas` optional extra (`pip install
  "community-streamflow-service[pandas]"`); the DataFrame methods raise a
  clear `ImportError` pointing to it when pandas is absent.
- Direct-fetch helpers: `csfs.open_store()` (read-only by default) and
  `csfs.fetch_observations()` / `csfs.fetch_observations_sync()` for
  one-shot, store-less pulls of a single gauge's series from any provider.

## [0.1.0] — 2026-06-11

Initial release.

### Added

- 86 provider connectors covering live agency APIs (USGS NWIS, UK EA,
  Hub'Eau, Environment Canada, PEGELONLINE, and many more), offline archive
  datasets (GRDC, Caravan, GSIM, EStreams, LamaH, CAMELS variants), and
  model/aggregator sources (GloFAS, GEOGLOWS, WMO WHOS views).
- Canonical `Station` / `Observation` / `TimeSeriesChunk` data model
  (pydantic) with discharge normalized to m³/s and UTC timestamps.
- DuckDB persistence layer (`DuckDBStore`) with station upserts,
  observation appends, acquisition history, and connector-health queries.
- Scheduler with four cron tiers (realtime / hourly / daily / weekly),
  per-tier lookback windows, and a long-lived daemon mode.
- `csfs` CLI: `fetch`, `daemon`, `providers`, `stations`, `status`,
  `health`, `download-data`, `serve`.
- FastAPI read layer for querying stations and observations over HTTP.
- Global provider inventory (`inventory/providers.yaml`): 104 cataloged
  sources labeled by readiness status (implemented / research / fallback /
  manual).
- Roster-integrity guard tests: every registered connector must have test
  coverage, belong to exactly one cron tier, and be documented in the
  inventory; `status: implemented` entries must have a registered connector.
- Hermetic test suite: an autouse DNS guard blocks real network access;
  live tests opt in via `@pytest.mark.network`.
- Scheduled acquisition workflow on GitHub Actions with a release-hosted
  DuckDB snapshot and health alerting.

[0.2.0]: https://github.com/DarriEy/CSFS/releases/tag/v0.2.0
[0.1.0]: https://github.com/DarriEy/CSFS/releases/tag/v0.1.0
