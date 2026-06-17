# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

> Requires SYMFLUENCE with contract **0.5.0** (the source-kind/provenance and
> posture-only observation gates). Against an older SYMFLUENCE the new
> capabilities are declined and CSFS falls through to the native path — the
> drop-ins simply do not activate until the framework side ships.

### Added

- **`CommunityObservationBackend`** registers under SYMFLUENCE's
  `R.observation_backends` (acquisition-backend protocol; `TARGET_INTERFACE_VERSION
  = "0.5.0"`). Under `DATA_ACCESS: community`, SYMFLUENCE's backend-first
  streamflow dispatch routes served providers through this backend, which writes
  a per-station OBS_CSV_V1 delivery (`datetime,value,quality_flag`, UTC, m³/s,
  half-open `[start, end)`) plus an `acquisition_manifest.json` sidecar, and maps
  CSFS failures onto the protocol error taxonomy. Pure helpers `obs_csv_v1_frame()`
  and the framework-free `OBSERVATION_CAPABILITIES` table are unit-tested without
  SYMFLUENCE installed.
- **Dataset-artifact tier** — authoritative large-sample datasets served as
  DOI-pinned, checksum-verified providers admitted by the provenance gate (DOI +
  version + verified checksum + open/attribution license), not parity:
  **16 CAMELS** (US, CL, BR, GB, AUS, DE, DK, CH, FR, SE, IND, NZ, FI, LUX, PE),
  **LamaH-CE**, **LamaH-Ice**, and **HYSETS** (NetCDF, 14,425 North American
  watersheds). Each ships a connector + auto-download `DATASETS` entry +
  capability + live round-trip verification. CAMELS-COL and CAMELS-SPAT are
  registered as manual/gated stubs (access-restricted / Globus-only).
- **Posture-only `provider_api` tier** — 26 live national/regional streamflow
  APIs exposed as drop-ins, admitted on an open/attribution source license (no
  native handler to parity-grade against) and each live round-trip verified
  (`tests/test_provider_api_verified.py`, network-marked; driven by the
  `NATIONAL_PROVIDER_APIS` table). Includes uk_ea, uk_nrfa, scotland_sepa,
  australia_bom, norway_nve, finland_syke, japan_mlit, czechia_chmu, poland_imgw,
  italy_emilia, greece_openhi, lithuania_lhmt, ireland_epa, denmark_dmihyd,
  belgium_waterinfo, argentina_snih, germany_pegelonline, germany_bavaria,
  austria_ehyd, slovenia_arso, bulgaria_eaemdr, taiwan_wra, spain_cedex,
  vietnam_mekong, plus `netherlands_rws` (CC0) and `germany_nrw` (DL-DE/Zero)
  as the two `open` providers.
- **France Hub'Eau** added to the native↔community parity tier
  (value-identical, L/s→m³/s) — the first live-API parity drop-in beyond
  usgs/wsc/smhi.
- **Content-checksum integrity** (`content_checksum` + `content_exclude`) for
  sources whose server regenerates the archive per request (e.g. CEH's
  CAMELS-GB), and a `filename` override for filename-less download URLs (OSF).
  Archive extraction now content-sniffs (magic bytes), keeps bare non-archive
  downloads in place, and reprojects national-grid coordinates to WGS84.

### Changed

- `TARGET_INTERFACE_VERSION` 0.2.0 → **0.5.0**. Capabilities declare source-data
  license posture (`redistribution` / `data_license` / `attribution`, contract
  0.4.0), the source-kind tier (`source_kind` / `dataset_doi` / `dataset_version`
  / `dataset_checksum`, 0.5.0), and a `noncommercial` flag (CC-BY-NC sources).
- The acquisition-backend interface modules carry an Apache-2.0 header so
  third-party backends need not inherit GPL.
- `spain_cedex` gained a keyless HTTP path to the CEDEX *Anuario de Aforos* (was
  local-file-only); `vietnam_mekong` now reads NERC EIDC data-package discharge
  (was reading sediment-flux files).

### Fixed

- `csfs.integrations.symfluence` no longer logs a misleading "Failed to load …
  skipping" warning when imported before SYMFLUENCE's bootstrap (re-entrant
  plugin load); registration self-heals regardless of import order.

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
