# CI Triage Guide — CSFS (Community Streamflow Service)

This file is read by the automated CI triage agent (`.github/workflows/ci-autotriage.yml`).
It defines how to classify a CI failure for **this** service and what is safe to auto-fix.

## What this service is

CSFS connectors fetch **streamflow observations** from national/agency providers and return
canonical `Station` metadata and `Observation` records — **discharge in m³/s, timestamps in
UTC**, with a quality flag. One connector wraps one upstream agency API.

## Classifications and actions

Pick exactly one. The action column is enforced by the workflows — the auto-merge job
**only** merges `adapter_drift`/`data_drift` fixes, and **only** when every changed file is
under `src/csfs/connectors/` or `tests/`.

| Classification | What it means | Action |
|---|---|---|
| `adapter_drift` | A **data provider** (agency API) changed something a connector consumes (endpoint, station-metadata format, field name, discharge unit, date format). Fixable **entirely inside `src/csfs/connectors/<slug>.py`** and/or its test. | Fix PR → **auto-merge on green** |
| `data_drift` | Contract and live provider are fine, but a recorded fixture / expected value in a test is stale. Fixable inside `connectors/` or `tests/`. | Fix PR → **auto-merge on green** |
| `contract_change` | The failure involves the canonical schema/contract — anything under `src/csfs/core/` (`models.py`: `Station`, `Observation`, quality-flag enum, the PyArrow `OBSERVATION_SCHEMA`/`STATION_SCHEMA`) or `BaseConnector`. | Fix PR → **human merge** |
| `tooling_drift` | A build / CI / dependency / tooling failure: mypy or ruff config, a dependency version bump (numpy, pandas, pyarrow, …), type stubs, packaging, or the CI workflow itself. Also the **roster-integrity** test (`tests/test_connector_integrity.py`) failing because a connector isn't tiered/inventoried — unless the right fix is purely in `connectors/`. **Not** a data-provider change. | Fix PR → **human merge** |
| `outage` | Transient external failure: HTTP 429/5xx, DNS, connection timeouts, agency/auth-service hiccups. | **Report only** (recommend re-run) |
| `real_bug` | A genuine logic error in non-adapter CSFS code. | **Report only** (describe the fix) |
| `other` | You cannot confidently classify it. | **Report only** |

## The canonical contract — never auto-fixed

Editing anything under `src/csfs/core/` is a `contract_change` (human-only), never drift:
- `models.py` — `Station`, `Observation` (`discharge_m3s` canonical unit, `timestamp` UTC,
  `quality_flag`: GOOD, SUSPECT, MISSING, ESTIMATED, RAW), and the PyArrow schemas.
- `BaseConnector` (`connectors/base.py`) — the public connector interface.

## The scope rule (critical — read before opening any fix PR)

An `adapter_drift` / `data_drift` fix **must change only files under `src/csfs/connectors/` or
`tests/`**. If the minimal fix would touch **any** other path — `pyproject.toml`, `.github/`,
`src/csfs/core/`, `inventory/`, docs, packaging — then it is **not** adapter/data drift.
Reclassify:
- touches `src/csfs/core/` (or `BaseConnector`) → `contract_change`
- touches build/CI/deps/inventory (e.g. `pyproject.toml` mypy/ruff/version config) → `tooling_drift`

Both take the **human-gated** path (label `needs-human-review`, never `automerge-on-green`).
"Upstream changed" applies to **data providers** (agencies), not to libraries like numpy/mypy.
The auto-merge job will refuse to merge any PR that changes files outside `connectors/`/`tests/`,
even if mislabeled.

## CI commands (what "green" means here)

```
ruff check src/ tests/
mypy src/csfs/
pytest tests/ -v --tb=short -m "not network"
```

Never make CI pass by skipping/weakening tests, loosening assertions, or marking things `network`
to deselect them. Fix the cause or classify honestly.
