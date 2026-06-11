# Contributing to CSFS

Thank you for considering a contribution! The most valuable contribution to
CSFS is a new provider connector — there are dozens of national and regional
hydrological agencies whose data is still out of reach.

## Development setup

```bash
git clone https://github.com/DarriEy/CSFS.git
cd CSFS
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev,api]"
```

Run the checks the CI runs:

```bash
ruff check src/ tests/
mypy src/csfs/
pytest -m "not network"
```

## Hermetic test policy

The test suite must pass **offline**. An autouse fixture in
`tests/conftest.py` blocks DNS resolution for any non-local host, so an
unmocked HTTP call fails fast instead of hanging. Consequences:

- Connector tests must mock their HTTP with [respx](https://lundberg.github.io/respx/)
  (or equivalent) — never hit the real upstream.
- A test that intentionally reaches a real API must be marked
  `@pytest.mark.network`. CI deselects these via `-m "not network"`.

## Adding a connector

1. Create `src/csfs/connectors/your_provider.py`.
2. Subclass `BaseConnector` and implement `fetch_stations()` and
   `fetch_observations()`. Normalize discharge to **m³/s** and timestamps to
   **UTC**; set the class metadata (`slug`, `display_name`, `base_url`,
   `country_codes`).
3. Decorate the class with `@register("your_slug")` — the slug must equal the
   class's `slug` attribute.
4. Add a provider entry to `inventory/providers.yaml` with
   `status: implemented`.
5. Add the slug to exactly one cron tier in
   `src/csfs/scheduler/cron.py` (`PROVIDER_TIERS`): `realtime`, `hourly`,
   `daily`, or `weekly`, matching the provider's update cadence.
6. Write `tests/connectors/test_your_slug.py` with mocked HTTP covering
   station discovery and observation parsing (units, timestamps, quality
   flags, error paths).

## Roster-integrity requirements

`tests/test_connector_integrity.py` mechanically enforces the roster's
honesty. Your PR will fail CI unless **all** of the following hold:

- **Test coverage is mandatory.** Every registered connector must either have
  its own `tests/connectors/test_<slug>.py` or be referenced by slug inside an
  existing connector test (for variant/subclass connectors exercised by a
  shared parent test).
- **Exactly one cron tier.** Every registered slug must appear in exactly one
  tier of `PROVIDER_TIERS` — an orphaned connector would never be acquired; a
  duplicated one would be acquired twice.
- **No ghost tiers.** Every slug listed in a tier must correspond to a
  registered connector (catches typos and deleted connectors).
- **Complete metadata.** Each connector class must expose a `slug` matching
  its registry key, a non-empty `display_name`, a non-empty `base_url`, and a
  `country_codes` list of ISO 3166-1 alpha-2 codes (or the `"global"`
  sentinel for global datasets).
- **Documented in the inventory.** Every registered connector needs an entry
  in `inventory/providers.yaml`.
- **No overclaiming.** An inventory entry may only carry
  `status: implemented` if its slug is actually registered. Anything
  documented but not built must be labeled `planned`, `research`, `manual`,
  or `fallback`.
- **Valid committed config.** Provider keys in the repo-root `csfs.yaml` must
  reference registered connectors.

## Style

- `ruff` (lint + import order) and `mypy` must pass; both run in CI.
- Line length 120; target Python 3.11+.
- Start new source files with the SPDX header used across the codebase.

## Reporting issues

Use the issue templates: **Bug report** for defects, **Provider request** to
propose a new streamflow agency or network.

By contributing you agree that your contributions are licensed under
GPL-3.0-or-later.
