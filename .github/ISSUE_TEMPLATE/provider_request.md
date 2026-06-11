---
name: Provider request
about: Propose a new streamflow agency, network, or dataset for CSFS
title: "Provider request: "
labels: provider, enhancement
assignees: ""
---

**Provider**

- Agency / network / dataset name:
- Country or region covered:
- Website:

**Data access**

- API or download URL (if known):
- Access model: open / registration / API key / restricted / no API (scraping or manual download)
- Data format(s): JSON / CSV / WaterML / NetCDF / other
- Realtime, recent, or archive-only?
- Approximate number of discharge stations:
- License / terms of use (link if possible):

**Why it matters**

What gap does this provider fill (region, density, record length, realtime
latency)?

**Checklist for whoever implements it**

A new connector must satisfy the roster-integrity guards
(`tests/test_connector_integrity.py`):

- [ ] Connector module in `src/csfs/connectors/` registered with `@register("slug")`
- [ ] Entry in `inventory/providers.yaml` (only `status: implemented` once the connector is registered)
- [ ] Exactly one cron tier in `PROVIDER_TIERS` (`src/csfs/scheduler/cron.py`)
- [ ] Hermetic test in `tests/connectors/test_<slug>.py` (mocked HTTP; no live calls)

See [CONTRIBUTING.md](../../CONTRIBUTING.md) for the full walkthrough.
