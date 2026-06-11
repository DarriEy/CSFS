## Summary

What does this PR change, and why?

## Type of change

- [ ] New provider connector
- [ ] Bug fix
- [ ] Core / store / scheduler change
- [ ] Documentation
- [ ] Other (describe)

## Checklist

- [ ] `ruff check src/ tests/` passes
- [ ] `mypy src/csfs/` passes
- [ ] `pytest -m "not network"` passes (the suite is hermetic — no test may
      reach a real upstream unless marked `@pytest.mark.network`)

### For new connectors (roster-integrity guards)

`tests/test_connector_integrity.py` will fail CI unless all of these hold:

- [ ] Class metadata complete: `slug` (matches the registry key),
      `display_name`, `base_url`, `country_codes`
- [ ] Slug added to **exactly one** tier in `PROVIDER_TIERS`
      (`src/csfs/scheduler/cron.py`)
- [ ] Entry added to `inventory/providers.yaml` with an honest `status`
- [ ] Hermetic test `tests/connectors/test_<slug>.py` with mocked HTTP
      (units normalized to m³/s, timestamps to UTC, error paths covered)

## Notes for the reviewer

Anything that needs special attention (upstream quirks, rate limits,
licensing of the source data, etc.).
