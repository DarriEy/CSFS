"""Operational-integrity guards for the connector roster.

These tests catch silent operational regressions that unit tests miss:
- a connector that is registered but assigned to no cron tier (it would never
  be acquired), or assigned to several;
- a tier referencing a slug that no longer exists (typo / deleted connector);
- a connector class missing required metadata, or whose slug disagrees with the
  key it is registered under.
"""

from __future__ import annotations

import pytest

from csfs.core.registry import discover, get_connector, list_providers
from csfs.scheduler.cron import (
    DEFAULT_SCHEDULES,
    PROVIDER_TIERS,
    TIER_LOOKBACK,
)


@pytest.fixture(scope="module")
def registered() -> set[str]:
    discover()
    return set(list_providers())


@pytest.fixture(scope="module")
def tier_assignments() -> dict[str, int]:
    """slug -> number of tiers it appears in."""
    counts: dict[str, int] = {}
    for slugs in PROVIDER_TIERS.values():
        for slug in slugs:
            counts[slug] = counts.get(slug, 0) + 1
    return counts


def test_every_connector_is_in_exactly_one_tier(registered, tier_assignments):
    """A registered connector must run on exactly one schedule."""
    orphaned = sorted(s for s in registered if s not in tier_assignments)
    duplicated = sorted(s for s, n in tier_assignments.items() if n > 1)
    assert not orphaned, f"registered connectors with no cron tier: {orphaned}"
    assert not duplicated, f"connectors assigned to multiple tiers: {duplicated}"


def test_no_tier_references_an_unregistered_connector(registered, tier_assignments):
    """Every slug in a tier must correspond to a registered connector."""
    ghosts = sorted(s for s in tier_assignments if s not in registered)
    assert not ghosts, f"tiers reference unregistered connectors: {ghosts}"


def test_every_tier_has_a_schedule_and_lookback():
    """A tier without a cron schedule or lookback would break the daemon."""
    tiers = set(PROVIDER_TIERS)
    assert tiers <= set(DEFAULT_SCHEDULES), (
        f"tiers missing a schedule: {sorted(tiers - set(DEFAULT_SCHEDULES))}"
    )
    assert tiers <= set(TIER_LOOKBACK), (
        f"tiers missing a lookback: {sorted(tiers - set(TIER_LOOKBACK))}"
    )


def test_connector_classes_have_required_metadata(registered):
    """Each connector exposes the metadata the scheduler/store/API rely on."""
    problems: list[str] = []
    for slug in registered:
        cls = get_connector(slug)

        if getattr(cls, "slug", None) != slug:
            problems.append(f"{slug}: class.slug={getattr(cls, 'slug', None)!r} != registry key")

        if not getattr(cls, "display_name", ""):
            problems.append(f"{slug}: missing display_name")

        base_url = getattr(cls, "base_url", "")
        if not isinstance(base_url, str) or not base_url:
            problems.append(f"{slug}: missing/invalid base_url")

        codes = getattr(cls, "country_codes", None)
        # Either ISO 3166-1 alpha-2 codes, or the "global" sentinel used by
        # global datasets (caravan, gsim).
        if not isinstance(codes, list) or not codes:
            problems.append(f"{slug}: missing country_codes")
        elif not all(
            isinstance(c, str) and (len(c) == 2 or c == "global") for c in codes
        ):
            problems.append(f"{slug}: invalid country_codes: {codes}")

    assert not problems, "connector metadata problems:\n" + "\n".join(problems)


def test_committed_config_references_only_registered_providers(registered):
    """A typo in csfs.yaml's provider keys would silently no-op that config."""
    from pathlib import Path

    from csfs.core.config import load_config

    config_path = Path(__file__).resolve().parent.parent / "csfs.yaml"
    if not config_path.is_file():
        pytest.skip("no csfs.yaml at repo root")

    configured = set(load_config(config_path))
    unknown = sorted(configured - registered)
    assert not unknown, f"csfs.yaml configures unregistered providers: {unknown}"
