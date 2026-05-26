"""Tests for the cron scheduler — config constants and run_scheduled_cycle."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest

from csfs.scheduler.cron import (
    DEFAULT_SCHEDULES,
    PROVIDER_TIERS,
    TIER_LOOKBACK,
    run_scheduled_cycle,
)


def test_default_schedules_valid_cron():
    """All default schedules are valid cron expressions."""
    from croniter import croniter

    for name, expr in DEFAULT_SCHEDULES.items():
        assert croniter.is_valid(expr), f"Invalid cron: {name}={expr}"


def test_all_tiers_present():
    assert set(DEFAULT_SCHEDULES.keys()) == {"realtime", "hourly", "daily", "weekly"}
    assert set(PROVIDER_TIERS.keys()) == {"realtime", "hourly", "daily", "weekly"}
    assert set(TIER_LOOKBACK.keys()) == {"realtime", "hourly", "daily", "weekly"}


def test_tier_lookbacks_increasing():
    assert TIER_LOOKBACK["realtime"] < TIER_LOOKBACK["hourly"]
    assert TIER_LOOKBACK["hourly"] < TIER_LOOKBACK["daily"]
    assert TIER_LOOKBACK["daily"] < TIER_LOOKBACK["weekly"]


def test_all_tier_providers_are_strings():
    for tier, slugs in PROVIDER_TIERS.items():
        assert isinstance(slugs, list), f"Tier {tier} is not a list"
        for slug in slugs:
            assert isinstance(slug, str), f"Provider {slug} in {tier} is not a string"


def test_no_duplicate_providers_across_tiers():
    """Each provider appears in exactly one tier."""
    seen: dict[str, str] = {}
    for tier, slugs in PROVIDER_TIERS.items():
        for slug in slugs:
            assert slug not in seen, f"Provider {slug} in both {seen[slug]} and {tier}"
            seen[slug] = tier


@pytest.mark.asyncio
async def test_run_scheduled_cycle_uses_tier_providers(tmp_path):
    mock_result = {"usgs": {"status": "ok", "observations": 100}}

    with patch(
        "csfs.scheduler.cron.run_acquisition",
        new_callable=AsyncMock,
        return_value=mock_result,
    ) as mock_run:
        result = await run_scheduled_cycle(
            str(tmp_path / "test.duckdb"),
            tier="realtime",
        )

    assert result == mock_result
    call_kwargs = mock_run.call_args
    assert call_kwargs.kwargs.get("providers") == PROVIDER_TIERS["realtime"]
    assert call_kwargs.kwargs.get("lookback_hours") == TIER_LOOKBACK["realtime"]


@pytest.mark.asyncio
async def test_run_scheduled_cycle_explicit_providers(tmp_path):
    mock_result = {"usgs": {"status": "ok"}}

    with patch(
        "csfs.scheduler.cron.run_acquisition",
        new_callable=AsyncMock,
        return_value=mock_result,
    ) as mock_run:
        result = await run_scheduled_cycle(
            str(tmp_path / "test.duckdb"),
            providers=["usgs"],
        )

    assert result == mock_result
    call_kwargs = mock_run.call_args
    assert call_kwargs.kwargs.get("providers") == ["usgs"]


@pytest.mark.asyncio
async def test_run_scheduled_cycle_default_lookback(tmp_path):
    """Without a tier, lookback defaults to daily (168h)."""
    with patch(
        "csfs.scheduler.cron.run_acquisition",
        new_callable=AsyncMock,
        return_value={},
    ) as mock_run:
        await run_scheduled_cycle(
            str(tmp_path / "test.duckdb"),
            providers=["usgs"],
        )

    call_kwargs = mock_run.call_args
    assert call_kwargs.kwargs.get("lookback_hours") == 168
