"""Tests for the CSFS CLI (src/csfs/cli/main.py)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock, patch

import duckdb
import pytest
from click.testing import CliRunner

from csfs.cli.main import cli
from csfs.store.duckdb_store import _INIT_SQL


@pytest.fixture
def runner():
    return CliRunner()


@pytest.fixture
def test_db(tmp_path):
    """Create a DuckDB with schema + sample data and return the path."""
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(_INIT_SQL)

    # Insert sample stations
    conn.execute("""
        INSERT INTO stations (id, provider, native_id, name, latitude, longitude, country_code, river)
        VALUES
            ('usgs:01646500', 'usgs', '01646500', 'Potomac River near DC', 38.95, -77.13, 'US', 'Potomac'),
            ('usgs:01638500', 'usgs', '01638500', 'Potomac River at Point of Rocks', 39.27, -77.54, 'US', 'Potomac'),
            ('uk_ea:TH001', 'uk_ea', 'TH001', 'Thames at Kingston', 51.41, -0.31, 'GB', 'Thames')
    """)

    # Insert sample observations — use recent timestamps so status health shows "ok"
    now = datetime.now(UTC)
    recent = now - timedelta(hours=2)
    conn.execute("""
        INSERT INTO observations (station_id, timestamp, discharge_m3s, quality, fetched_at)
        VALUES
            ('usgs:01646500', ?, 150.5, 'good', ?),
            ('usgs:01646500', ?, 145.2, 'good', ?),
            ('usgs:01638500', ?, 200.0, 'good', ?),
            ('uk_ea:TH001',   ?, 65.3,  'good', ?)
    """, [
        recent - timedelta(hours=1), recent,
        recent, recent,
        recent, recent,
        recent, recent,
    ])

    # Insert acquisition_log rows
    conn.execute("""
        INSERT INTO acquisition_log
            (provider, started_at, duration_s, status, stations, observations, fetched, failed, retried, recovered, error_message)
        VALUES
            ('usgs',  ?, 45.0, 'ok',       100, 5000, 100, 0, 0, 0, NULL),
            ('usgs',  ?, 50.0, 'degraded', 100, 3000, 100, 10, 10, 3, NULL),
            ('usgs',  ?, 48.0, 'ok',       100, 4800, 100, 2, 2, 2, NULL),
            ('uk_ea', ?, 30.0, 'error',    0,   0,    50,  50, 5, 0, 'Connection refused')
    """, [
        now - timedelta(hours=72),
        now - timedelta(hours=48),
        now - timedelta(hours=1),
        now - timedelta(hours=2),
    ])

    conn.close()
    return db_path


# ---- providers command ----

def test_providers_lists_registered(runner):
    """The providers command should list registered providers with tier info."""
    result = runner.invoke(cli, ["providers"])
    assert result.exit_code == 0
    assert "PROVIDER" in result.output
    assert "TIER" in result.output
    assert "providers registered" in result.output


# ---- stations command ----

def test_stations_lists_from_db(runner, test_db):
    """The stations command should list stations from the test database."""
    result = runner.invoke(cli, ["--db", str(test_db), "stations"])
    assert result.exit_code == 0
    assert "Found 3 stations" in result.output
    assert "Potomac" in result.output
    assert "Thames" in result.output


def test_stations_filter_by_provider(runner, test_db):
    """The stations command --provider flag should filter results."""
    result = runner.invoke(cli, ["--db", str(test_db), "stations", "-p", "uk_ea"])
    assert result.exit_code == 0
    assert "Found 1 stations" in result.output
    assert "Thames" in result.output


def test_stations_filter_by_country(runner, test_db):
    """The stations command --country flag should filter results."""
    result = runner.invoke(cli, ["--db", str(test_db), "stations", "-c", "GB"])
    assert result.exit_code == 0
    assert "Found 1 stations" in result.output
    assert "Thames" in result.output


# ---- status command ----

def test_status_shows_db_stats(runner, test_db):
    """The status command should show station and observation counts."""
    result = runner.invoke(cli, ["--db", str(test_db), "status"])
    assert result.exit_code == 0
    assert "Stations: 3" in result.output
    assert "Observations: 4" in result.output
    assert "Time range:" in result.output


def test_status_shows_per_provider_table(runner, test_db):
    """The status command should show a per-provider breakdown."""
    result = runner.invoke(cli, ["--db", str(test_db), "status"])
    assert result.exit_code == 0
    assert "usgs" in result.output
    assert "uk_ea" in result.output
    assert "PROVIDER" in result.output


def test_status_shows_acquisition_health(runner, test_db):
    """The status command should show acquisition health section."""
    result = runner.invoke(cli, ["--db", str(test_db), "status"])
    assert result.exit_code == 0
    assert "Acquisition health" in result.output
    assert "runs logged" in result.output
    assert "LAST RUN" in result.output
    assert "TREND" in result.output
    assert "SINCE OK" in result.output


def test_status_shows_country_count(runner, test_db):
    """The status command should display the number of countries."""
    result = runner.invoke(cli, ["--db", str(test_db), "status"])
    assert result.exit_code == 0
    assert "2 countries represented" in result.output


def test_status_history_flag(runner, test_db):
    """The status --history flag should show detailed acquisition history."""
    result = runner.invoke(cli, ["--db", str(test_db), "status", "--history", "3"])
    assert result.exit_code == 0
    assert "Detailed history" in result.output
    assert "last 3 per provider" in result.output
    # Should have the detailed column headers
    assert "DUR" in result.output
    assert "STA" in result.output
    assert "OBS" in result.output
    assert "FAIL" in result.output


def test_status_history_shows_error_messages(runner, test_db):
    """The status --history should show error messages from failed runs."""
    result = runner.invoke(cli, ["--db", str(test_db), "status", "--history", "3"])
    assert result.exit_code == 0
    assert "Connection refused" in result.output


def test_status_missing_db(runner, tmp_path):
    """The status command should handle a missing database gracefully."""
    missing = tmp_path / "nonexistent.duckdb"
    result = runner.invoke(cli, ["--db", str(missing), "status"])
    assert result.exit_code == 0
    assert "No database found" in result.output


def test_status_empty_db(runner, tmp_path):
    """The status command should work on an empty (schema-only) database."""
    db_path = tmp_path / "empty.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(_INIT_SQL)
    conn.close()

    result = runner.invoke(cli, ["--db", str(db_path), "status"])
    assert result.exit_code == 0
    assert "Stations: 0" in result.output
    assert "Observations: 0" in result.output


def test_status_trend_calculation(runner, test_db):
    """The status command should compute trend for providers with >= 3 runs."""
    result = runner.invoke(cli, ["--db", str(test_db), "status"])
    assert result.exit_code == 0
    # usgs has 3 runs, so should have a computed trend (not "---")
    lines = result.output.split("\n")
    usgs_acq_lines = [l for l in lines if "usgs" in l and ("stable" in l or "improving" in l or "worsening" in l)]
    assert len(usgs_acq_lines) >= 1, "Expected usgs to have a computed trend"


# ---- fetch command ----

def test_fetch_calls_run_acquisition(runner, tmp_path):
    """The fetch command should call run_acquisition with the correct args."""
    db_path = tmp_path / "fetch_test.duckdb"
    # Create schema so DuckDBStore can open
    conn = duckdb.connect(str(db_path))
    conn.execute(_INIT_SQL)
    conn.close()

    mock_results = {
        "usgs": {
            "status": "ok",
            "stations": 50,
            "observations": 2500,
            "fetched": 50,
            "failed": 0,
        }
    }

    with patch("csfs.cli.main.click") as _:
        # We need to patch run_acquisition inside the fetch command's local import
        with patch("csfs.scheduler.runner.run_acquisition", new_callable=AsyncMock, return_value=mock_results) as mock_run:
            result = runner.invoke(cli, [
                "--db", str(db_path),
                "fetch", "-p", "usgs", "--lookback", "24",
            ])
            assert result.exit_code == 0, f"CLI error: {result.output}"
            mock_run.assert_called_once()
            call_kwargs = mock_run.call_args
            # Verify keyword args
            assert call_kwargs.kwargs["lookback_hours"] == 24
            assert call_kwargs.kwargs["providers"] == ["usgs"]


def test_fetch_output_format(runner, tmp_path):
    """The fetch command should format provider results nicely."""
    db_path = tmp_path / "fetch_test.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(_INIT_SQL)
    conn.close()

    mock_results = {
        "usgs": {
            "status": "ok",
            "stations": 50,
            "observations": 2500,
            "fetched": 50,
            "failed": 0,
        },
        "uk_ea": {
            "status": "error",
            "error": "timeout",
        },
    }

    with patch("csfs.scheduler.runner.run_acquisition", new_callable=AsyncMock, return_value=mock_results):
        result = runner.invoke(cli, ["--db", str(db_path), "fetch"])
        assert result.exit_code == 0
        assert "usgs: 50 stations" in result.output
        assert "2500 obs" in result.output or "2,500 obs" in result.output
        assert "uk_ea: ERROR" in result.output
        assert "Total:" in result.output


def test_fetch_degraded_status(runner, tmp_path):
    """The fetch command should show DEGRADED for degraded providers."""
    db_path = tmp_path / "fetch_test.duckdb"
    conn = duckdb.connect(str(db_path))
    conn.execute(_INIT_SQL)
    conn.close()

    mock_results = {
        "usgs": {
            "status": "degraded",
            "observations": 3000,
            "failed": 10,
            "fetched": 100,
        },
    }

    with patch("csfs.scheduler.runner.run_acquisition", new_callable=AsyncMock, return_value=mock_results):
        result = runner.invoke(cli, ["--db", str(db_path), "fetch"])
        assert result.exit_code == 0
        assert "DEGRADED" in result.output
