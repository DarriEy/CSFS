# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2024-2026 SYMFLUENCE Team <dev@symfluence.org>

"""USGS native <-> community parity harness (Phase 2 gate).

Proves the CSFS USGS pipeline produces the SAME processed streamflow CSV as
SYMFLUENCE's native USGS handler, given the same underlying observations — the
``parity_grade="bit-identical"`` claim declared on the USGS ObservationCapability.

Hermetic by construction: a single canonical dataset (UTC timestamps + cfs
discharge) is rendered into each pipeline's raw input — an NWIS RDB for the
native handler, a CSFS raw CSV for the community handler — and the two
``process()`` outputs are compared **byte-for-byte**. CSFS's input is
pre-converted to m3/s using the SAME ``CFS_TO_CMS`` constant the native handler
applies, and CSFS reads its raw CSV with ``float_precision="round_trip"`` so the
re-read floats are exact; an identical pipeline therefore yields identical bytes.
Any divergence — units, resampling, windowing, tz, or float precision — fails
the gate. This is the automated guard behind the ``bit-identical`` parity grade
declared on the USGS capability.

(Historical note: with pandas' DEFAULT ``read_csv`` float parser, CSFS lost
~1 ULP re-reading high-precision values, so this comparison was once value-only;
the ``round_trip`` fix restored true byte parity.)

Requires symfluence (the native reference). Skipped where it is absent, so it
never makes CSFS depend on the framework — it is the framework-side gate run in
an environment that has both installed.
"""
from __future__ import annotations

import logging
from datetime import UTC, datetime, timedelta

import pytest

logger = logging.getLogger("test_usgs_native_parity")

# Canonical observations: 9 daily UTC steps fully inside the experiment window
# [2020-01-01, 2020-01-10), so CSFS's inclusive .loc window filter and the
# native handler's unfiltered resample see the same rows.
_START = datetime(2020, 1, 1, tzinfo=UTC)
_CFS_VALUES = [100.0, 110.0, 125.0, 140.0, 155.0, 160.0, 175.0, 180.0, 195.0]
_TIMESTAMPS = [_START + timedelta(days=i) for i in range(len(_CFS_VALUES))]


def _config(data_dir):
    return {
        "SYMFLUENCE_DATA_DIR": str(data_dir),
        "SYMFLUENCE_CODE_DIR": str(data_dir / "code"),
        "DOMAIN_NAME": "ParityDomain",
        "EXPERIMENT_ID": "exp1",
        "EXPERIMENT_TIME_START": "2020-01-01 00:00",
        "EXPERIMENT_TIME_END": "2020-01-10 00:00",
        "DOMAIN_DEFINITION_METHOD": "lumped",
        "SUB_GRID_DISCRETIZATION": "lumped",
        "FORCING_DATASET": "ERA5",
        "HYDROLOGICAL_MODEL": "SUMMA",
        "FORCING_TIME_STEP_SIZE": 86400,  # daily resample on both sides
    }


def _write_native_rdb(path, cfs_to_cms):  # noqa: ARG001 - signature symmetry
    """Render the canonical data as an NWIS IV RDB file (native handler input)."""
    lines = [
        "# USGS NWIS instantaneous values (synthetic parity fixture)",
        "agency_cd\tsite_no\tdatetime\ttz_cd\t00060\t00060_cd",
        "5s\t15s\t20d\t6s\t14n\t10s",  # RDB format-info line; native skips it
    ]
    for ts, cfs in zip(_TIMESTAMPS, _CFS_VALUES):
        stamp = ts.strftime("%Y-%m-%d %H:%M")
        lines.append(f"USGS\t01646500\t{stamp}\tUTC\t{cfs}\tA")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _write_csfs_raw(path, cfs_to_cms):
    """Render the same data as a CSFS raw CSV (discharge already in m3/s)."""
    rows = ["timestamp,discharge_m3s"]
    for ts, cfs in zip(_TIMESTAMPS, _CFS_VALUES):
        # Pre-convert with the SAME constant the native handler applies; CSFS
        # reads this back with round_trip precision, so the floats are exact.
        rows.append(f"{ts.strftime('%Y-%m-%dT%H:%M:%SZ')},{cfs * cfs_to_cms!r}")
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return path


@pytest.fixture(autouse=True)
def _requires_symfluence():
    pytest.importorskip("symfluence")
    pytest.importorskip("pandas")


def test_usgs_processed_csv_is_bit_identical_native_vs_community(tmp_path):
    import symfluence  # noqa: F401 — bootstrap registers handlers + the csfs plugin
    from symfluence.core.constants import UnitConversion

    # Import the native handler class DIRECTLY, not via R.observation_handlers:
    # the CSFS plugin registers a drop-in under the 'usgs' registry key
    # (shadow-wrapper design), so the registry no longer points at the native
    # implementation. The parity reference must be the genuine native handler.
    from symfluence.data.observation.handlers.usgs import USGSStreamflowHandler

    import csfs.integrations.symfluence as integration

    cfs_to_cms = UnitConversion.CFS_TO_CMS

    # --- native reference ------------------------------------------------
    native_dir = tmp_path / "native"
    native_handler = USGSStreamflowHandler(_config(native_dir), logger)
    rdb = _write_native_rdb(tmp_path / "nwis_iv.rdb", cfs_to_cms)
    native_csv = native_handler.process(rdb)

    # --- community (CSFS) ------------------------------------------------
    csfs_dir = tmp_path / "csfs"
    csfs_handler = integration.CSFSStreamflowHandler(_config(csfs_dir), logger)
    csfs_raw = _write_csfs_raw(tmp_path / "csfs_usgs_01646500_raw.csv", cfs_to_cms)
    csfs_csv = csfs_handler.process(csfs_raw)

    # Byte-for-byte parity: identical input through equivalent pipelines must
    # yield identical processed CSVs (the bit-identical grade). The structural
    # read below is only for a readable diff if the byte assertion ever fails.
    native_text = native_csv.read_text()
    csfs_text = csfs_csv.read_text()
    if native_text != csfs_text:
        import pandas as pd

        native_df = pd.read_csv(native_csv, parse_dates=["datetime"], index_col="datetime")
        csfs_df = pd.read_csv(csfs_csv, parse_dates=["datetime"], index_col="datetime")
        assert list(native_df.columns) == list(csfs_df.columns) == ["discharge_cms"]
        assert native_df.index.equals(csfs_df.index), (
            f"timestep mismatch:\nnative={list(native_df.index)}\ncsfs={list(csfs_df.index)}"
        )
    assert native_text == csfs_text, (
        "USGS parity regression: CSFS processed CSV diverged from native.\n"
        f"--- native ---\n{native_text}\n--- csfs ---\n{csfs_text}"
    )


def test_usgs_capability_declares_open_and_graded():
    """USGS declares the public-domain posture and a parity grade (gate inputs)."""
    pytest.importorskip("symfluence")
    import csfs.integrations.symfluence as integration

    usgs = next(s for s in integration.OBSERVATION_CAPABILITIES if s.provider_id == "USGS")
    assert usgs.parity_grade == "bit-identical"  # the harness above guards this byte-for-byte
    assert usgs.redistribution == "open"
    assert usgs.data_license == "public-domain"
