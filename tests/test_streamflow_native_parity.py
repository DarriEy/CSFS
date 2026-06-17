# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2024-2026 SYMFLUENCE Team <dev@symfluence.org>

"""Streamflow native <-> community parity harness (Phase 2 gate).

For each drop-in provider, proves the CSFS pipeline produces the same processed
streamflow CSV as SYMFLUENCE's native handler, given the same underlying
observations — the parity grade declared on each ObservationCapability.

Hermetic by construction: one canonical dataset is rendered into each pipeline's
raw input (the provider-native raw layout for the native handler, a CSFS raw CSV
for the community handler), both are ``process()``'d, and the outputs compared
at the declared grade:

* USGS — ``bit-identical``: cfs values are exactly representable and the native
  handler multiplies by ``CFS_TO_CMS`` in memory; CSFS reads its raw with
  ``float_precision="round_trip"``. Compared **byte-for-byte**.
* WSC / SMHI — ``value-identical``: the native handlers read cms straight from a
  CSV with pandas' DEFAULT (≈1-ULP-lossy) float parser, so byte parity is not
  guaranteed against CSFS's round_trip read. Compared by **value** to a tight
  float tolerance — the hydrologically-meaningful gate.

Requires symfluence (the native reference); skipped where absent, so CSFS keeps
no framework dependency. This is the framework-side gate run where both are
installed.
"""
from __future__ import annotations

import importlib
import logging
from collections.abc import Callable
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import NamedTuple

import pytest

logger = logging.getLogger("test_streamflow_native_parity")

# 9 daily UTC steps fully inside the experiment window [2020-01-01, 2020-01-10).
_START = datetime(2020, 1, 1, tzinfo=UTC)
_N = 9
_TIMESTAMPS = [_START + timedelta(days=i) for i in range(_N)]


def _config(data_dir: Path) -> dict:
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
        "STATION_ID": "00000000",
    }


# --- native raw-layout writers (one per provider's process() input) ----------

def _write_usgs_rdb(path: Path, native_values: list[float]) -> Path:
    """NWIS IV RDB: cfs in column 00060, tz_cd=UTC (no shift)."""
    lines = [
        "# USGS NWIS instantaneous values (synthetic parity fixture)",
        "agency_cd\tsite_no\tdatetime\ttz_cd\t00060\t00060_cd",
        "5s\t15s\t20d\t6s\t14n\t10s",  # RDB format-info line; native skips it
    ]
    for ts, cfs in zip(_TIMESTAMPS, native_values):
        lines.append(f"USGS\t01646500\t{ts.strftime('%Y-%m-%d %H:%M')}\tUTC\t{cfs}\tA")
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    return path


def _write_wsc_csv(path: Path, native_values: list[float]) -> Path:
    """WSC GeoMet-shaped CSV: cms in VALUE, ISO datetime in DATE."""
    rows = ["DATE,VALUE"]
    for ts, cms in zip(_TIMESTAMPS, native_values):
        rows.append(f"{ts.strftime('%Y-%m-%dT%H:%M:%SZ')},{cms}")
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return path


def _write_smhi_csv(path: Path, native_values: list[float]) -> Path:
    """SMHI hydroobs-shaped CSV: cms in discharge_m3s, ISO datetime in date."""
    rows = ["date,discharge_m3s,quality_code"]
    for ts, cms in zip(_TIMESTAMPS, native_values):
        rows.append(f"{ts.strftime('%Y-%m-%dT%H:%M:%SZ')},{cms},G")
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return path


def _write_hubeau_json(path: Path, native_values: list[float]) -> Path:
    """Hub'Eau daily JSON: L/s in resultat_obs_elab, date in date_obs_elab."""
    import json

    data = [
        {
            "date_obs_elab": ts.strftime("%Y-%m-%d"),
            "resultat_obs_elab": lps,  # litres per second
            "code_station": "M107302001",
            "libelle_qualification": "Bonne",
        }
        for ts, lps in zip(_TIMESTAMPS, native_values)
    ]
    path.write_text(json.dumps({"data_type": "daily", "data": data}), encoding="utf-8")
    return path


class _ParitySpec(NamedTuple):
    provider_id: str
    native_module: str
    native_class: str
    write_native_raw: Callable[[Path, list[float]], Path]
    #: values in the native raw's unit (cfs for USGS, cms for WSC/SMHI)
    native_values: list[float]
    #: convert one native-unit value to the cms value CSFS ingests
    to_cms: Callable[[float, float], float]
    bit_identical: bool


_CFS = [100.0, 110.0, 125.0, 140.0, 155.0, 160.0, 175.0, 180.0, 195.0]
_CMS = [2.831, 3.114, 3.539, 12.5, 23.75, 31.0, 4.95, 5.097, 5.521]
_LPS = [v * 1000.0 for v in _CMS]  # Hub'Eau reports litres per second

SPECS = {
    "USGS": _ParitySpec(
        provider_id="USGS",
        native_module="symfluence.data.observation.handlers.usgs",
        native_class="USGSStreamflowHandler",
        write_native_raw=_write_usgs_rdb,
        native_values=_CFS,
        to_cms=lambda cfs, cfs_to_cms: cfs * cfs_to_cms,
        bit_identical=True,
    ),
    "WSC": _ParitySpec(
        provider_id="WSC",
        native_module="symfluence.data.observation.handlers.wsc",
        native_class="WSCStreamflowHandler",
        write_native_raw=_write_wsc_csv,
        native_values=_CMS,
        to_cms=lambda cms, _cfs_to_cms: cms,  # WSC is already cms
        bit_identical=False,
    ),
    "SMHI": _ParitySpec(
        provider_id="SMHI",
        native_module="symfluence.data.observation.handlers.smhi",
        native_class="SMHIStreamflowHandler",
        write_native_raw=_write_smhi_csv,
        native_values=_CMS,
        to_cms=lambda cms, _cfs_to_cms: cms,  # SMHI is already cms
        bit_identical=False,
    ),
    "HUBEAU": _ParitySpec(
        provider_id="HUBEAU",
        native_module="symfluence.data.observation.handlers.hubeau",
        native_class="HubEauStreamflowHandler",
        write_native_raw=_write_hubeau_json,
        native_values=_LPS,
        to_cms=lambda lps, _cfs_to_cms: lps / 1000.0,  # Hub'Eau L/s -> m3/s
        bit_identical=False,
    ),
}


def _write_csfs_raw(path: Path, cms_values: list[float]) -> Path:
    """CSFS raw CSV (discharge already in m3/s); read back with round_trip precision."""
    rows = ["timestamp,discharge_m3s"]
    for ts, cms in zip(_TIMESTAMPS, cms_values):
        rows.append(f"{ts.strftime('%Y-%m-%dT%H:%M:%SZ')},{cms!r}")
    path.write_text("\n".join(rows) + "\n", encoding="utf-8")
    return path


@pytest.fixture(autouse=True)
def _requires_symfluence():
    pytest.importorskip("symfluence")
    pytest.importorskip("pandas")


@pytest.mark.parametrize("spec", list(SPECS.values()), ids=list(SPECS))
def test_processed_csv_parity_native_vs_community(spec: _ParitySpec, tmp_path):
    import symfluence  # noqa: F401 — bootstrap registers handlers + the csfs plugin
    from symfluence.core.constants import UnitConversion

    import csfs.integrations.symfluence as integration

    cfs_to_cms = UnitConversion.CFS_TO_CMS

    # --- native reference: import the handler class DIRECTLY. The CSFS plugin
    #     registers drop-ins under the bare provider keys in
    #     R.observation_handlers (shadow-wrapper design), so the registry no
    #     longer points at the native implementation. ---------------------
    native_cls = getattr(importlib.import_module(spec.native_module), spec.native_class)
    native_handler = native_cls(_config(tmp_path / "native"), logger)
    native_raw = spec.write_native_raw(tmp_path / "native_raw", spec.native_values)
    native_csv = native_handler.process(native_raw)

    # --- community (CSFS) ------------------------------------------------
    cms_values = [spec.to_cms(v, cfs_to_cms) for v in spec.native_values]
    csfs_handler = integration.CSFSStreamflowHandler(_config(tmp_path / "csfs"), logger)
    csfs_raw = _write_csfs_raw(tmp_path / "csfs_x_raw.csv", cms_values)
    csfs_csv = csfs_handler.process(csfs_raw)

    native_text = native_csv.read_text()
    csfs_text = csfs_csv.read_text()

    import numpy as np
    import pandas as pd

    def _read_utc_naive(csv_path):
        df = pd.read_csv(csv_path, parse_dates=["datetime"], index_col="datetime")
        # Native WSC/SMHI preserve tz-aware UTC timestamps (their upstreams carry
        # tz); CSFS standardizes to tz-naive UTC. Same instants, different dtype
        # in the CSV — normalize to compare UTC instants + values, not tz repr.
        if isinstance(df.index.dtype, pd.DatetimeTZDtype):
            df.index = df.index.tz_convert("UTC").tz_localize(None)
        return df

    native_df = _read_utc_naive(native_csv)
    csfs_df = _read_utc_naive(csfs_csv)

    # Structural parity: same single column, same UTC timesteps.
    assert list(native_df.columns) == list(csfs_df.columns) == ["discharge_cms"]
    assert native_df.index.equals(csfs_df.index), (
        f"{spec.provider_id} timestep mismatch:\n"
        f"native={list(native_df.index)}\ncsfs={list(csfs_df.index)}"
    )

    if spec.bit_identical:
        # USGS: byte-for-byte (native strips tz via tz_cd -> naive, matching
        # CSFS, so the raw CSVs are identical). The declared bit-identical grade.
        assert native_text == csfs_text, (
            f"{spec.provider_id} parity regression (expected bit-identical):\n"
            f"--- native ---\n{native_text}\n--- csfs ---\n{csfs_text}"
        )
    else:
        # WSC/SMHI: value-identical to float tolerance. Native reads cms with
        # pandas' default ~1-ULP-lossy parser and emits tz-aware timestamps, so
        # byte parity is not promised; equal UTC instants + values is the gate.
        np.testing.assert_allclose(
            csfs_df["discharge_cms"].to_numpy(),
            native_df["discharge_cms"].to_numpy(),
            rtol=1e-9, atol=0.0,
            err_msg=(
                f"{spec.provider_id} value-parity regression:\n"
                f"--- native ---\n{native_text}\n--- csfs ---\n{csfs_text}"
            ),
        )


def test_capabilities_declare_posture_and_grade():
    """Each drop-in provider declares its license posture and a parity grade."""
    pytest.importorskip("symfluence")
    import csfs.integrations.symfluence as integration

    by_id = {s.provider_id: s for s in integration.OBSERVATION_CAPABILITIES}
    expected = {
        "USGS": ("open", "public-domain", "bit-identical"),
        "WSC": ("attribution", "OGL-Canada-2.0", "value-identical:float-repr"),
        "SMHI": ("attribution", "CC-BY-4.0", "value-identical:rounding"),
        "HUBEAU": ("attribution", "Licence-Ouverte-2.0", "value-identical:unit-conversion"),
    }
    for provider, (redistribution, lic, grade) in expected.items():
        cap = by_id[provider]
        assert cap.redistribution == redistribution, provider
        assert cap.data_license == lic, provider
        assert cap.parity_grade == grade, provider
