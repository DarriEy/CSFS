# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright (C) 2024-2026 SYMFLUENCE Team <dev@symfluence.org>

"""Dataset-artifact integrity gate: archive checksum verification (contract 0.5.0).

The dataset-artifact tier admits a published dataset on a verifiable content
hash. These tests prove the CSFS-side enforcement of that hash — match passes,
any mismatch fails closed — hermetically, on a tiny synthetic archive (no
network, no multi-GB download).
"""
from __future__ import annotations

import hashlib

import pytest

from csfs.core import downloads

pytestmark = pytest.mark.unit


def _write(path, data: bytes = b"lamah-ice-archive-bytes"):
    path.write_bytes(data)
    return data


def test_matching_checksum_passes(tmp_path):
    archive = tmp_path / "lamah_ice.zip"
    data = _write(archive)
    digest = hashlib.md5(data).hexdigest()  # noqa: S324 — integrity check, not security
    downloads._verify_archive_checksum(archive, f"md5:{digest}")  # no raise


def test_mismatched_checksum_fails_closed(tmp_path):
    archive = tmp_path / "lamah_ice.zip"
    _write(archive)
    with pytest.raises(ValueError, match="checksum mismatch"):
        downloads._verify_archive_checksum(archive, "md5:" + "0" * 32)


def test_sha256_algorithm_supported(tmp_path):
    archive = tmp_path / "d.zip"
    data = _write(archive, b"abc")
    digest = hashlib.sha256(data).hexdigest()
    downloads._verify_archive_checksum(archive, f"sha256:{digest}")


def test_bare_hexdigest_defaults_to_sha256(tmp_path):
    archive = tmp_path / "d.zip"
    data = _write(archive, b"abc")
    downloads._verify_archive_checksum(archive, hashlib.sha256(data).hexdigest())


def test_unsupported_algorithm_raises(tmp_path):
    archive = tmp_path / "d.zip"
    _write(archive)
    with pytest.raises(ValueError, match="unsupported checksum algorithm"):
        downloads._verify_archive_checksum(archive, "crc32:deadbeef")


def test_lamah_ice_has_a_recorded_checksum():
    # The dataset-artifact tier requires a verifiable hash; LamaH-Ice's is the
    # HydroShare-published md5 for lamah_ice.zip.
    assert downloads._checksum_for("iceland_lamahice") == "md5:6246f7300c77ead2c9f097ad5da89ba9"


def test_unrecorded_dataset_returns_no_checksum():
    assert downloads._checksum_for("panama_stri") is None
