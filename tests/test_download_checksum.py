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


def test_extract_archive_extracts_zip(tmp_path):
    import zipfile
    arc = tmp_path / "d.zip"
    with zipfile.ZipFile(arc, "w") as zf:
        zf.writestr("inner.csv", "a,b\n1,2\n")
    assert downloads._extract_archive(arc, tmp_path) is True
    assert (tmp_path / "inner.csv").is_file()


def test_extract_archive_leaves_bare_file(tmp_path):
    # A bare published .csv is the data itself: not extracted, kept in place
    # (the download flow must NOT delete it). Returns False so the caller knows.
    bare = tmp_path / "MasterTable.csv"
    bare.write_text("station_id,lat\nX,1.0\n", encoding="utf-8")
    assert downloads._extract_archive(bare, tmp_path) is False
    assert bare.is_file()


def test_content_hash_is_stable_and_layout_independent(tmp_path):
    # The content hash depends only on file contents + relative paths, so two
    # extractions of the same data (even written in a different order) match —
    # the property that lets CEH-style dynamically-rezipped archives be gated.
    a = tmp_path / "a"
    b = tmp_path / "b"
    for root in (a, b):
        (root / "sub").mkdir(parents=True)
    (a / "sub" / "x.csv").write_text("1,2\n", encoding="utf-8")
    (a / "y.txt").write_text("hello", encoding="utf-8")
    # Same content, written in a different order into b.
    (b / "y.txt").write_text("hello", encoding="utf-8")
    (b / "sub" / "x.csv").write_text("1,2\n", encoding="utf-8")
    ha = downloads._content_hash(a)
    hb = downloads._content_hash(b)
    assert ha == hb
    assert ha.startswith("content-sha256:")


def test_content_hash_changes_with_content(tmp_path):
    a = tmp_path / "a"
    a.mkdir()
    (a / "x.csv").write_text("1,2\n", encoding="utf-8")
    h1 = downloads._content_hash(a)
    (a / "x.csv").write_text("1,3\n", encoding="utf-8")  # changed byte
    assert downloads._content_hash(a) != h1


def test_extract_archive_detects_zip_by_content_not_extension(tmp_path):
    # Extension-less download (e.g. Dataverse .../datafile/<id>) that is really a
    # zip must still be extracted — detection is by magic bytes, not filename.
    import zipfile
    arc = tmp_path / "83022"  # no extension, like a Dataverse datafile id
    with zipfile.ZipFile(arc, "w") as zf:
        zf.writestr("sub/data.csv", "a,b\n1,2\n")
    assert downloads._extract_archive(arc, tmp_path) is True
    assert (tmp_path / "sub" / "data.csv").is_file()
