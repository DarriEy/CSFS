"""Tests for the dataset download module."""

import io
import tarfile
import zipfile
from pathlib import Path

import httpx
import pytest
import respx

from csfs.core import downloads
from csfs.core.downloads import DATASETS, download_dataset, ensure_dataset


def _make_test_zip(path: Path) -> bytes:
    """Create a minimal ZIP file with a test CSV."""
    import io

    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr("test_data.csv", "datetime,station_id,discharge\n2024-01-01,CHA,1.5\n")
    return buf.getvalue()


@pytest.mark.asyncio
@respx.mock
async def test_download_panama_stri(tmp_path):
    url = next(d["url"] for d in DATASETS if d["slug"] == "panama_stri")
    respx.get(url).mock(
        return_value=httpx.Response(200, content=_make_test_zip(tmp_path)),
    )

    ok = await download_dataset("panama_stri", tmp_path)
    assert ok is True

    dest = tmp_path / "panama_stri"
    assert dest.is_dir()
    assert (dest / "test_data.csv").exists()


@pytest.mark.asyncio
async def test_download_skips_existing(tmp_path):
    dest = tmp_path / "panama_stri"
    dest.mkdir()
    (dest / "existing.csv").write_text("data")

    ok = await download_dataset("panama_stri", tmp_path)
    assert ok is True


@pytest.mark.asyncio
async def test_download_manual_returns_false(tmp_path):
    ok = await download_dataset("grdc", tmp_path)
    assert ok is False


@pytest.mark.asyncio
async def test_download_unknown_slug(tmp_path):
    ok = await download_dataset("nonexistent", tmp_path)
    assert ok is False


@pytest.mark.asyncio
@respx.mock
async def test_download_http_error(tmp_path):
    url = next(d["url"] for d in DATASETS if d["slug"] == "panama_stri")
    respx.get(url).mock(return_value=httpx.Response(500))

    ok = await download_dataset("panama_stri", tmp_path)
    assert ok is False


def _make_test_tar_gz() -> bytes:
    """Create a minimal tar.gz with a nested CSV."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w:gz") as tf:
        data = b"YYYY;MM;DD;qobs\n1990;01;01;1.5\n"
        info = tarfile.TarInfo("D_gauges/2_timeseries/daily/ID_1.csv")
        info.size = len(data)
        tf.addfile(info, io.BytesIO(data))
    return buf.getvalue()


@pytest.mark.asyncio
@respx.mock
async def test_download_tar_gz_extracts(tmp_path):
    """A .tar.gz dataset (LamaH-CE) downloads and extracts via tarfile."""
    url = next(d["url"] for d in DATASETS if d["slug"] == "lamah_ce")
    respx.get(url).mock(
        return_value=httpx.Response(200, content=_make_test_tar_gz()),
    )

    ok = await download_dataset("lamah_ce", tmp_path)
    assert ok is True

    extracted = tmp_path / "lamah_ce" / "D_gauges" / "2_timeseries" / "daily" / "ID_1.csv"
    assert extracted.is_file()


def test_archive_name_from_url_strips_zenodo_content():
    """Zenodo /content URLs resolve to the real archive filename."""
    assert downloads._archive_name_from_url(
        "https://zenodo.org/api/records/5153305/files/2_LamaH-CE_daily.tar.gz/content",
        "lamah_ce",
    ) == "2_LamaH-CE_daily.tar.gz"
    assert downloads._archive_name_from_url(
        "https://example.org/x/acp_discharge_15min.zip", "panama_stri",
    ) == "acp_discharge_15min.zip"


def test_safe_extract_tar_rejects_escape(tmp_path):
    """tar-slip members escaping the destination are rejected."""
    buf = io.BytesIO()
    with tarfile.open(fileobj=buf, mode="w") as tf:
        info = tarfile.TarInfo("../escape.csv")
        info.size = 0
        tf.addfile(info, io.BytesIO(b""))
    buf.seek(0)
    with tarfile.open(fileobj=buf) as tf, pytest.raises(ValueError, match="escapes"):
        downloads._safe_extract_tar(tf, tmp_path)


# ---------------------------------------------------------------------------
# ensure_dataset — the connector bridge
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_ensure_dataset_explicit_data_dir():
    """An explicit data_dir is returned unchanged (no download)."""
    result = await ensure_dataset("lamah_ce", {"data_dir": "/some/path"})
    assert result == Path("/some/path")


@pytest.mark.asyncio
async def test_ensure_dataset_auto_download_disabled():
    """With auto_download=False and no data_dir, returns None."""
    result = await ensure_dataset("lamah_ce", {"auto_download": False})
    assert result is None


@pytest.mark.asyncio
async def test_ensure_dataset_skips_when_present(tmp_path, monkeypatch):
    """A non-empty cache dir is used without triggering a download."""
    dest = tmp_path / "lamah_ce"
    dest.mkdir()
    (dest / "ID_1.csv").write_text("data")

    async def _fail(*_a, **_k):  # pragma: no cover - must not be called
        raise AssertionError("download_dataset should not be called")

    monkeypatch.setattr(downloads, "download_dataset", _fail)

    result = await ensure_dataset(
        "lamah_ce", {"datasets_dir": str(tmp_path)},
    )
    assert result == dest.resolve()


@pytest.mark.asyncio
async def test_ensure_dataset_triggers_download(tmp_path, monkeypatch):
    """When the cache is empty, ensure_dataset downloads then returns the dir."""
    called = {}

    async def _fake_download(slug, base_dir):
        called["slug"] = slug
        d = Path(base_dir) / slug
        d.mkdir(parents=True, exist_ok=True)
        (d / "ID_1.csv").write_text("data")
        return True

    monkeypatch.setattr(downloads, "download_dataset", _fake_download)

    result = await ensure_dataset(
        "lamah_ce", {"datasets_dir": str(tmp_path)},
    )
    assert called["slug"] == "lamah_ce"
    assert result == (tmp_path / "lamah_ce").resolve()


@pytest.mark.asyncio
async def test_ensure_dataset_download_failure_returns_none(tmp_path, monkeypatch):
    """A failed download yields None."""
    async def _fake_download(slug, base_dir):
        return False

    monkeypatch.setattr(downloads, "download_dataset", _fake_download)

    result = await ensure_dataset(
        "lamah_ce", {"datasets_dir": str(tmp_path)},
    )
    assert result is None
