"""Tests for the dataset download module."""

import zipfile
from pathlib import Path

import httpx
import pytest
import respx

from csfs.core.downloads import DATASETS, download_dataset


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
