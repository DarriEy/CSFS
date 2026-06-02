# SPDX-License-Identifier: GPL-3.0-or-later
"""Download freely-available datasets for local-file connectors.

Two layers live here:

* ``download_dataset`` / ``DATASETS`` — the registry + streaming downloader
  used by the ``csfs download-data`` CLI. Handles ZIP and tar.gz archives
  with path-traversal-safe extraction.
* ``ensure_dataset`` — the bridge the dataset connectors call from
  ``fetch_observations``. It resolves a managed cache directory and triggers
  an auto-download (once, cached) when the data isn't already on disk.
"""

from __future__ import annotations

import asyncio
import tarfile
import zipfile
from pathlib import Path

import httpx
import structlog

logger = structlog.get_logger()

# Default location for auto-downloaded datasets (matches csfs.yaml convention).
_DEFAULT_DATASETS_DIR = Path("data/datasets")

DATASETS: list[dict] = [
    {
        "slug": "panama_stri",
        "name": "Panama Canal Watershed (STRI)",
        "auto": True,
        "size": "~370 MB",
        "url": "https://biogeodb.stri.si.edu/physical_monitoring/downloads/acp_discharge_15min.zip",
    },
    {
        "slug": "israel_caravan",
        "name": "Caravan-Israel extension (Zenodo)",
        "auto": True,
        "size": "~300 MB",
        "url": "https://zenodo.org/api/records/15003600/files/Caravan_extension_Israel_Ver3.zip/content",
    },
    {
        "slug": "gsim",
        "name": "GSIM — Global Streamflow Indices (PANGAEA)",
        "auto": True,
        "size": "~970 MB",
        "url": "https://store.pangaea.de/Publications/GudmundssonL-etal_2018/GSIM_indices.zip",
    },
    {
        "slug": "caravan",
        "name": "Caravan — global large-sample hydrology (Zenodo)",
        "auto": True,
        "size": "~12.5 GB",
        "url": "https://zenodo.org/api/records/7540792/files/Caravan.zip/content",
    },
    {
        "slug": "lamah_ce",
        "name": "LamaH-CE — Central Europe large-sample hydrology (Zenodo, daily)",
        "auto": True,
        "size": "~1.5 GB",
        "url": "https://zenodo.org/api/records/5153305/files/2_LamaH-CE_daily.tar.gz/content",
    },
    {
        "slug": "iceland_lamahice",
        "name": "LamaH-Ice — Iceland large-sample hydrology (HydroShare, daily)",
        "auto": True,
        "size": "~636 MB",
        "url": "https://www.hydroshare.org/resource/86117a5f36cc4b7c90a5d54e18161c91/data/contents/lamah_ice.zip",
    },
    {
        "slug": "grdc",
        "name": "Global Runoff Data Centre",
        "auto": False,
        "size": "varies",
        "url": "https://grdc.bafg.de/data/data_portal/",
    },
    {
        "slug": "spain_cedex",
        "name": "Spain MITECO/CEDEX Yearbooks",
        "auto": False,
        "size": "~306 MB",
        "url": "https://www.miteco.gob.es/en/cartografia-y-sig/ide/descargas/agua/anuario-de-aforos.html",
    },
    {
        "slug": "vietnam_mekong",
        "name": "Vietnam Mekong Delta Discharge",
        "auto": False,
        "size": "varies",
        "url": "https://catalogue.ceh.ac.uk/documents/ac5b28ca-e087-4aec-974a-5a9f84b06595",
    },
    {
        "slug": "bolivia_ine",
        "name": "Bolivia INE Hydrological Data",
        "auto": False,
        "size": "varies",
        "url": "https://anda.ine.gob.bo/index.php/catalog/209",
    },
]


def _archive_name_from_url(url: str, slug: str) -> str:
    """Derive the real archive filename from a download URL.

    Handles Zenodo content URLs of the form ``.../files/<name>/content`` by
    stripping the trailing ``/content`` so the extension survives for type
    detection.
    """
    clean = url.split("?")[0].rstrip("/")
    if clean.endswith("/content"):
        clean = clean[: -len("/content")]
    name = clean.rsplit("/", 1)[-1]
    return name or f"{slug}.zip"


def _safe_extractall(zf: zipfile.ZipFile, dest: Path) -> None:
    """Extract all members, rejecting any whose path escapes ``dest`` (zip-slip)."""
    dest = dest.resolve()
    for member in zf.namelist():
        target = (dest / member).resolve()
        if target != dest and dest not in target.parents:
            raise ValueError(f"Unsafe path in archive escapes destination: {member!r}")
    zf.extractall(dest)


def _safe_extract_tar(tf: tarfile.TarFile, dest: Path) -> None:
    """Extract all tar members, rejecting any whose path escapes ``dest``."""
    dest = dest.resolve()
    for member in tf.getmembers():
        target = (dest / member.name).resolve()
        if target != dest and dest not in target.parents:
            raise ValueError(f"Unsafe path in archive escapes destination: {member.name!r}")
    # filter="data" (py3.12) additionally strips unsafe metadata/links.
    tf.extractall(dest, filter="data")


_ARCHIVE_SUFFIXES = (".zip", ".tar.gz", ".tgz", ".tar")


def _has_extracted_content(dest: Path) -> bool:
    """True if ``dest`` holds extracted data (any file that isn't an archive).

    A lone partial/complete archive left by a failed extraction does NOT count,
    so a previous incomplete download is retried rather than mistaken for ready.
    """
    if not dest.is_dir():
        return False
    return any(
        p.is_file() and not p.name.lower().endswith(_ARCHIVE_SUFFIXES)
        for p in dest.rglob("*")
    )


def _extract_archive(archive_path: Path, dest: Path) -> None:
    """Extract a ZIP or tar.gz archive into ``dest`` safely."""
    name = archive_path.name.lower()
    if name.endswith((".tar.gz", ".tgz", ".tar")):
        with tarfile.open(archive_path) as tf:
            _safe_extract_tar(tf, dest)
    else:
        with zipfile.ZipFile(archive_path) as zf:
            _safe_extractall(zf, dest)


async def download_dataset(slug: str, base_dir: Path) -> bool:
    """Download a single dataset. Returns True on success."""
    entry = next((d for d in DATASETS if d["slug"] == slug), None)
    if entry is None:
        logger.error("unknown_dataset", slug=slug)
        return False

    if not entry["auto"]:
        logger.info(
            "manual_download_required",
            slug=slug,
            url=entry["url"],
            hint=f"Download from {entry['url']} and place files in {base_dir / slug}/",
        )
        return False

    dest = base_dir / slug
    if _has_extracted_content(dest):
        logger.info("dataset_already_exists", slug=slug, path=str(dest))
        return True

    dest.mkdir(parents=True, exist_ok=True)

    return await _download_and_extract(slug, dest, entry["url"])


# Connection-level errors worth resuming a partial download on. Large archives
# (e.g. HydroShare's 636 MB LamaH-Ice) routinely have the peer close the
# connection mid-transfer; the storage backend supports HTTP range requests,
# so we resume from the bytes already on disk rather than starting over.
_RESUMABLE_ERRORS = (
    httpx.RemoteProtocolError,
    httpx.ReadError,
    httpx.ReadTimeout,
    httpx.ConnectError,
    httpx.ConnectTimeout,
    httpx.WriteError,
)
_MAX_RESUME_ATTEMPTS = 8


def _content_total(resp: httpx.Response, current: int | None) -> int | None:
    """Determine the full archive size from a 200 or 206 response."""
    if resp.status_code == 206:
        content_range = resp.headers.get("content-range", "")
        if "/" in content_range:
            try:
                return int(content_range.rsplit("/", 1)[-1])
            except ValueError:
                return current
    cl = resp.headers.get("content-length")
    if cl is not None and resp.status_code == 200:
        try:
            return int(cl)
        except ValueError:
            return current
    return current


async def _stream_to_file(
    client: httpx.AsyncClient, url: str, archive_path: Path,
) -> None:
    """Stream ``url`` to ``archive_path``, resuming on dropped connections."""
    downloaded = archive_path.stat().st_size if archive_path.exists() else 0
    total: int | None = None
    attempt = 0

    while True:
        headers = {"Range": f"bytes={downloaded}-"} if downloaded else {}
        try:
            async with client.stream("GET", url, headers=headers) as resp:
                # 416 on a resume means the file on disk is already complete.
                if resp.status_code == 416 and downloaded:
                    return
                resp.raise_for_status()
                # A 200 means the server ignored our Range — restart cleanly.
                if resp.status_code == 200 and downloaded:
                    downloaded = 0
                total = _content_total(resp, total)
                mode = "ab" if downloaded else "wb"
                with open(archive_path, mode) as f:
                    async for chunk in resp.aiter_bytes(chunk_size=1024 * 1024):
                        f.write(chunk)
                        downloaded += len(chunk)
        except _RESUMABLE_ERRORS as exc:
            attempt += 1
            if attempt > _MAX_RESUME_ATTEMPTS:
                raise
            logger.warning(
                "download_resuming",
                got_bytes=downloaded,
                total_bytes=total,
                attempt=attempt,
                error=str(exc),
            )
            await asyncio.sleep(min(2 ** attempt, 30))
            continue

        if total is None or downloaded >= total:
            return
        # Stream ended cleanly but short (server-side cutoff): resume.
        attempt = 0


async def _download_and_extract(slug: str, dest: Path, url: str) -> bool:
    """Download an archive (streaming, resumable) and extract it into dest."""
    archive_name = _archive_name_from_url(url, slug)
    archive_path = dest / archive_name
    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(7200.0, connect=60.0),
        ) as client:
            logger.info("downloading", slug=slug, url=url)
            await _stream_to_file(client, url, archive_path)
            logger.info(
                "download_complete",
                slug=slug,
                size_mb=round(archive_path.stat().st_size / 1e6, 1),
            )

        _extract_archive(archive_path, dest)
        # Drop the archive once extracted — saves significant disk (Caravan is
        # ~12.5 GB) and stops a leftover archive from masking a failed extract.
        archive_path.unlink(missing_ok=True)
        logger.info("dataset_extracted", slug=slug, files=len(list(dest.iterdir())))
        return True
    except Exception as exc:
        logger.error("download_failed", slug=slug, error=str(exc))
        return False


# ---------------------------------------------------------------------------
# Connector bridge: ensure a dataset is present on disk (auto-download once).
# ---------------------------------------------------------------------------

# Serialize concurrent first-fetches of the same dataset. fetch_observations
# runs per-station and concurrently in the runner, so without this several
# coroutines could race to download the same multi-GB archive.
_DOWNLOAD_LOCKS: dict[Path, asyncio.Lock] = {}


def _lock_for(dest: Path) -> asyncio.Lock:
    lock = _DOWNLOAD_LOCKS.get(dest)
    if lock is None:
        lock = asyncio.Lock()
        _DOWNLOAD_LOCKS[dest] = lock
    return lock


async def ensure_dataset(slug: str, config: dict | None) -> Path | None:
    """Resolve the local directory holding ``slug``'s dataset, downloading it.

    Resolution order:

    * If ``config['data_dir']`` is set, return it unchanged — the user manages
      that directory explicitly and no download is attempted.
    * Otherwise resolve a managed cache dir (``config['datasets_dir']`` or the
      default ``data/datasets``) and, when ``config['auto_download']`` is true
      (the default) and the data isn't already present, download + extract the
      archive once via :func:`download_dataset`.

    Returns the directory containing the dataset, or ``None`` if the data is
    unavailable (download disabled or failed).
    """
    config = config or {}

    data_dir = config.get("data_dir")
    if data_dir:
        return Path(data_dir)

    if not config.get("auto_download", True):
        return None

    base = Path(config.get("datasets_dir") or _DEFAULT_DATASETS_DIR)
    dest = (base / slug).resolve()

    async with _lock_for(dest):
        if _has_extracted_content(dest):
            return dest
        ok = await download_dataset(slug, base)
        if ok and _has_extracted_content(dest):
            return dest
    return None
