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
import hashlib
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
        # Zenodo-published content hash for 2_LamaH-CE_daily.tar.gz (record
        # 5153305, 1477248617 bytes). Verified before extraction (fail-closed).
        "checksum": "md5:69fd2733e969513403f923ecc5eaa3dc",
    },
    {
        # CAMELS-BR streamflow (primary archive; the observation source). The
        # attributes archive below carries the station coordinates.
        "slug": "camels_br",
        "name": "CAMELS-BR — Brazil large-sample hydrology, daily streamflow (Zenodo)",
        "auto": True,
        "size": "~135 MB",
        "url": "https://zenodo.org/api/records/3964745/files/02_CAMELS_BR_streamflow_m3s.zip/content",
        # Zenodo-published md5 for 02_CAMELS_BR_streamflow_m3s.zip (135342171 bytes).
        "checksum": "md5:599b96f48ec78e25751cf1cc691a22bb",
    },
    {
        "slug": "camels_br_attributes",
        "name": "CAMELS-BR — catchment attributes incl. gauge coordinates (Zenodo)",
        "auto": True,
        "size": "~0.3 MB",
        "url": "https://zenodo.org/api/records/3964745/files/01_CAMELS_BR_attributes.zip/content",
        # Zenodo-published md5 for 01_CAMELS_BR_attributes.zip (271002 bytes).
        "checksum": "md5:8bdb80831ce0ceb64ae14618e46cfae6",
    },
    {
        # CAMELS-CL streamflow matrix (the observation source). PANGAEA serves
        # direct store.pangaea.de zips with no auth/bot-protection. PANGAEA
        # publishes no per-file hash, so the checksum is self-recorded from a
        # verified download (md5 confirmed against the bytes).
        "slug": "camels_cl",
        "name": "CAMELS-CL — Chile large-sample hydrology, daily streamflow (PANGAEA)",
        "auto": True,
        "size": "~13 MB",
        "url": "https://store.pangaea.de/Publications/Alvarez-Garreton-etal_2018/2_CAMELScl_streamflow_m3s.zip",
        "checksum": "md5:3457bc87e444e1e7d84a1b703965708d",
    },
    {
        "slug": "camels_cl_attributes",
        "name": "CAMELS-CL — catchment attributes incl. gauge coordinates (PANGAEA)",
        "auto": True,
        "size": "~0.2 MB",
        "url": "https://store.pangaea.de/Publications/Alvarez-Garreton-etal_2018/1_CAMELScl_attributes.zip",
        "checksum": "md5:5cdfa46b675201068d5fc7f42470770c",
    },
    {
        # CAMELS-DE: single bundle (timeseries + attributes) — the authoritative
        # standalone dataset (replaces the former Caravan-derived camels_de alias).
        "slug": "camels_de",
        "name": "CAMELS-DE — Germany large-sample hydrology, v1.1.0 (Zenodo)",
        "auto": True,
        "size": "~2.2 GB",
        "url": "https://zenodo.org/api/records/16755906/files/camels_de.zip/content",
        # Zenodo-published md5 for camels_de.zip (2220260293 bytes).
        "checksum": "md5:5ee2f89f6204e8eafdbc11b491d34afb",
    },
    {
        # CAMELS-IND v2.2 — single bundle (streamflow matrix + attributes).
        "slug": "camels_ind",
        "name": "CAMELS-IND — India large-sample hydrology, v2.2 (Zenodo)",
        "auto": True,
        "size": "~178 MB",
        "url": "https://zenodo.org/api/records/14999580/files/CAMELS_IND_Catchments_Streamflow_Sufficient.zip/content",
        "checksum": "md5:3993c25ba7d7b86df0541de91e094f39",
    },
    {
        # CAMELS-DK streamflow (per-catchment obs CSVs) — the observation source.
        "slug": "camels_dk",
        "name": "CAMELS-DK — Denmark large-sample hydrology, gauged catchments (GEUS Dataverse)",
        "auto": True,
        "size": "~152 MB",
        "url": "https://dataverse.geus.dk/api/access/datafile/83022",
        "checksum": "md5:50b6d3957e6abf0017973ac872aea67f",
    },
    {
        # CAMELS-DK topography — a BARE published .csv with outlet coords
        # (easting/northing in EPSG:25832).
        "slug": "camels_dk_attributes",
        "name": "CAMELS-DK — topography incl. outlet coordinates (GEUS Dataverse)",
        "auto": True,
        "size": "~0.3 MB",
        "url": "https://dataverse.geus.dk/api/access/datafile/84631",
        "checksum": "md5:794bc56a7dfc6d9cf21a472daa25a4cd",
    },
    {
        # CAMELS-US — single bundle (per-basin streamflow + gauge metadata coords).
        "slug": "camels_us",
        "name": "CAMELS-US — USA large-sample hydrology, v1.2 (Zenodo/NCAR)",
        "auto": True,
        "size": "~3.4 GB",
        "url": "https://zenodo.org/api/records/15529996/files/basin_timeseries_v1p2_metForcing_obsFlow.zip/content",
        "checksum": "md5:8e9a466710e8270b58f01d332a87184f",
    },
    {
        # CAMELS-GB — single CEH bundle (timeseries + attributes incl. coords).
        # CEH regenerates the zip per request, so the archive md5 is NOT
        # reproducible; integrity is a CONTENT checksum over the extracted data,
        # excluding the readme.html (carries a per-download generation timestamp).
        "slug": "camels_gb",
        "name": "CAMELS-GB — Great Britain large-sample hydrology (CEH EIDC)",
        "auto": True,
        "size": "~256 MB",
        "url": "https://data-package.ceh.ac.uk/data/8344e4f3-d2ea-44f5-8afa-86d2987543a9.zip",
        "content_checksum": "content-sha256:de33e2731d7285423801db723acbd0c8d97c1505b3d184830032c755a341742c",
        "content_exclude": ["readme.html"],
    },
    {
        # CAMELS-AUS streamflow matrix (ML/day) — the observation source.
        "slug": "camels_aus",
        "name": "CAMELS-AUS — Australia large-sample hydrology, daily streamflow (Zenodo)",
        "auto": True,
        "size": "~287 MB",
        "url": "https://zenodo.org/api/records/13350616/files/03_streamflow.zip/content",
        "checksum": "md5:28113b991387796fe374aa0d1f4d4a4f",
    },
    {
        # CAMELS-AUS attributes master table — a BARE published CSV (no zip);
        # the download layer keeps non-archive files in place.
        "slug": "camels_aus_attributes",
        "name": "CAMELS-AUS — attributes & indices master table incl. outlet coords (Zenodo)",
        "auto": True,
        "size": "~0.8 MB",
        "url": "https://zenodo.org/api/records/13350616/files/CAMELS_AUS_Attributes%26Indices_MasterTable.csv/content",
        "checksum": "md5:aa47ba598d0486d5ea4ccca6e132a7be",
    },
    {
        # CAMELS-SE streamflow — per-catchment daily obs (Qobs_m3s). SND
        # publishes no checksum; md5 self-computed on the fetched bytes.
        "slug": "camels_se",
        "name": "CAMELS-SE — Sweden large-sample hydrology, catchment time series (SND)",
        "auto": True,
        "size": "~15 MB",
        "url": "https://api.researchdata.se/dataset/2023-173/1/file/data?filePath=catchment%20time%20series.zip",
        "checksum": "md5:5e6972cf29c9220e547bc00dddd7b03a",
    },
    {
        # CAMELS-SE GIS — WGS84 gauge point shapefile (station coordinates).
        "slug": "camels_se_gis",
        "name": "CAMELS-SE — catchment GIS shapefiles incl. WGS84 gauge points (SND)",
        "auto": True,
        "size": "~1 MB",
        "url": "https://api.researchdata.se/dataset/2023-173/1/file/data?filePath=catchment_GIS_shapefiles.zip",
        "checksum": "md5:2983f5e255b74e01da656c671519163a",
    },
    {
        # CAMELS-CH — single bundle (observation-based timeseries + attributes).
        "slug": "camels_ch",
        "name": "CAMELS-CH — Switzerland large-sample hydrology (Zenodo)",
        "auto": True,
        "size": "~247 MB",
        "url": "https://zenodo.org/api/records/15025258/files/camels_ch.zip/content",
        "checksum": "md5:04f909d9904375647d030c4ab8ddfdbe",
    },
    {
        "slug": "iceland_lamahice",
        "name": "LamaH-Ice — Iceland large-sample hydrology (HydroShare, daily)",
        "auto": True,
        "size": "~636 MB",
        "url": "https://www.hydroshare.org/resource/86117a5f36cc4b7c90a5d54e18161c91/data/contents/lamah_ice.zip",
        # HydroShare-published content hash for lamah_ice.zip (hsapi files
        # endpoint, resource 86117a5f…, 636283348 bytes). The download is
        # verified against this before extraction (fail-closed on mismatch).
        "checksum": "md5:6246f7300c77ead2c9f097ad5da89ba9",
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


def _extract_archive(archive_path: Path, dest: Path) -> bool:
    """Extract a ZIP or tar.gz archive into ``dest`` safely.

    Detection is by CONTENT (magic bytes), not filename — many repository
    download URLs are extension-less (e.g. Dataverse ``.../datafile/<id>``) or
    serve a bare published file. Returns True if *archive_path* was an archive
    and was extracted; False if it is a bare file (e.g. a ``.csv`` master table)
    that needs no extraction and is left in place as the dataset's data.
    """
    if zipfile.is_zipfile(archive_path):
        with zipfile.ZipFile(archive_path) as zf:
            _safe_extractall(zf, dest)
        return True
    if tarfile.is_tarfile(archive_path):
        with tarfile.open(archive_path) as tf:
            _safe_extract_tar(tf, dest)
        return True
    return False  # bare non-archive download: keep the file as-is


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


def _checksum_for(slug: str) -> str | None:
    """The recorded integrity hash for *slug*, or None.

    Returns the archive ``checksum`` when present, else the ``content_checksum``
    (for sources whose archive bytes are non-reproducible). This is the value
    the dataset-artifact capability declares.
    """
    entry = next((d for d in DATASETS if d["slug"] == slug), None)
    if entry is None:
        return None
    return entry.get("checksum") or entry.get("content_checksum")


def _content_hash(dest: Path, exclude: tuple[str, ...] = ()) -> str:
    """Canonical ``content-sha256:`` hash of the extracted file set under *dest*.

    Hashes a sorted manifest of ``(relative_posix_path, sha256-of-file)`` for
    every file, so the value depends only on file *contents* and layout — not on
    archive compression or timestamps. This is the stable integrity anchor for
    sources whose server regenerates the zip per request (e.g. CEH's CAMELS-GB,
    where every download yields a different archive md5).

    ``exclude`` is a tuple of ``fnmatch`` globs (matched against the relative
    posix path) for files whose contents legitimately vary per download — e.g.
    a ``readme.html`` carrying a generation timestamp. Excluding them keeps the
    hash stable over the actual data without weakening it for the data files.
    """
    import fnmatch

    entries: list[str] = []
    for p in sorted(dest.rglob("*")):
        if not p.is_file():
            continue
        rel = p.relative_to(dest).as_posix()
        if any(fnmatch.fnmatch(rel, pat) for pat in exclude):
            continue
        h = hashlib.sha256()
        with p.open("rb") as fh:
            for block in iter(lambda: fh.read(1 << 20), b""):
                h.update(block)
        entries.append(f"{rel}\0{h.hexdigest()}")
    manifest = "\n".join(entries).encode("utf-8")
    return "content-sha256:" + hashlib.sha256(manifest).hexdigest()


def _verify_archive_checksum(archive_path: Path, expected: str) -> None:
    """Verify *archive_path* against ``algo:hexdigest``; raise on mismatch.

    Provenance gate enforcement (contract 0.5.0, dataset-artifact tier): a
    published artifact is only authentic if its bytes match the hash recorded
    from the source's record. Fail-closed — a mismatch means tampering, a
    truncated download, or a moved/re-published archive, none of which should
    silently become a calibration input.
    """
    algo, _, want = expected.partition(":")
    if not want:  # tolerate a bare hexdigest; default to sha256
        algo, want = "sha256", expected
    try:
        hasher = hashlib.new(algo)
    except ValueError as exc:
        raise ValueError(f"unsupported checksum algorithm {algo!r} for {archive_path.name}") from exc
    with archive_path.open("rb") as fh:
        for block in iter(lambda: fh.read(1 << 20), b""):
            hasher.update(block)
    got = hasher.hexdigest()
    if got.lower() != want.lower():
        raise ValueError(
            f"checksum mismatch for {archive_path.name}: expected {algo}:{want}, got {algo}:{got}"
        )


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

        # Provenance gate: verify the published ARCHIVE hash before extraction
        # (fail-closed). Datasets whose server regenerates the zip per request
        # (e.g. CEH) use a content-checksum instead — verified after extraction.
        entry = next((d for d in DATASETS if d["slug"] == slug), {})
        archive_ck = entry.get("checksum")
        content_ck = entry.get("content_checksum")
        if archive_ck:
            _verify_archive_checksum(archive_path, archive_ck)
            logger.info("checksum_verified", slug=slug, checksum=archive_ck)
        elif not content_ck:
            logger.warning("checksum_unverified", slug=slug, reason="no recorded checksum")

        extracted = _extract_archive(archive_path, dest)
        # Drop the archive once extracted — saves significant disk (Caravan is
        # ~12.5 GB) and stops a leftover archive from masking a failed extract.
        # A bare non-archive download (e.g. a published .csv) is the data itself
        # and must be kept in place.
        if extracted:
            archive_path.unlink(missing_ok=True)

        # Content-checksum gate: stable across archive re-zips because it hashes
        # the extracted file set, not the archive wrapper. Must run AFTER the
        # archive is removed so it does not hash the spent archive.
        if content_ck:
            got = _content_hash(dest, tuple(entry.get("content_exclude", ())))
            if got != content_ck:
                raise ValueError(
                    f"content checksum mismatch for {slug}: expected {content_ck}, got {got}"
                )
            logger.info("content_checksum_verified", slug=slug, checksum=content_ck)

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
