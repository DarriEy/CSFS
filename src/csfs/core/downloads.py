# SPDX-License-Identifier: GPL-3.0-or-later
"""Download freely-available datasets for local-file connectors."""

from __future__ import annotations

import zipfile
from pathlib import Path

import httpx
import structlog

logger = structlog.get_logger()

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
        "slug": "grdc",
        "name": "Global Runoff Data Centre",
        "auto": False,
        "size": "varies",
        "url": "https://grdc.bafg.de/data/data_portal/",
    },
    {
        "slug": "spain_miteco",
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


def _safe_extractall(zf: zipfile.ZipFile, dest: Path) -> None:
    """Extract all members, rejecting any whose path escapes ``dest`` (zip-slip)."""
    dest = dest.resolve()
    for member in zf.namelist():
        target = (dest / member).resolve()
        if target != dest and dest not in target.parents:
            raise ValueError(f"Unsafe path in archive escapes destination: {member!r}")
    zf.extractall(dest)


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
    if any(dest.iterdir()) if dest.is_dir() else False:
        logger.info("dataset_already_exists", slug=slug, path=str(dest))
        return True

    dest.mkdir(parents=True, exist_ok=True)

    return await _download_and_extract_zip(slug, dest, entry["url"])


async def _download_and_extract_zip(slug: str, dest: Path, url: str) -> bool:
    """Download a ZIP (streaming) and extract it into dest."""
    zip_name = url.rsplit("/", 1)[-1].split("?")[0] or f"{slug}.zip"
    zip_path = dest / zip_name
    try:
        async with httpx.AsyncClient(
            follow_redirects=True,
            timeout=httpx.Timeout(7200.0, connect=60.0),
        ) as client:
            logger.info("downloading", slug=slug, url=url)
            async with client.stream("GET", url) as resp:
                resp.raise_for_status()
                total = 0
                with open(zip_path, "wb") as f:
                    async for chunk in resp.aiter_bytes(chunk_size=1024 * 1024):
                        f.write(chunk)
                        total += len(chunk)
                logger.info("download_complete", slug=slug, size_mb=round(total / 1e6, 1))

        with zipfile.ZipFile(zip_path) as zf:
            _safe_extractall(zf, dest)
        logger.info("dataset_extracted", slug=slug, files=len(list(dest.iterdir())))
        return True
    except Exception as exc:
        logger.error("download_failed", slug=slug, error=str(exc))
        return False
