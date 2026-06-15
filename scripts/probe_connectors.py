# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Live connector health sweep.

Probes each *live* connector against its real upstream endpoint:
fetch_stations() -> try several stations -> fetch_observations() over a
recent window -> count observations carrying non-null discharge. This covers
both queryable APIs (rest/ogc/kiwis/...) and live scraping/portal connectors
(web_portal/web_scraping/html_scrape/csv_download/...).

Offline-archive connectors (bulk_download/zenodo/dataverse/... -- GRDC,
Caravan, LamaH, CAMELS, etc.) need a downloaded dataset to return anything, so
live-probing them is meaningless (and would trigger multi-GB downloads); they
are reported as SKIPPED_OFFLINE rather than probed. Pass --include-offline to
probe them anyway.

Multi-station by design: a connector is judged OK if *any* probed station
returns discharge (never trust stations[0] alone). Empty results are retried
over a wide window (--wide-days) so lagged archives like NRFA/ANA -- which
publish months-to-years behind -- are classified OK_LAGGED, not EMPTY.
Emits a JSON report.

Run:  .venv/bin/python scripts/probe_connectors.py [--days 30] [--wide-days 1095] [--stations 8]
"""

from __future__ import annotations

import argparse
import asyncio
import json
from datetime import UTC, datetime, timedelta
from pathlib import Path

import yaml

from csfs.core.registry import discover, get_connector, list_providers

# api_type values that represent a live, queryable observation API.
LIVE_API_TYPES = {
    "rest", "ogc_features", "kiwis_rest", "hilltop_rest", "wfs",
    "structured_url", "arcgis_rest", "soap_wof", "waterml2",
    "aquarius_rest", "socrata_soda", "soap",
}

# api_type values that scrape/parse a live portal, page, CSV, or keyed service.
# These still fetch *current* data, so they are probed like the APIs above.
SCRAPE_PORTAL_TYPES = {
    "web_portal", "web_scraping", "html_scrape", "csv_download",
    "open_data", "pdf_yearbooks", "cds_api",
}

# api_type values that need a downloaded dataset (research archives / bulk
# dumps). Live-probing them is meaningless and can trigger multi-GB downloads,
# so they are reported as SKIPPED_OFFLINE unless --include-offline is set.
OFFLINE_TYPES = {
    "bulk_download", "zenodo", "dataverse", "nada_catalog", "eidc_catalogue",
}

# Connectors whose api_type is bulk_download but which actually fetch live data
# (the api_type label is misleading) -- probe these despite the OFFLINE_TYPES
# rule.
LIVE_OVERRIDE = {"finland_syke", "belgium_spw"}

PER_CONNECTOR_TIMEOUT = 90.0  # seconds, hard cap per connector


def _auto_download_slugs() -> set[str]:
    """Slugs that auto-download a (potentially multi-GB) dataset on fetch.

    These are NEVER probed (even with --include-offline) to avoid triggering
    huge downloads; e.g. the Caravan archive is ~12.5 GB.
    """
    try:
        from csfs.core.downloads import DATASETS
    except Exception:
        return set()
    items = DATASETS.values() if isinstance(DATASETS, dict) else DATASETS
    out = set()
    for v in items:
        if isinstance(v, dict) and v.get("auto") and v.get("slug"):
            out.add(v["slug"])
    if isinstance(DATASETS, dict):
        out |= {k for k, v in DATASETS.items()
                if isinstance(v, dict) and v.get("auto")}
    return out


def classify_slugs(include_offline: bool = False) -> tuple[dict[str, dict], list[str]]:
    """Return ({slug: meta} to probe, [offline slugs skipped]).

    Probes registered connectors whose api_type is a live API or live
    scrape/portal type. Offline-archive types are skipped and returned
    separately so the report accounts for them honestly. Auto-downloading
    datasets are always skipped regardless of --include-offline.
    """
    data = yaml.safe_load(Path("inventory/providers.yaml").read_text())
    registered = set(list_providers())
    auto = _auto_download_slugs()
    targets: dict[str, dict] = {}
    offline: list[str] = []
    for e in data:
        slug = e.get("slug")
        if slug not in registered:
            continue
        api_type = e.get("api_type")
        is_offline = api_type in OFFLINE_TYPES and slug not in LIVE_OVERRIDE
        if slug in auto:
            offline.append(slug)  # never probe -- avoids multi-GB downloads
        elif is_offline and not include_offline:
            offline.append(slug)
        else:
            targets[slug] = e
    return targets, sorted(offline)


async def _sample_window(conn, stations, start, end, n_stations) -> tuple[list, int, int]:
    """Probe up to n_stations over [start, end). Returns (probed, obs, discharge).

    Prefer active stations -- large connectors carry many discontinued gauges
    whose recent window is legitimately empty. Spread the sample across the
    list (not just the head), and stop early once discharge is confirmed: one
    producing station proves the connector is live.
    """
    candidates = [s for s in stations if s.is_active] or stations
    step = max(1, len(candidates) // n_stations)
    probed, obs_total, discharge_hits = [], 0, 0
    for st in candidates[::step][:n_stations]:
        try:
            chunk = await conn.fetch_observations(st.native_id, start, end)
            n_obs = len(chunk.observations)
            n_disc = sum(1 for o in chunk.observations if o.discharge_m3s is not None)
            obs_total += n_obs
            discharge_hits += n_disc
            probed.append({"station": st.native_id, "obs": n_obs, "discharge": n_disc})
            if discharge_hits > 0:
                break
        except Exception as exc:  # noqa: BLE001
            probed.append({"station": st.native_id, "error": f"{type(exc).__name__}: {str(exc)[:100]}"})
    return probed, obs_total, discharge_hits


async def probe_one(slug: str, meta: dict, days: int, n_stations: int, wide_days: int = 1095) -> dict:
    end = datetime.now(UTC)
    result = {"slug": slug, "name": meta.get("name", slug), "country": meta.get("country")}
    cls = get_connector(slug)
    try:
        async with cls() as conn:
            stations = await conn.fetch_stations()
            result["n_stations"] = len(stations)
            if not stations:
                result["status"] = "NO_STATIONS"
                return result
            probed, obs_total, discharge_hits = await _sample_window(
                conn, stations, end - timedelta(days=days), end, n_stations,
            )
            # Lagged archives (NRFA, ANA) publish months-to-years behind, so a
            # recent window false-negatives them. If the primary window is dry,
            # retry once over a wide window before calling it empty. (Only when
            # NO rows came back -- a connector that returns rows but no
            # discharge is handled as OBS_NO_DISCHARGE; a blanket wide retry
            # there would time out chunked month-by-month scrapers like MLIT.)
            if discharge_hits == 0 and obs_total == 0 and wide_days > days:
                wide_probed, wide_obs, wide_disc = await _sample_window(
                    conn, stations, end - timedelta(days=wide_days), end, n_stations,
                )
                if wide_disc > 0 or wide_obs > 0:
                    result["wide_window_days"] = wide_days
                    probed, obs_total, discharge_hits = wide_probed, wide_obs, wide_disc
            result["probed"] = probed
            result["obs_total"] = obs_total
            result["discharge_total"] = discharge_hits
            if discharge_hits > 0:
                result["status"] = "OK_LAGGED" if result.get("wide_window_days") else "OK"
            elif obs_total > 0:
                result["status"] = "OBS_NO_DISCHARGE"
            elif any("error" in p for p in probed):
                result["status"] = "FETCH_ERROR"
            else:
                result["status"] = "EMPTY"
    except TimeoutError:
        result["status"] = "TIMEOUT"
    except Exception as exc:  # noqa: BLE001
        result["status"] = "STATIONS_ERROR"
        result["error"] = f"{type(exc).__name__}: {str(exc)[:200]}"
    return result


async def probe_guarded(slug, meta, days, n_stations, wide_days=1095):
    try:
        return await asyncio.wait_for(
            probe_one(slug, meta, days, n_stations, wide_days), PER_CONNECTOR_TIMEOUT,
        )
    except TimeoutError:
        return {"slug": slug, "name": meta.get("name", slug), "status": "TIMEOUT"}


async def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--days", type=int, default=30,
                    help="primary recent window in days")
    ap.add_argument("--wide-days", type=int, default=1095,
                    help="fallback window for empty results (catches lagged archives)")
    ap.add_argument("--stations", type=int, default=8)
    ap.add_argument("--out", default="connector_health.json")
    ap.add_argument("--concurrency", type=int, default=8)
    ap.add_argument("--include-offline", action="store_true",
                    help="also probe offline-archive connectors (may download datasets)")
    args = ap.parse_args()

    discover()
    targets, offline = classify_slugs(include_offline=args.include_offline)
    n_api = sum(1 for m in targets.values() if m.get("api_type") in LIVE_API_TYPES)
    n_scrape = sum(1 for m in targets.values() if m.get("api_type") in SCRAPE_PORTAL_TYPES)
    print(f"Probing {len(targets)} live connectors "
          f"({n_api} api, {n_scrape} scrape/portal, {len(targets) - n_api - n_scrape} other) "
          f"window={args.days}d, up to {args.stations} stations each; "
          f"skipping {len(offline)} offline archives.\n")

    sem = asyncio.Semaphore(args.concurrency)

    async def run(slug, meta):
        async with sem:
            r = await probe_guarded(slug, meta, args.days, args.stations, args.wide_days)
            r["api_type"] = meta.get("api_type")
            print(f"  {r['status']:18s} {slug}")
            return r

    results = await asyncio.gather(*(run(s, m) for s, m in sorted(targets.items())))

    for slug in offline:
        results.append({"slug": slug, "status": "SKIPPED_OFFLINE"})

    Path(args.out).write_text(json.dumps(results, indent=2, default=str))
    by_status: dict[str, list[str]] = {}
    for r in results:
        by_status.setdefault(r["status"], []).append(r["slug"])
    print("\n=== SUMMARY ===")
    for status in sorted(by_status):
        print(f"{status:18s} {len(by_status[status]):3d}  {', '.join(sorted(by_status[status]))}")
    print(f"\nFull report: {args.out}")


if __name__ == "__main__":
    asyncio.run(main())
