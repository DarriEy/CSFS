# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Regenerate docs/catalog.md and the README roster numbers from source.

The single source of truth is ``inventory/providers.yaml`` plus the live
connector registry. Run from the repo root::

    python scripts/gen_catalog.py

``tests/test_connector_integrity.py`` regenerates the same content in memory
and fails CI when the committed files have drifted, so the published numbers
can never silently disagree with the code again.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path

import yaml

REPO_ROOT = Path(__file__).resolve().parent.parent
INVENTORY = REPO_ROOT / "inventory" / "providers.yaml"
CATALOG = REPO_ROOT / "docs" / "catalog.md"
README = REPO_ROOT / "README.md"

STATS_START = "<!-- stats:start -->"
STATS_END = "<!-- stats:end -->"

_STATUS_MEANING = {
    "implemented": "Registered connector exists in `csfs/connectors/`, with tests",
    "research": "API exists but needs investigation",
    "fallback": "Community/research dataset used for gap-filling",
    "manual": "No API; requires scraping or manual download",
    "degraded": "Connector exists but the upstream source is impaired",
    "deprecated": "Source retired or superseded",
}

_NOTE_WIDTH = 140

_CATALOG_HEADER = """\
# Provider Catalog

The full provider inventory lives in
[`inventory/providers.yaml`](https://github.com/DarriEy/CSFS/blob/main/inventory/providers.yaml).
This page is generated from it by `scripts/gen_catalog.py`. **Statuses are
honest by construction**: the CI-enforced roster-integrity tests (see
[Architecture](architecture.md#roster-integrity-guards)) forbid an entry
from claiming `implemented` unless a registered connector actually exists,
every registered connector must have test coverage, a scheduler tier, and
an inventory entry, and this page itself is regenerated in CI and compared
against the committed copy.
"""

_CATALOG_WOBBLE = """\
!!! note "Live providers wobble"
    A connector being `implemented` means the code path is real and tested
    against recorded responses — not that the upstream agency API is up at
    any given moment. Transient upstream outages are expected and surface
    in `csfs health`.
"""


def load_inventory() -> list[dict]:
    entries = yaml.safe_load(INVENTORY.read_text(encoding="utf-8"))
    if not isinstance(entries, list):
        raise SystemExit("providers.yaml did not parse to a list of entries")
    return entries


def registered_slugs() -> list[str]:
    sys.path.insert(0, str(REPO_ROOT / "src"))
    from csfs.core.registry import discover, list_providers

    discover()
    return list_providers()


def _realtime_cell(entry: dict) -> str:
    value = entry.get("realtime")
    if value is True:
        return "yes"
    if value is False:
        return "no"
    return "—"


def _note_cell(entry: dict) -> str:
    note = str(entry.get("notes", "") or "").strip().replace("\n", " ")
    if len(note) > _NOTE_WIDTH:
        note = note[: _NOTE_WIDTH].rstrip() + "..."
    return note


def render_catalog(entries: list[dict], registered: list[str]) -> str:
    counts: dict[str, int] = {}
    for entry in entries:
        counts[entry["status"]] = counts.get(entry["status"], 0) + 1
    implemented = counts.get("implemented", 0)
    realtime_impl = sum(
        1 for e in entries if e["status"] == "implemented" and e.get("realtime") is True
    )

    lines = [_CATALOG_HEADER]
    lines.append("## Status breakdown\n")
    lines.append(f"Of the **{len(entries)} cataloged sources**:\n")
    lines.append("| Status | Count | Meaning |")
    lines.append("| --- | ---: | --- |")
    for status, meaning in _STATUS_MEANING.items():
        if counts.get(status):
            lines.append(f"| `{status}` | {counts[status]} | {meaning} |")
    lines.append("")
    lines.append(
        f"In code, **{len(registered)} connectors are registered**. "
        f"**{realtime_impl} of the {implemented} implemented providers deliver realtime "
        "or near-realtime data**; the remainder are recent/archive sources, including "
        "roughly a dozen offline research archives (GRDC, Caravan, GSIM, EStreams, "
        "LamaH, CAMELS variants, ROBIN, ADHI, SIEREM)."
    )
    lines.append("")
    lines.append(_CATALOG_WOBBLE)
    lines.append("## All cataloged providers\n")
    lines.append("| Provider | Country | Status | Realtime | Notes |")
    lines.append("| --- | --- | --- | --- | --- |")
    for entry in sorted(
        entries, key=lambda e: (str(e.get("country", "zz")), str(e.get("name", "")))
    ):
        lines.append(
            f"| {entry['name']} (`{entry['slug']}`) | {entry.get('country', '—')} "
            f"| `{entry['status']}` | {_realtime_cell(entry)} | {_note_cell(entry)} |"
        )
    lines.append("")
    return "\n".join(lines)


def render_readme_stats(entries: list[dict], registered: list[str]) -> str:
    counts: dict[str, int] = {}
    for entry in entries:
        counts[entry["status"]] = counts.get(entry["status"], 0) + 1
    implemented = counts.get("implemented", 0)
    realtime_impl = sum(
        1 for e in entries if e["status"] == "implemented" and e.get("realtime") is True
    )
    others = ", ".join(
        f"{counts[s]} {s}"
        for s in ("research", "fallback", "manual", "degraded", "deprecated")
        if counts.get(s)
    )
    non_implemented_registered = len(registered) - implemented
    registered_gloss = (
        f" — the {implemented} `implemented` entries plus "
        f"{non_implemented_registered} still labeled `research`/`degraded` while their "
        "upstream data paths are validated"
        if non_implemented_registered > 0
        else " — exactly the `implemented` entries"
    )
    return (
        f"- **{len(entries)} sources cataloged** in "
        "[`inventory/providers.yaml`](inventory/providers.yaml),\n"
        f"  labeled by readiness: **{implemented} implemented**, {others}.\n"
        f"- **{len(registered)} connectors registered in code**{registered_gloss}.\n"
        f"- **{realtime_impl} implemented providers are realtime/near-realtime**; the rest are\n"
        "  recent/archive sources, including roughly a dozen offline research archives\n"
        "  (GRDC, Caravan, GSIM, EStreams, LamaH, CAMELS variants, ROBIN, ADHI, SIEREM)."
    )


def updated_readme(readme_text: str, stats_block: str) -> str:
    block = f"{STATS_START}\n{stats_block}\n{STATS_END}"
    if STATS_START in readme_text:
        return re.sub(
            re.escape(STATS_START) + r".*?" + re.escape(STATS_END),
            block.replace("\\", r"\\"),
            readme_text,
            flags=re.DOTALL,
        )
    raise SystemExit(
        f"README.md is missing the {STATS_START} / {STATS_END} markers around "
        "the roster numbers"
    )


def main() -> None:
    entries = load_inventory()
    registered = registered_slugs()
    CATALOG.write_text(render_catalog(entries, registered), encoding="utf-8")
    README.write_text(
        updated_readme(README.read_text(encoding="utf-8"), render_readme_stats(entries, registered)),
        encoding="utf-8",
    )
    print(f"catalog: {len(entries)} entries; registry: {len(registered)} connectors")
    print(f"wrote {CATALOG.relative_to(REPO_ROOT)} and README stats block")


if __name__ == "__main__":
    main()
