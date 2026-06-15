# Data Clearance — CSFS (Community Streamflow Service)

Per-provider licensing clearance for commercial use and redistribution. **This documents the terms of third-party data sources; it does not grant any rights to that data.** The CSFS *code* is licensed separately (see `LICENSE`); using CSFS to acquire data does not transfer any rights in the data itself. You are responsible for complying with each source's terms. Machine-readable detail: [`inventory/clearance.csv`](inventory/clearance.csv).

## Two-axis model

- **commercial_use** — may an *end user* use the data for commercial purposes?
- **redistribution** — may a *third party re-host/re-serve* it? (`conditional` = yes with attribution/share-alike)

## Tiers

| Tier | Meaning | Self-hosted client (commercial) | Hosted SaaS (redistribution) |
|---|---|---|---|
| A | Public domain / open | ✅ | ✅ |
| B | Attribution required | ✅ (attribute) | ✅ (attribute) |
| B-SA | Attribution + share-alike | ✅ | ⚠️ derived data inherits copyleft |
| C | Non-commercial / research-only | 🔴 gate out | 🔴 gate out |
| D | No redistribution / gated | user-BYO only | 🔴 never serve |
| E | Unknown — unverified | ⚠️ treat as restricted | 🔴 until cleared |

## CSFS summary (84 providers)

| A | B | B-SA | C | D | E |
|--|--|--|--|--|--|
| 6 | 48 | 3 | 3 | 14 | 10 |

**Commercial-clearable (A/B): 54/84.** Gate from commercial use: 3 C + 10 E. Never host: 14 D.

## Restricted / unverified providers (do not auto-clear)

| Tier | Provider | License | Why |
|---|---|---|---|
| C | IMGW Public Data | IMGW-custom | non-commercial |
| C | LamaH-Ice | CC-BY-NC-4.0 | non-commercial |
| C | SIEREM (IRD African Hydrology) | CC-BY-NC-4.0 | non-commercial |
| D | CWC India (WRIS) | all-rights-reserved | no redistribution / permission-gated |
| D | DHMZ (Croatia) | permission-gated | no redistribution / permission-gated |
| D | Global Runoff Data Centre | GRDC-policy | no redistribution / permission-gated |
| D | HII (Hydro-Informatics Institute Thailand) | permission-gated | no redistribution / permission-gated |
| D | INE Bolivia (Caudales y Niveles) | ANDA-restricted | no redistribution / permission-gated |
| D | India WRIS / CWC | all-rights-reserved | no redistribution / permission-gated |
| D | LUBW Baden-Württemberg | permission-required | no redistribution / permission-gated |
| D | MLIT Water Information System | permission-gated | no redistribution / permission-gated |
| D | SHMU (Slovakia) | permission-required | no redistribution / permission-gated |
| D | SPW Wallonia | permission-required | no redistribution / permission-gated |
| D | UK National River Flow Archive | NRFA-API-ToS | no redistribution / permission-gated |
| D | WMO WHOS (Hydrological Observing System) | WMO-terms | no redistribution / permission-gated |
| D | WMO WHOS-Africa (HydroSOS) | WMO-terms | no redistribution / permission-gated |
| D | WMO WHOS-Plata | WMO-terms | no redistribution / permission-gated |
| E | CONAGUA BANDAS | none-found | no verifiable terms — contact source |
| E | DGA Chile (SNIA) | none-found | no verifiable terms — contact source |
| E | DSI Turkey (FACE Portal) | none-found | no verifiable terms — contact source |
| E | FFWC Bangladesh (BWDB) | none-found | no verifiable terms — contact source |
| E | FHMZ Bosnia | none-found | no verifiable terms — contact source |
| E | NIMH Bulgaria (open data) | none-found | no verifiable terms — contact source |
| E | New Zealand Regional Councils (Hilltop) | mixed-per-council | no verifiable terms — contact source |
| E | Pakistan IRSA/WAPDA | none-found | no verifiable terms — contact source |
| E | STRI Panama Canal Watershed (ACP) | STRI-DUA | no verifiable terms — contact source |
| E | WAMIS (Water Management Information System) | none-found | no verifiable terms — contact source |

## Method

Each provider's free-text licence was normalized to the two-axis schema and tier; values marked `agent-verified` in `clearance.csv` were confirmed against the official licence page (see the `source` column). Tiers are derived deterministically; re-running the classifier preserves verified rows. `E` rows have no publishable terms and require a direct request to the source agency or a legal determination.
