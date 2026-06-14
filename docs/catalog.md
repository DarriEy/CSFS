# Provider Catalog

The full provider inventory lives in
[`inventory/providers.yaml`](https://github.com/DarriEy/CSFS/blob/main/inventory/providers.yaml).
This page is generated from it. **Statuses are honest by construction**:
the CI-enforced roster-integrity tests (see
[Architecture](architecture.md#roster-integrity-guards)) forbid an entry
from claiming `implemented` unless a registered connector actually exists,
and every registered connector must have test coverage, a scheduler tier,
and an inventory entry.

## Status breakdown

Of the **104 cataloged sources**:

| Status | Count | Meaning |
| --- | ---: | --- |
| `implemented` | 78 | Registered connector exists in `csfs/connectors/`, with tests |
| `research` | 17 | API exists but needs investigation (8 of these already have a connector under validation) |
| `fallback` | 5 | Community/research dataset used for gap-filling |
| `manual` | 3 | No API; requires scraping or manual download |
| `deprecated` | 1 | Source retired or superseded |

In code, **86 connectors are registered**: the 78 `implemented` entries plus 8 whose inventory entries remain `research` while their upstream data paths are validated. **41 of the implemented providers deliver realtime or near-realtime data**; the remainder are recent/archive sources, including roughly a dozen offline research archives (GRDC, Caravan, GSIM, EStreams, LamaH, CAMELS variants, ROBIN, ADHI, SIEREM).

!!! note "Live providers wobble"
    A connector being `implemented` means the code path is real and tested
    against recorded responses — not that the upstream agency API is up at
    any given moment. Transient upstream outages are expected and surface
    in `csfs health`.

## All cataloged providers

| Provider | Country | Status | Realtime | Notes |
| --- | --- | --- | --- | --- |
| Afghanistan (USGS NWIS) (`afghanistan_usgs`) | AF | `implemented` | no | Historical digitized records. Discharge param 00060 (cfs), convert to m3/s. |
| SNIH Argentina (`argentina_snih`) | AR | `implemented` | — |  |
| WMO WHOS-Plata (`wmo_whos_plata`) | AR, BO, BR, PY, UY | `implemented` | yes | La Plata River Basin federated access. |
| eHYD Austria (BMLUK) (`austria_ehyd`) | AT | `implemented` | yes | Official WFS service from the Federal Ministry (BMLUK). |
| LamaH-CE (Central Europe) (`lamah_ce`) | AT,DE,CZ | `implemented` | no | Danube basin focus. Hourly resolution available. |
| Bureau of Meteorology Water Data Online (`australia_bom`) | AU | `implemented` | yes |  |
| FHMZ Bosnia (`bosnia_fhmz`) | BA | `implemented` | no | PDF hydrological yearbooks; requires tabula-style extraction. |
| SPW Wallonia (`belgium_spw`) | BE | `implemented` | yes | No-redistribution license. |
| Waterinfo Flanders (`belgium_waterinfo`) | BE | `implemented` | yes |  |
| SIEREM (IRD African Hydrology) (`sierem`) | BF, BJ, CF, CG, CI, CM, GA, GN, ML, MR, NE, SN, TD, TG | `implemented` | no | IRD database focused on West and Central Africa. |
| EAEMDR Bulgaria (`bulgaria_eaemdr`) | BG | `implemented` | yes | Scrapes the daily Danube hydrology bulletin (/hidrology-en) for current discharge (m3/s) at 6 gauges (Novo Selo, Lom, Oryahovo, Svishtov,... |
| NIMH Bulgaria (open data) (`bulgaria_nimh`) | BG | `implemented` | no | Daily water runoff data. |
| DanubeHIS (`danube_his`) | BG, RS, UA, HU, RO, SK, AT, DE, CZ, HR, SI, BA, MD | `implemented` | yes | Covers the entire Danube River basin. |
| INE Bolivia (Caudales y Niveles) (`bolivia_ine`) | BO | `implemented` | no | NADA catalog dataset |
| ANA HidroWeb / Telemetria (`brazil_ana`) | BR | `implemented` | yes |  |
| Environment Canada Hydrometric Data (`environment_canada`) | CA | `implemented` | yes |  |
| BAFU Hydrodaten (`switzerland_bafu`) | CH | `implemented` | yes |  |
| DGA Chile (SNIA) (`chile_dga`) | CL | `implemented` | yes |  |
| Ministry of Water Resources (`china_mwr`) | CN | `implemented` | — | Limited public access; flood data intermittently available. |
| CAMELS-COL (`camels_co`) | CO | `implemented` | no |  |
| Czech Hydrometeorological Institute (`czechia_chmu`) | CZ | `implemented` | no |  |
| CAMELS-DE (`camels_de`) | DE | `implemented` | no |  |
| GKD Bayern (`germany_bavaria`) | DE | `implemented` | yes | Discharge (m3/s) via HTML table scraping of the GKD portal; the CSV path is email/ToS-gated. ~610 stations; lat/lon not exposed by these... |
| LUBW Baden-Württemberg (`germany_bw`) | DE | `implemented` | yes | Discharge (m3/s) parsed from the HVZ JS catalogue (hvz_peg_stmn.js). LATEST-VALUE ONLY - no historical series; ~260 discharge stations. L... |
| PEGELONLINE (BfG) (`germany_pegelonline`) | DE | `implemented` | yes | Primarily water level; discharge at federal waterways only. |
| CAMELS-DK (`camels_dk`) | DK | `implemented` | no | Offline archive (CAMELS-DK, Zenodo). Returns a small seed catalogue only; observations require local downloaded dataset files (config['da... |
| VanDa Hydro (Denmark) (`denmark_dmihyd`) | DK | `implemented` | yes | Near-real-time river data via Danmarks Miljøportal (IoT). |
| ADHI (African Database of Hydrometric Indices) (`adhi`) | DZ, AO, BJ, BW, BF, BI, CM, CF, TD, CG, CD, CI, DJ, EG, GQ, ER, SZ, ET, GA, GM, GH, GN, GW, KE, LS, LR, LY, MG, MW, ML, MR, MZ, NA, NE, NG, RW, SN, SL, SO, ZA, SD, TZ, TG, TN, UG, ZM, ZW | `implemented` | no | Pan-African monthly discharge series and hydrometric statistics. |
| WMO WHOS-Africa (HydroSOS) (`wmo_whos_africa`) | DZ, AO, BJ, BW, BF, BI, CM, CF, TD, CG, CD, CI, DJ, EG, GQ, ER, SZ, ET, GA, GM, GH, GN, GW, KE, LS, LR, LY, MG, MW, ML, MR, MZ, NA, NE, NG, RW, SN, SL, SO, ZA, SD, TZ, TG, TN, UG, ZM, ZW | `implemented` | yes | Federated access for Africa RA1 (including Rwanda/Ethiopia). |
| Ecuador INAMHI (GEOGLOWS) (`ecuador_inamhi`) | EC | `implemented` | yes | INAMHI Ecuador streamflow via the GEOGLOWS ECMWF model (reach-based, m3/s). Shares the GEOGLOWS backend with the global geoglows connecto... |
| CEDEX Anuario de Aforos (`spain_cedex`) | ES | `implemented` | no | Offline archive connector. Returns the seed station catalogue, but yields observations only when config['data_dir'] points at downloaded... |
| SYKE (Finnish Environment Institute) (`finland_syke`) | FI | `implemented` | no |  |
| Hub'Eau Hydrométrie (`france_hubeau`) | FR | `implemented` | yes | Returns discharge in L/s — divide by 1000 for m3/s. |
| SEPA (Scotland) (`scotland_sepa`) | GB | `implemented` | yes | Uses KISTERS KiWIS service. |
| UK Environment Agency Hydrology API (`uk_ea`) | GB | `implemented` | yes | Open Government Licence. Covers England only. |
| UK National River Flow Archive (`uk_nrfa`) | GB | `implemented` | no | Historical daily only; complements uk_ea for long records. |
| OpenHI Greece (`greece_openhi`) | GR | `implemented` | — |  |
| DHMZ (Croatia) (`croatia_dhz`) | HR | `implemented` | yes | Real-time data via backend hisbaza.py API. |
| EPA Ireland HydroNet (`ireland_epa`) | IE | `implemented` | yes |  |
| Caravan-Israel Extension (Zenodo) (`israel_caravan`) | IL | `implemented` | no | Zenodo record 15003600. |
| CAMELS-IND (`camels_in`) | IN | `implemented` | no |  |
| IWRMC Iran (stu.wrm.ir) (`iran_iwrmc`) | IR | `implemented` | — |  |
| LamaH-Ice (`iceland_lamahice`) | IS | `implemented` | no |  |
| ARPAE Emilia-Romagna (`italy_emilia`) | IT | `implemented` | yes | Discharge (m3/s) from the ARPAE open-data instantaneous-flow feed (dati-simc.arpae.it). Only ~7 Po-river discharge gauges are public; rol... |
| WRA (Jamaica) (`jamaica_wra`) | JM | `implemented` | no |  |
| MLIT Water Information System (`japan_mlit`) | JP | `implemented` | yes |  |
| CA-discharge (Central Asian Discharge Dataset) (`ca_discharge`) | KG,TJ,KZ,UZ,AF | `implemented` | no | Academic dataset covering mountainous Central Asia. |
| Kazhydromet (Kazakhstan) (`kazakhstan_kazhydromet`) | KZ | `implemented` | yes |  |
| LHMT (Lithuania) (`lithuania_lhmt`) | LT | `implemented` | yes | Hydrology API launched Nov 2023. |
| EStreams (European Streamflow Dataset) (`estreams`) | LU,AL,ME,MK | `implemented` | no | Catalogue connector for countries without national APIs. |
| DID Malaysia (Public Infobanjir) (`malaysia_did`) | MY | `implemented` | yes |  |
| Rijkswaterstaat (`netherlands_rws`) | NL | `implemented` | yes |  |
| Norwegian Water Resources (NVE) (`norway_nve`) | NO | `implemented` | yes |  |
| ICIMOD RDS Nepal (`nepal_icimod`) | NP | `implemented` | no |  |
| New Zealand Regional Councils (Hilltop) (`newzealand_hilltop`) | NZ | `implemented` | yes | Distributed across regional councils, each running Hilltop servers. |
| STRI Panama Canal Watershed (ACP) (`panama_stri`) | PA | `implemented` | yes |  |
| DPWH Philippines (Bureau of Design) (`philippines_dpwh`) | PH | `implemented` | yes |  |
| Pakistan IRSA/WAPDA (`pakistan_wapda`) | PK | `implemented` | yes |  |
| IMGW Public Data (`poland_imgw`) | PL | `implemented` | no | Hydrological year (Nov start). All gauges in monthly zip files. |
| R-ArcticNET v4.0 (Russian Arctic) (`russia_arcticnet`) | RU | `implemented` | no | Monthly mean discharge for Russian Arctic stations. |
| SMHI Open Data — Hydrology (`sweden_smhi`) | SE | `implemented` | yes | Two discharge products — parameter 1 "Vattenföring (Dygn)" (daily mean, connector default) and parameter 2 "Vattenföring (15 min)" via config resolution="15min". Both serve epoch-ms UTC timestamps and m³/s. |
| ARSO (Slovenia) (`slovenia_arso`) | SI | `implemented` | yes | Real-time XML feed of latest observations. |
| MARN / SNET (El Salvador) (`elsalvador_marn`) | SV | `implemented` | yes | Uses AQUARIUS Time-Series platform. |
| HII (Hydro-Informatics Institute Thailand) (`thailand_hii`) | TH | `implemented` | yes |  |
| WRA (Taiwan Water Resources Agency) (`taiwan_wra`) | TW | `implemented` | yes | Bilingual API (English + Chinese field names). |
| CAMELSH (Hourly US) (`camelsh`) | US | `implemented` | no | Hourly CAMELSH (Zenodo, 1980-2024). Offline archive: seed catalogue only; observations require local downloaded files (config['data_dir']... |
| USGS National Water Information System (NWIS) (`usgs`) | US | `implemented` | yes | Gold standard. Discharge param 00060 (cfs), convert to m3/s. |
| Vietnam Mekong Delta (EIDC) (`vietnam_mekong`) | VN | `implemented` | no | Hourly discharge and sediment data via CEH EIDC. |
| Department of Water and Sanitation (`south_africa_dws`) | ZA | `implemented` | — |  |
| Caravan (unified large-sample hydrology) (`caravan`) | global | `implemented` | no | Unified format across CAMELS variants + extensions (v1.6). |
| Caravan-GRDC Extension (`caravan_grdc`) | global | `implemented` | no | 2025 extension adding GRDC data to Caravan. |
| GEOGloWS ECMWF V2 (`geoglows`) | global | `implemented` | yes | GEOGLOWS ECMWF V2 global simulated streamflow (keyless REST). Reach-based model exposed as 7 curated major-river virtual stations (Amazon... |
| GloFAS (ECMWF/Copernicus) (`glofas`) | global | `implemented` | yes | GloFAS v4 daily discharge (m3/s) via the keyless Open-Meteo Flood API; 15 virtual reporting points on major rivers (config['virtual_stati... |
| Global Runoff Data Centre (`grdc`) | global | `implemented` | no | No-redistribution. Historical daily. Covers countries with no national API. Used as fallback for: BG, BY, CY, EE, LT, LV, MD, MK, RO, RS,... |
| GSIM (Global Streamflow Indices and Metadata) (`gsim`) | global | `implemented` | no | Monthly indices from merged archives. Good for coverage gap analysis. |
| ROBIN (Reference Observatory of Basins) (`robin`) | global | `implemented` | no | ROBIN near-natural reference basins (CEH/EIDC). Offline archive: seed catalogue only; observations require local downloaded files. No obs... |
| WMO WHOS (Hydrological Observing System) (`wmo_whos`) | global | `implemented` | — | Federated WHOS / GEO DAB broker. Uses the public anonymous token; fetch_stations bounded by config['countries'] x limit. Discharge in m3/... |
| FFWC Bangladesh (BWDB) (`bangladesh_ffwc`) | BD | `research` | yes |  |
| ELWAS NRW (`germany_nrw`) | DE | `research` | yes | No open discharge API - NRW portals expose only level/temperature/precip (verified 2026-06). germany_pegelonline already covers the major... |
| Ecuador INAMHI (via GEOGloWS) (`peru_senamhi_legacy`) | EC | `research` | no | Legacy/duplicate entry, superseded by ecuador_inamhi and peru_senamhi; no connector. (name field is stale.) |
| Ilmateenistus (Estonia) (`estonia_ilmateenistus`) | EE | `research` | no |  |
| OVF (Hungary) (`hungary_ovf`) | HU | `research` | yes |  |
| PUPR SDA Indonesia (`indonesia_pupr`) | ID | `research` | yes | UNVERIFIED (2026-06): SIGI ArcGIS endpoint returns HTTP 500; fetch_stations fails. |
| CWC India (WRIS) (`india_cwc`) | IN | `research` | yes | Defensive dual-endpoint. Replaces india_wris in inventory. |
| India WRIS / CWC (`india_wris`) | IN | `research` | yes |  |
| ISPRA HIS-Central Italy (`italy_ispra_wof`) | IT | `research` | yes | UNVERIFIED (2026-06): 1-station stub; HIS-Central WaterOneFlow endpoints unverified; returns no discharge. |
| ARPA Piemonte (`italy_piedmont`) | IT | `research` | yes | ARPA Piemonte public API exposes only water LEVEL (m), no discharge; connector returns the catalogue with discharge_m3s=None. Verified 20... |
| SIR Toscana (`italy_tuscany`) | IT | `research` | yes | SIR Toscana exposes only hydrometric LEVEL (m), no discharge (verified 2026-06). Returns no discharge. |
| WAMIS (Water Management Information System) (`south_korea_wamis`) | KR | `research` | yes |  |
| SENAMHI Peru (`peru_senamhi`) | PE | `research` | yes | UNVERIFIED (2026-06): 1-station stub (PHISIS); returns no discharge. |
| PAGASA Philippines (`philippines_pagasa`) | PH | `research` | yes | UNVERIFIED (2026-06): fetch_stations fails (connection/retry error); returns no discharge. |
| INHGA Romania (`romania_inhga`) | RO | `research` | yes | UNVERIFIED (2026-06): 1-station stub, RoWaterAPI endpoint unverified, returns no discharge. |
| SHMU (Slovakia) (`slovakia_shmu`) | SK | `research` | no |  |
| DSI Turkey (FACE Portal) (`turkey_dsi`) | TR | `research` | no | Historical discharge 1936-2015. |
| CAMELS-AUS (`camels_aus`) | AU | `fallback` | no |  |
| CAMELS-BR (`camels_br`) | BR | `fallback` | no |  |
| CAMELS-CL (`camels_cl`) | CL | `fallback` | no |  |
| CAMELS-GB (`camels_gb`) | GB | `fallback` | no |  |
| CAMELS (Catchment Attributes and Meteorology for Large-sample Studies) (`camels_us`) | US | `fallback` | no |  |
| SAIH (regional real-time networks) (`spain_saih`) | ES | `manual` | yes | Distributed across basin authorities (Ebro, Guadalquivir, etc.). |
| CONAGUA BANDAS (`mexico_conagua`) | MX | `manual` | — |  |
| SNIRH Portugal (`portugal_snirh`) | PT | `manual` | no | Max 50 stations per download batch. |
| ISPRA SINTAI (`italy_ispra`) | IT | `deprecated` | no | HIS Central API is broken; replaced by italy_isprasina (SINA). |
