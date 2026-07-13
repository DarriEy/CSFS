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

## Status breakdown

Of the **98 cataloged sources**:

| Status | Count | Meaning |
| --- | ---: | --- |
| `implemented` | 76 | Registered connector exists in `csfs/connectors/`, with tests |
| `research` | 8 | API exists but needs investigation |
| `fallback` | 5 | Community/research dataset used for gap-filling |
| `manual` | 5 | No API; requires scraping or manual download |
| `degraded` | 1 | Connector exists but the upstream source is impaired |
| `deprecated` | 3 | Source retired or superseded |

In code, **84 connectors are registered**. **34 of the 76 implemented providers deliver realtime or near-realtime data**; the remainder are recent/archive sources, including roughly a dozen offline research archives (GRDC, Caravan, GSIM, EStreams, LamaH, CAMELS variants, ROBIN, ADHI, SIEREM).

!!! note "Live providers wobble"
    A connector being `implemented` means the code path is real and tested
    against recorded responses — not that the upstream agency API is up at
    any given moment. Transient upstream outages are expected and surface
    in `csfs health`.

## All cataloged providers

| Provider | Country | Status | Realtime | Notes |
| --- | --- | --- | --- | --- |
| SNIH Argentina (INA a5) (`argentina_snih`) | AR | `implemented` | yes | INA Alerta a5 REST API (discharge 'caudal' + stage series). The raw station catalogue has ~4,664 points but only ~890 hold a populated disch... |
| WMO WHOS-Plata (`wmo_whos_plata`) | AR, BO, BR, PY, UY | `degraded` | yes | La Plata River Basin federated access. Degraded 2026-07: ARG (INA-brokered) stations 500-error server-side, PRY/URY/BOL yield no data even o... |
| eHYD Austria (BMLUK) (`austria_ehyd`) | AT | `implemented` | yes | Official WFS service from the Federal Ministry (BMLUK). |
| LamaH-CE (Central Europe) (`lamah_ce`) | AT,DE,CZ | `implemented` | no | Danube basin focus. Hourly resolution available. |
| Bureau of Meteorology Water Data Online (`australia_bom`) | AU | `implemented` | yes |  |
| CAMELS-AUS (`camels_aus`) | AU | `fallback` | no |  |
| FHMZ Bosnia (`bosnia_fhmz`) | BA | `manual` | no | PDF hydrological yearbooks; requires tabula-style extraction. Connector removed 2026-07: the coded JSON API never existed (404 on every path... |
| FFWC Bangladesh (BWDB) (`bangladesh_ffwc`) | BD | `research` | yes |  |
| SPW Wallonia (`belgium_spw`) | BE | `implemented` | yes | No-redistribution license. |
| Waterinfo Flanders (`belgium_waterinfo`) | BE | `implemented` | yes |  |
| SIEREM (IRD African Hydrology) (`sierem`) | BF, BJ, CF, CG, CI, CM, GA, GN, ML, MR, NE, SN, TD, TG | `implemented` | no | IRD database focused on West and Central Africa. |
| EAEMDR Bulgaria (`bulgaria_eaemdr`) | BG | `implemented` | yes | Scrapes the daily Danube hydrology bulletin (/hidrology-en) for current discharge (m3/s) at 6 gauges (Novo Selo, Lom, Oryahovo, Svishtov, Ru... |
| NIMH Bulgaria (open data) (`bulgaria_nimh`) | BG | `implemented` | no | Daily discharge (Q, m3/s) from the NIMH river-runoff page (POST mydate=YYYY-MM-DD -> HTML table, one row/gauge). ~68 gauges; comma decimals;... |
| INE Bolivia (Caudales y Niveles) (`bolivia_ine`) | BO | `implemented` | no | NADA catalog dataset |
| ANA HidroWeb / Telemetria (`brazil_ana`) | BR | `implemented` | no | HidroSerieHistorica is the consolidated archive and lags 4-19 months (newest data anywhere as of 2026-07: March 2026, verified live on 11 ga... |
| CAMELS-BR (`camels_br`) | BR | `fallback` | no |  |
| Environment Canada Hydrometric Data (`environment_canada`) | CA | `implemented` | yes |  |
| BAFU Hydrodaten (`switzerland_bafu`) | CH | `implemented` | yes |  |
| CAMELS-CH (`camels_ch`) | CH | `implemented` | no |  |
| CAMELS-CL (`camels_cl`) | CL | `fallback` | no |  |
| CR2 explorador (Chile DGA archive) (`chile_cr2`) | CL | `implemented` | no | Universidad de Chile's CR2 explorador re-serves the DGA gauge archive through a scriptable request.php endpoint (JSON metadata + generated C... |
| DGA Chile (SNIA) (`chile_dga`) | CL | `deprecated` | no | Retired 2026-07: the ArcGIS service (DGA/Red_Hidrometrica/MapServer) was decommissioned (verified dead 2026-06-17) and the connector never h... |
| CAMELS-COL (`camels_co`) | CO | `implemented` | no |  |
| CAMELS-COL (`camels_col`) | CO | `manual` | no | Access-gated standalone (CAMELS-COL, Zenodo doi:10.5281/zenodo.15554735, CC-BY but files access-restricted: HTTP 403 + manual Request access... |
| Czech Hydrometeorological Institute (`czechia_chmu`) | CZ | `implemented` | no |  |
| CAMELS-DE (`camels_de`) | DE | `implemented` | no |  |
| GKD Bayern (`germany_bavaria`) | DE | `implemented` | yes | Discharge (m3/s) via HTML table scraping of the GKD portal; the CSV path is email/ToS-gated. ~610 stations; lat/lon not exposed by these pag... |
| LUBW Baden-Württemberg (`germany_bw`) | DE | `implemented` | yes | Discharge (m3/s) parsed from the HVZ JS catalogue (hvz_peg_stmn.js). LATEST-VALUE ONLY - no historical series; ~260 discharge stations. Live... |
| OpenGeodata.NRW (`germany_nrw`) | DE | `implemented` | no | Open discharge (Abfluss, m3/s) from the OpenGeodata.NRW CSV archive (q/index.json -> per-catchment, per-decade zips of 15-min series). Bulk/... |
| PEGELONLINE (BfG) (`germany_pegelonline`) | DE | `implemented` | yes | Primarily water level; discharge at federal waterways only. |
| Wasserportal Berlin (`germany_berlin`) | DE | `implemented` | yes | Berlin state surface-water portal: discharge + stage (cm -> m) + water temperature, daily means and 15-min instantaneous (station.php CSV; l... |
| CAMELS-DK (`camels_dk`) | DK | `implemented` | no | Offline archive (CAMELS-DK, Zenodo). Returns a small seed catalogue only; observations require local downloaded dataset files (config['data_... |
| VanDa Hydro (Denmark) (`denmark_dmihyd`) | DK | `implemented` | yes | Near-real-time river data via Danmarks Miljøportal (IoT). |
| ADHI (African Database of Hydrometric Indices) (`adhi`) | DZ, AO, BJ, BW, BF, BI, CM, CF, TD, CG, CD, CI, DJ, EG, GQ, ER, SZ, ET, GA, GM, GH, GN, GW, KE, LS, LR, LY, MG, MW, ML, MR, MZ, NA, NE, NG, RW, SN, SL, SO, ZA, SD, TZ, TG, TN, UG, ZM, ZW | `implemented` | no | Pan-African monthly discharge series and hydrometric statistics. |
| WMO WHOS-Africa (HydroSOS) (`wmo_whos_africa`) | DZ, AO, BJ, BW, BF, BI, CM, CF, TD, CG, CD, CI, DJ, EG, GQ, ER, SZ, ET, GA, GM, GH, GN, GW, KE, LS, LR, LY, MG, MW, ML, MR, MZ, NA, NE, NG, RW, SN, SL, SO, ZA, SD, TZ, TG, TN, UG, ZM, ZW | `deprecated` | no | Connector removed 2026-07: the whos-ra1 view was deleted upstream (HTTP 500 'View whos-ra1 not found' on every call, verified 2026-07-13); n... |
| Ecuador INAMHI (GEOGLOWS) (`ecuador_inamhi`) | EC | `implemented` | yes | INAMHI Ecuador streamflow via the GEOGLOWS ECMWF model (reach-based, m3/s). Shares the GEOGLOWS backend with the global geoglows connector b... |
| Ilmateenistus (Estonia) (`estonia_ilmateenistus`) | EE | `research` | no |  |
| CEDEX Anuario de Aforos (`spain_cedex`) | ES | `implemented` | no | Offline archive connector. Returns the seed station catalogue, but yields observations only when config['data_dir'] points at downloaded yea... |
| SAIH (regional real-time networks) (`spain_saih`) | ES | `manual` | yes | Distributed across basin authorities (Ebro, Guadalquivir, etc.). |
| CAMELS-FI (`camels_fi`) | FI | `implemented` | no | Dataset artifact (CAMELS-FI, Zenodo doi:10.5281/zenodo.15853357, CC-BY-4.0; ESSD preprint under review). Single bundle: per-gauge timeseries... |
| SYKE (Finnish Environment Institute) (`finland_syke`) | FI | `implemented` | no |  |
| CAMELS-FR (`camels_fr`) | FR | `implemented` | no | Dataset artifact (CAMELS-FR, Recherche Data Gouv doi:10.57745/WH7FJR, CC-BY-4.0). Auto-downloads two archives via ensure_dataset: per-statio... |
| Hub'Eau Hydrométrie (`france_hubeau`) | FR | `implemented` | yes | Returns discharge in L/s — divide by 1000 for m3/s. |
| CAMELS-GB (`camels_gb`) | GB | `fallback` | no |  |
| SEPA (Scotland) (`scotland_sepa`) | GB | `implemented` | yes | Uses KISTERS KiWIS service. |
| UK Environment Agency Hydrology API (`uk_ea`) | GB | `implemented` | yes | Open Government Licence. Covers England only. |
| UK National River Flow Archive (`uk_nrfa`) | GB | `implemented` | no | Historical daily only; complements uk_ea for long records. Gauged daily flow publishes ~annually per UK water year (archive currently ends 2... |
| OpenHI Greece (`greece_openhi`) | GR | `implemented` | — |  |
| DHMZ (Croatia) (`croatia_dhz`) | HR | `implemented` | yes | Real-time data via backend hisbaza.py API. NOTE: the zadnjipodaci feed serves water LEVEL (cm), not discharge — stored in the discharge fiel... |
| OVF (Hungary) (`hungary_ovf`) | HU | `research` | yes |  |
| EPA Ireland HydroNet (`ireland_epa`) | IE | `implemented` | yes |  |
| Caravan-Israel Extension (Zenodo) (`israel_caravan`) | IL | `implemented` | no | Zenodo record 15003600. |
| CAMELS-IND (`camels_ind`) | IN | `implemented` | no |  |
| CAMELS-IND (`camels_in`) | IN | `implemented` | no |  |
| CWC India (WRIS) (`india_cwc`) | IN | `research` | yes | Defensive dual-endpoint. Replaces india_wris in inventory. |
| India WRIS / CWC (`india_wris`) | IN | `research` | yes |  |
| LamaH-Ice (`iceland_lamahice`) | IS | `implemented` | no |  |
| ARPAE Emilia-Romagna (`italy_emilia`) | IT | `implemented` | yes | Discharge (m3/s) from the ARPAE open-data instantaneous-flow feed (dati-simc.arpae.it). Only ~7 Po-river discharge gauges are public; rollin... |
| ISPRA SINTAI (`italy_ispra`) | IT | `deprecated` | no | HIS Central API is broken; replaced by italy_isprasina (SINA). |
| MLIT Water Information System (`japan_mlit`) | JP | `implemented` | yes |  |
| CA-discharge (Central Asian Discharge Dataset) (`ca_discharge`) | KG,TJ,KZ,UZ,AF | `implemented` | no | Academic dataset covering mountainous Central Asia. |
| WAMIS (Water Management Information System) (`south_korea_wamis`) | KR | `research` | yes |  |
| LHMT (Lithuania) (`lithuania_lhmt`) | LT | `implemented` | yes | Hydrology API launched Nov 2023. Serves water level (cm) and water temperature only — no discharge; the connector emits stage (m) + water_te... |
| CAMELS-LUX (`camels_lux`) | LU | `implemented` | no | Dataset artifact (CAMELS-LUX, Zenodo doi:10.5281/zenodo.13846619, CC-BY-4.0; ESSD preprint under review). Auto-downloads the timeseries bund... |
| EStreams (European Streamflow Dataset) (`estreams`) | LU,AL,ME,MK | `implemented` | no | Catalogue connector for countries without national APIs. |
| CONAGUA BANDAS (`mexico_conagua`) | MX | `manual` | — |  |
| Rijkswaterstaat (`netherlands_rws`) | NL | `implemented` | yes |  |
| Norwegian Water Resources (NVE) (`norway_nve`) | NO | `implemented` | yes |  |
| CAMELS-NZ (`camels_nz`) | NZ | `implemented` | no | Dataset artifact (CAMELS-NZ, U. Canterbury figshare doi:10.26021/canterburynz.28827644, CC-BY-4.0). Auto-downloads daily streamflow (flow, m... |
| New Zealand Regional Councils (Hilltop) (`newzealand_hilltop`) | NZ | `implemented` | yes | Distributed across regional councils, each running Hilltop servers. |
| STRI Panama Canal Watershed (ACP) (`panama_stri`) | PA | `implemented` | yes |  |
| CAMELS-PE (`camels_pe`) | PE | `implemented` | no | Dataset artifact (CAMELS-PE, Llauca et al. 2026, Zenodo doi:10.5281/zenodo.20058778, CC-BY-4.0). Single bundle: per-catchment timeseries wit... |
| Pakistan IRSA/WAPDA (`pakistan_wapda`) | PK | `implemented` | yes |  |
| IMGW Public Data (`poland_imgw`) | PL | `implemented` | no | Hydrological year (Nov start). All gauges in monthly zip files. |
| SNIRH Portugal (`portugal_snirh`) | PT | `implemented` | no | Daily-mean discharge (pars 1850) and stage (pars 1845) scraped from the janela_verdados.php HTML tables; bundled 715-gauge seed catalog (cod... |
| R-ArcticNET v4.0 (Russian Arctic) (`russia_arcticnet`) | RU | `implemented` | no | Monthly mean discharge for Russian Arctic stations. |
| CAMELS-SE (`camels_se`) | SE | `implemented` | no | Dataset artifact (CAMELS-SE, SND 2023-173, CC-BY-4.0). Auto-downloads two archives via ensure_dataset: per-catchment daily Qobs_m3s timeseri... |
| SMHI Open Data — Hydrology (`sweden_smhi`) | SE | `implemented` | yes | Two discharge products — parameter 1 "Vattenföring (Dygn)" (daily mean, connector default) and parameter 2 "Vattenföring (15 min)" via confi... |
| ARSO (Slovenia) (`slovenia_arso`) | SI | `implemented` | yes | Real-time XML feed of latest observations. |
| SHMU (Slovakia) (`slovakia_shmu`) | SK | `research` | no |  |
| HII (Hydro-Informatics Institute Thailand) (`thailand_hii`) | TH | `implemented` | yes |  |
| DSI Turkey (FACE Portal) (`turkey_dsi`) | TR | `research` | no | Historical discharge 1936-2015. |
| WRA (Taiwan Water Resources Agency) (`taiwan_wra`) | TW | `implemented` | yes | Bilingual API (English + Chinese field names). |
| CAMELS (Catchment Attributes and Meteorology for Large-sample Studies) (`camels_us`) | US | `fallback` | no |  |
| CAMELSH (Hourly US) (`camelsh`) | US | `implemented` | no | Hourly CAMELSH (Zenodo, 1980-2024). Offline archive: seed catalogue only; observations require local downloaded files (config['data_dir']).... |
| USGS National Water Information System (NWIS) (`usgs`) | US | `implemented` | yes | Gold standard. Discharge param 00060 (cfs), convert to m3/s. |
| Vietnam Mekong Delta (EIDC) (`vietnam_mekong`) | VN | `implemented` | no | Static EIDC ratings archive (4 gauges, records end 2017-09) — backfill-only, like chile_cr2; scheduled short-lookback runs always yield 0, h... |
| DWS South Africa (Verified Hydrology) (`southafrica_dws`) | ZA | `implemented` | no | Department of Water and Sanitation verified hydrology (HyData.aspx <pre> text): daily-mean discharge plus instantaneous discharge and stage... |
| HYSETS (`hysets`) | ['CA', 'US', 'MX'] | `implemented` | no | Dataset artifact (HYSETS, OSF doi:10.17605/OSF.IO/RPC3W, CC-BY-4.0). Observed daily discharge (m3/s) is a discharge(watershed, time) variabl... |
| CAMELS-SPAT (`camels_spat`) | ['US', 'CA'] | `manual` | no | Distribution-gated standalone (CAMELS-SPAT, FRDR doi:10.20383/103.01306, Globus-only; no HTTPS endpoint). NOT in the provenance-gated tier (... |
| Caravan (unified large-sample hydrology) (`caravan`) | global | `implemented` | no | Unified format across CAMELS variants + extensions (v1.6). |
| Caravan-GRDC Extension (`caravan_grdc`) | global | `implemented` | no | 2025 extension adding GRDC data to Caravan. |
| GEOGloWS ECMWF V2 (`geoglows`) | global | `implemented` | yes | GEOGLOWS ECMWF V2 global simulated streamflow (keyless REST). Reach-based model exposed as 7 curated major-river virtual stations (Amazon, M... |
| GSIM (Global Streamflow Indices and Metadata) (`gsim`) | global | `implemented` | no | Monthly indices from merged archives. Good for coverage gap analysis. |
| GloFAS (ECMWF/Copernicus) (`glofas`) | global | `implemented` | yes | GloFAS v4 daily discharge (m3/s) via the keyless Open-Meteo Flood API; 15 virtual reporting points on major rivers (config['virtual_stations... |
| Global Runoff Data Centre (`grdc`) | global | `implemented` | no | No-redistribution. Historical daily. Covers countries with no national API. Used as fallback for: BG, BY, CY, EE, LT, LV, MD, MK, RO, RS, RU... |
| ROBIN (Reference Observatory of Basins) (`robin`) | global | `implemented` | no | ROBIN near-natural reference basins (CEH/EIDC). Offline archive: seed catalogue only; observations require local downloaded files. No obs in... |
| WMO WHOS (Hydrological Observing System) (`wmo_whos`) | global | `implemented` | — | Federated WHOS / GEO DAB broker. Uses the public anonymous token; fetch_stations bounded by config['countries'] x limit. Discharge in m3/s.... |
