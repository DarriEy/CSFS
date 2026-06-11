#!/bin/bash
# CSFS Database Builder
# Runs all live-API connectors sequentially to populate the local DuckDB.
# Usage: ./scripts/build_database.sh [--max-stations N] [--lookback HOURS]

set -e

MAX_STATIONS="${1:---max-stations 200}"
LOOKBACK="${2:-168}"

echo "=== CSFS Database Builder ==="
echo "Max stations per provider: ${MAX_STATIONS}"
echo "Lookback: ${LOOKBACK} hours"
echo ""

# Tier 1: Large providers with stable APIs (run with station limits first)
echo "--- TIER 1: Major providers ---"
for p in usgs environment_canada france_hubeau; do
    echo "Fetching $p..."
    csfs fetch -p $p --lookback $LOOKBACK -n 500 || echo "  $p: FAILED (continuing)"
done

# Tier 2: Medium European providers
echo ""
echo "--- TIER 2: European providers ---"
for p in germany_pegelonline uk_ea uk_nrfa norway_nve sweden_smhi \
         finland_syke austria_ehyd switzerland_bafu poland_imgw \
         spain_cedex netherlands_rws ireland_epa \
         denmark_dmihyd belgium_waterinfo belgium_spw \
         romania_inhga czechia_chmu croatia_dhz slovenia_arso \
         greece_openhi scotland_sepa bulgaria_eaemdr \
         lithuania_lhmt bosnia_fhmz iceland_lamahice \
         bulgaria_nimh; do
    echo "Fetching $p..."
    csfs fetch -p $p --lookback $LOOKBACK -n 200 || echo "  $p: FAILED (continuing)"
done

# Tier 3: Italian/German regional
echo ""
echo "--- TIER 3: Regional providers ---"
for p in italy_ispra_wof italy_emilia italy_piedmont italy_tuscany \
         germany_bavaria germany_bw germany_nrw; do
    echo "Fetching $p..."
    csfs fetch -p $p --lookback $LOOKBACK -n 100 || echo "  $p: FAILED (continuing)"
done

# Tier 4: Americas
echo ""
echo "--- TIER 4: Americas ---"
for p in brazil_ana chile_dga argentina_snih colombia_ideam \
         peru_senamhi ecuador_inamhi \
         elsalvador_marn panama_stri jamaica_wra \
         bolivia_ine pakistan_wapda; do
    echo "Fetching $p..."
    csfs fetch -p $p --lookback $LOOKBACK -n 100 || echo "  $p: FAILED (continuing)"
done

# Tier 5: Asia/Africa/Oceania
echo ""
echo "--- TIER 5: Asia/Africa/Oceania ---"
for p in japan_mlit taiwan_wra \
         china_mwr thailand_hii \
         iran_iwrmc philippines_dpwh malaysia_did \
         nepal_icimod vietnam_mekong afghanistan_usgs \
         kazakhstan_kazhydromet \
         australia_bom newzealand_hilltop \
         south_africa_dws; do
    echo "Fetching $p..."
    csfs fetch -p $p --lookback $LOOKBACK -n 100 || echo "  $p: FAILED (continuing)"
done

# Tier 6: Aggregator/regional
echo ""
echo "--- TIER 6: Aggregators ---"
for p in danube_his russia_arcticnet estreams; do
    echo "Fetching $p..."
    csfs fetch -p $p --lookback 720 -n 50 || echo "  $p: FAILED (continuing)"
done

echo ""
echo "=== Build complete ==="
csfs status
