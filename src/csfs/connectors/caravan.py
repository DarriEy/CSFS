"""Caravan connector -- unified large-sample hydrology dataset (Zenodo).

Caravan aggregates 10,000+ catchments globally from multiple CAMELS datasets
(US, GB, CL, BR, AUS, DE, IND, COL, and extensions). Data is distributed on
Zenodo (record 17593968 for CSV) as standardized time series and attributes.

This connector supports two modes:

1. **Station catalogue** -- a curated seed list of representative
   stations from each sub-dataset, with coordinates and metadata
   embedded in the connector.

2. **Observations from local files** -- Caravan distributes CSV time
   series in ``timeseries/csv/{basin_id}.csv`` with columns ``date``
   and ``streamflow``.

References
----------
- DOI: 10.5281/zenodo.17593968
- Paper: Kratzert et al. (2023, 2025 updates) – Caravan
"""

from __future__ import annotations

import csv
import io
from datetime import UTC, datetime
from pathlib import Path

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.downloads import ensure_dataset
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import (
    Observation,
    QualityFlag,
    Station,
    TimeSeriesChunk,
)
from csfs.core.registry import register

logger = structlog.get_logger()

# Zenodo record for Caravan v1.6 (CSV version)
_ZENODO_RECORD_ID = "17593968"
# Zenodo record for GRDC-Caravan extension (2025)
_GRDC_CARAVAN_RECORD_ID = "15349031"

_ZENODO_DOWNLOAD_URL = (
    f"https://zenodo.org/records/{_ZENODO_RECORD_ID}"
)

# ---------------------------------------------------------------------------
# Curated seed catalogue -- representative stations from each dataset.
# ---------------------------------------------------------------------------

_SEED_STATIONS: list[dict] = [
    # CAMELS-US / CAMELSH (United States)
    {
        "id": 'camels_us_01013500',
        "name": 'Fish River near Fort Kent',
        "lat": 47.24,
        "lon": -68.58,
        "country": 'US',
        "river": 'Fish River',
        "area": 2252.0,
        "source": 'camels_us',
    },
    {
        "id": 'camels_us_01022500',
        "name": 'Narraguagus River at Cherryfield',
        "lat": 44.61,
        "lon": -67.93,
        "country": 'US',
        "river": 'Narraguagus River',
        "area": 588.0,
        "source": 'camels_us',
    },
    # CAMELS-DE (Germany) - New in v1.6
    {
        "id": 'camels_de_DE110000',
        "name": 'Baden-Württemberg Sample Gauge',
        "lat": 48.5,
        "lon": 9.0,
        "country": 'DE',
        "river": 'Neckar',
        "area": 500.0,
        "source": 'camels_de',
    },
    # CAMELS-IND (India) - New in v1.6
    {
        "id": 'camels_in_01001',
        "name": 'Peninsular India Sample',
        "lat": 20.0,
        "lon": 78.0,
        "country": 'IN',
        "river": 'Godavari',
        "area": 1200.0,
        "source": 'camels_in',
    },
    # CAMELS-COL (Colombia) - New in v1.6
    {
        "id": 'camels_co_26137000',
        "name": 'Rio Magdalena Sample',
        "lat": 4.5,
        "lon": -74.8,
        "country": 'CO',
        "river": 'Magdalena',
        "area": 25000.0,
        "source": 'camels_co',
    },
    # CAMELS-GB (Great Britain)
    {
        "id": 'camels_gb_39001',
        "name": 'River Thames at Kingston',
        "lat": 51.41,
        "lon": -0.31,
        "country": 'GB',
        "river": 'River Thames',
        "area": 9948.0,
        "source": 'camels_gb',
    },
    # ... (keeping existing seed stations below in implementation)
    {
        "id": 'camels_us_01031500',
        "name": 'Piscataquis River near Dover-Foxcroft',
        "lat": 45.2,
        "lon": -69.35,
        "country": 'US',
        "river": 'Piscataquis River',
        "area": 770.0,
        "source": 'camels_us',
    },
    {
        "id": 'camels_us_01047000',
        "name": 'Carrabassett River near North Anson',
        "lat": 44.89,
        "lon": -70.04,
        "country": 'US',
        "river": 'Carrabassett River',
        "area": 906.0,
        "source": 'camels_us',
    },
    {
        "id": 'camels_us_02246000',
        "name": 'North Fork Black Creek near Middleburg',
        "lat": 30.08,
        "lon": -81.89,
        "country": 'US',
        "river": 'Black Creek',
        "area": 451.0,
        "source": 'camels_us',
    },
    {
        "id": 'camels_us_03015500',
        "name": 'Brokenstraw Creek at Youngsville',
        "lat": 41.85,
        "lon": -79.32,
        "country": 'US',
        "river": 'Brokenstraw Creek',
        "area": 831.0,
        "source": 'camels_us',
    },
    {
        "id": 'camels_us_06191500',
        "name": 'Yellowstone River at Corwin Springs',
        "lat": 45.11,
        "lon": -110.79,
        "country": 'US',
        "river": 'Yellowstone River',
        "area": 6783.0,
        "source": 'camels_us',
    },
    {
        "id": 'camels_us_09066300',
        "name": 'Gore Creek at Upper Station near Minturn',
        "lat": 39.64,
        "lon": -106.34,
        "country": 'US',
        "river": 'Gore Creek',
        "area": 102.0,
        "source": 'camels_us',
    },
    {
        "id": 'camels_us_11264500',
        "name": 'Merced River at Happy Isles Bridge',
        "lat": 37.73,
        "lon": -119.56,
        "country": 'US',
        "river": 'Merced River',
        "area": 469.0,
        "source": 'camels_us',
    },
    {
        "id": 'camels_us_14301000',
        "name": 'Nehalem River near Foss',
        "lat": 45.7,
        "lon": -123.24,
        "country": 'US',
        "river": 'Nehalem River',
        "area": 1738.0,
        "source": 'camels_us',
    },
    # CAMELS-GB (Great Britain)
    {
        "id": 'camels_gb_15006',
        "name": 'River Nairn at Firhall',
        "lat": 57.58,
        "lon": -3.88,
        "country": 'GB',
        "river": 'River Nairn',
        "area": 313.0,
        "source": 'camels_gb',
    },
    {
        "id": 'camels_gb_21009',
        "name": 'River Tweed at Norham',
        "lat": 55.73,
        "lon": -2.16,
        "country": 'GB',
        "river": 'River Tweed',
        "area": 4390.0,
        "source": 'camels_gb',
    },
    {
        "id": 'camels_gb_27009',
        "name": 'River Ouse at Skelton',
        "lat": 54.0,
        "lon": -1.14,
        "country": 'GB',
        "river": 'River Ouse',
        "area": 3315.0,
        "source": 'camels_gb',
    },
    {
        "id": 'camels_gb_33029',
        "name": 'River Lark at Temple',
        "lat": 52.32,
        "lon": 0.55,
        "country": 'GB',
        "river": 'River Lark',
        "area": 272.0,
        "source": 'camels_gb',
    },
    {
        "id": 'camels_gb_39001',
        "name": 'River Thames at Kingston',
        "lat": 51.41,
        "lon": -0.31,
        "country": 'GB',
        "river": 'River Thames',
        "area": 9948.0,
        "source": 'camels_gb',
    },
    {
        "id": 'camels_gb_45001',
        "name": 'River Exe at Thorverton',
        "lat": 50.8,
        "lon": -3.51,
        "country": 'GB',
        "river": 'River Exe',
        "area": 601.0,
        "source": 'camels_gb',
    },
    {
        "id": 'camels_gb_54001',
        "name": 'River Severn at Bewdley',
        "lat": 52.38,
        "lon": -2.32,
        "country": 'GB',
        "river": 'River Severn',
        "area": 4325.0,
        "source": 'camels_gb',
    },
    {
        "id": 'camels_gb_67015',
        "name": 'River Dee at Manley Hall',
        "lat": 52.98,
        "lon": -3.18,
        "country": 'GB',
        "river": 'River Dee',
        "area": 1019.0,
        "source": 'camels_gb',
    },
    # CAMELS-CL (Chile)
    {
        "id": 'camels_cl_1311002',
        "name": 'Rio Elqui en Algarrobal',
        "lat": -29.97,
        "lon": -70.6,
        "country": 'CL',
        "river": 'Rio Elqui',
        "area": 3428.0,
        "source": 'camels_cl',
    },
    {
        "id": 'camels_cl_4530001',
        "name": 'Rio Aconcagua en Chacabuquito',
        "lat": -32.85,
        "lon": -70.51,
        "country": 'CL',
        "river": 'Rio Aconcagua',
        "area": 2110.0,
        "source": 'camels_cl',
    },
    {
        "id": 'camels_cl_5410002',
        "name": 'Rio Maipo en El Manzano',
        "lat": -33.59,
        "lon": -70.38,
        "country": 'CL',
        "river": 'Rio Maipo',
        "area": 4968.0,
        "source": 'camels_cl',
    },
    {
        "id": 'camels_cl_7355002',
        "name": 'Rio Maule en Armerillo',
        "lat": -35.67,
        "lon": -71.1,
        "country": 'CL',
        "river": 'Rio Maule',
        "area": 4045.0,
        "source": 'camels_cl',
    },
    {
        "id": 'camels_cl_8317001',
        "name": 'Rio Biobio en Rucalhue',
        "lat": -37.73,
        "lon": -71.72,
        "country": 'CL',
        "river": 'Rio Biobio',
        "area": 5700.0,
        "source": 'camels_cl',
    },
    {
        "id": 'camels_cl_9437001',
        "name": 'Rio Imperial en Almagro',
        "lat": -38.72,
        "lon": -72.62,
        "country": 'CL',
        "river": 'Rio Imperial',
        "area": 12054.0,
        "source": 'camels_cl',
    },
    # CAMELS-BR (Brazil)
    {
        "id": 'camels_br_10200000',
        "name": 'Rio Negro em Manaus',
        "lat": -3.13,
        "lon": -60.02,
        "country": 'BR',
        "river": 'Rio Negro',
        "area": 696810.0,
        "source": 'camels_br',
    },
    {
        "id": 'camels_br_31500000',
        "name": 'Rio Paraiba do Sul em Campos',
        "lat": -21.75,
        "lon": -41.33,
        "country": 'BR',
        "river": 'Rio Paraiba do Sul',
        "area": 55500.0,
        "source": 'camels_br',
    },
    {
        "id": 'camels_br_40100000',
        "name": 'Rio Doce em Colatina',
        "lat": -19.54,
        "lon": -40.63,
        "country": 'BR',
        "river": 'Rio Doce',
        "area": 78200.0,
        "source": 'camels_br',
    },
    {
        "id": 'camels_br_46035000',
        "name": 'Rio Jequitinhonha em Itaobim',
        "lat": -16.57,
        "lon": -41.5,
        "country": 'BR',
        "river": 'Rio Jequitinhonha',
        "area": 49000.0,
        "source": 'camels_br',
    },
    {
        "id": 'camels_br_61903000',
        "name": 'Rio Paraguai em Caceres',
        "lat": -16.07,
        "lon": -57.68,
        "country": 'BR',
        "river": 'Rio Paraguai',
        "area": 34100.0,
        "source": 'camels_br',
    },
    {
        "id": 'camels_br_74100000',
        "name": 'Rio Iguacu em Uniao da Vitoria',
        "lat": -26.23,
        "lon": -51.07,
        "country": 'BR',
        "river": 'Rio Iguacu',
        "area": 24200.0,
        "source": 'camels_br',
    },
    # CAMELS-AUS (Australia)
    {
        "id": 'camels_aus_102101A',
        "name": 'Daintree River at Bairds',
        "lat": -16.52,
        "lon": 145.37,
        "country": 'AU',
        "river": 'Daintree River',
        "area": 568.0,
        "source": 'camels_aus',
    },
    {
        "id": 'camels_aus_110003A',
        "name": 'Burdekin River at Sellheim',
        "lat": -19.87,
        "lon": 146.42,
        "country": 'AU',
        "river": 'Burdekin River',
        "area": 36260.0,
        "source": 'camels_aus',
    },
    {
        "id": 'camels_aus_203012',
        "name": 'Clarence River at Lilydale',
        "lat": -29.42,
        "lon": 152.62,
        "country": 'AU',
        "river": 'Clarence River',
        "area": 16100.0,
        "source": 'camels_aus',
    },
    {
        "id": 'camels_aus_401210',
        "name": 'Goulburn River at Eildon',
        "lat": -37.23,
        "lon": 145.9,
        "country": 'AU',
        "river": 'Goulburn River',
        "area": 3780.0,
        "source": 'camels_aus',
    },
    {
        "id": 'camels_aus_410730',
        "name": 'Murray River at Jingellic',
        "lat": -35.92,
        "lon": 147.68,
        "country": 'AU',
        "river": 'Murray River',
        "area": 10200.0,
        "source": 'camels_aus',
    },
    {
        "id": 'camels_aus_613002',
        "name": 'Swan River at Great Northern Highway',
        "lat": -31.73,
        "lon": 116.03,
        "country": 'AU',
        "river": 'Swan River',
        "area": 1230.0,
        "source": 'camels_aus',
    },
    # CAMELS extensions -- LamaH-CE (Central Europe)
    {
        "id": 'camels_lamah_398',
        "name": 'Donau at Wien',
        "lat": 48.23,
        "lon": 16.38,
        "country": 'AT',
        "river": 'Donau',
        "area": 101700.0,
        "source": 'lamah_ce',
    },
    {
        "id": 'camels_lamah_120',
        "name": 'Inn at Innsbruck',
        "lat": 47.26,
        "lon": 11.39,
        "country": 'AT',
        "river": 'Inn',
        "area": 5790.0,
        "source": 'lamah_ce',
    },
    {
        "id": 'camels_lamah_200',
        "name": 'Salzach at Salzburg',
        "lat": 47.8,
        "lon": 13.05,
        "country": 'AT',
        "river": 'Salzach',
        "area": 6700.0,
        "source": 'lamah_ce',
    },
    {
        "id": 'camels_lamah_310',
        "name": 'Mur at Graz',
        "lat": 47.07,
        "lon": 15.44,
        "country": 'AT',
        "river": 'Mur',
        "area": 8530.0,
        "source": 'lamah_ce',
    },
    # CAMELS extensions -- HYSETS (North America)
    {
        "id": 'camels_hysets_01AD002',
        "name": 'Saint John River at Fort Kent',
        "lat": 47.26,
        "lon": -68.6,
        "country": 'CA',
        "river": 'Saint John River',
        "area": 14700.0,
        "source": 'hysets',
    },
    {
        "id": 'camels_hysets_02HA003',
        "name": 'Credit River at Norval',
        "lat": 43.62,
        "lon": -79.85,
        "country": 'CA',
        "river": 'Credit River',
        "area": 641.0,
        "source": 'hysets',
    },
    {
        "id": 'camels_hysets_05BB001',
        "name": 'Bow River at Banff',
        "lat": 51.17,
        "lon": -115.57,
        "country": 'CA',
        "river": 'Bow River',
        "area": 2210.0,
        "source": 'hysets',
    },
    {
        "id": 'camels_hysets_08MF005',
        "name": 'Fraser River at Hope',
        "lat": 49.38,
        "lon": -121.45,
        "country": 'CA',
        "river": 'Fraser River',
        "area": 217000.0,
        "source": 'hysets',
    },
    # CAMELS-SE (Sweden)
    {
        "id": 'camels_se_1',
        "name": 'Torne alv vid Kukkolaforsen',
        "lat": 65.97,
        "lon": 24.08,
        "country": 'SE',
        "river": 'Torne alv',
        "area": 40130.0,
        "source": 'camels_se',
    },
    {
        "id": 'camels_se_22',
        "name": 'Dalälven vid Fäggeby',
        "lat": 60.5,
        "lon": 16.47,
        "country": 'SE',
        "river": 'Dalälven',
        "area": 28954.0,
        "source": 'camels_se',
    },
    # CAMELS-DK (Denmark)
    {
        "id": 'camels_dk_21000040',
        "name": 'Gudenaa ved Skovgaarden',
        "lat": 56.41,
        "lon": 9.85,
        "country": 'DK',
        "river": 'Gudenaa',
        "area": 1091.0,
        "source": 'camels_dk',
    },
    {
        "id": 'camels_dk_46000004',
        "name": 'Skjern Aa ved Ahlergaarde',
        "lat": 55.9,
        "lon": 8.5,
        "country": 'DK',
        "river": 'Skjern Aa',
        "area": 1420.0,
        "source": 'camels_dk',
    },
    # India (gap country)
    {
        "id": 'camels_in_ganges_farakka',
        "name": 'Ganges at Farakka',
        "lat": 25.0,
        "lon": 87.9,
        "country": 'IN',
        "river": 'Ganges',
        "area": 835000.0,
        "source": 'camels_in',
    },
    {
        "id": 'camels_in_brahmaputra_pandu',
        "name": 'Brahmaputra at Pandu',
        "lat": 26.2,
        "lon": 91.7,
        "country": 'IN',
        "river": 'Brahmaputra',
        "area": 405000.0,
        "source": 'camels_in',
    },
    # South Korea (gap country)
    {
        "id": 'camels_kr_han_seoul',
        "name": 'Han River at Seoul',
        "lat": 37.5,
        "lon": 127.0,
        "country": 'KR',
        "river": 'Han River',
        "area": 23800.0,
        "source": 'camels_kr',
    },
    {
        "id": 'camels_kr_nakdong_changnyeong',
        "name": 'Nakdong at Changnyeong',
        "lat": 35.5,
        "lon": 128.5,
        "country": 'KR',
        "river": 'Nakdong',
        "area": 16352.0,
        "source": 'camels_kr',
    },
    # Mexico (gap country)
    {
        "id": 'camels_mx_grijalva_villahermosa',
        "name": 'Grijalva at Villahermosa',
        "lat": 18.0,
        "lon": -92.9,
        "country": 'MX',
        "river": 'Grijalva',
        "area": 36500.0,
        "source": 'camels_mx',
    },
    {
        "id": 'camels_mx_lerma_guadalajara',
        "name": 'Lerma-Santiago at Guadalajara',
        "lat": 20.7,
        "lon": -103.3,
        "country": 'MX',
        "river": 'Lerma-Santiago',
        "area": 51200.0,
        "source": 'camels_mx',
    },
    # Italy (gap country)
    {
        "id": 'camels_it_po_pontelagoscuro',
        "name": 'Po at Pontelagoscuro',
        "lat": 44.9,
        "lon": 11.6,
        "country": 'IT',
        "river": 'Po',
        "area": 70091.0,
        "source": 'camels_it',
    },
    {
        "id": 'camels_it_tevere_roma',
        "name": 'Tevere at Roma',
        "lat": 41.9,
        "lon": 12.5,
        "country": 'IT',
        "river": 'Tevere',
        "area": 16545.0,
        "source": 'camels_it',
    },
    # Estonia (gap country)
    {
        "id": 'camels_ee_emajogi_tartu',
        "name": 'Emajogi at Tartu',
        "lat": 58.4,
        "lon": 26.7,
        "country": 'EE',
        "river": 'Emajogi',
        "area": 7850.0,
        "source": 'camels_ee',
    },
    {
        "id": 'camels_ee_narva_narva',
        "name": 'Narva at Narva',
        "lat": 59.4,
        "lon": 28.0,
        "country": 'EE',
        "river": 'Narva',
        "area": 56200.0,
        "source": 'camels_ee',
    },
    # Peru (gap country)
    {
        "id": 'camels_pe_amazonas_iquitos',
        "name": 'Amazonas at Iquitos',
        "lat": -3.7,
        "lon": -73.2,
        "country": 'PE',
        "river": 'Amazonas',
        "area": 720000.0,
        "source": 'camels_pe',
    },
    {
        "id": 'camels_pe_rimac_lima',
        "name": 'Rimac at Lima',
        "lat": -12.0,
        "lon": -77.0,
        "country": 'PE',
        "river": 'Rimac',
        "area": 2237.0,
        "source": 'camels_pe',
    },
    # Portugal (gap country)
    {
        "id": 'camels_pt_douro_porto',
        "name": 'Douro at Porto',
        "lat": 41.1,
        "lon": -8.6,
        "country": 'PT',
        "river": 'Douro',
        "area": 97603.0,
        "source": 'camels_pt',
    },
    {
        "id": 'camels_pt_tejo_santarem',
        "name": 'Tejo at Santarem',
        "lat": 39.2,
        "lon": -8.7,
        "country": 'PT',
        "river": 'Tejo',
        "area": 67490.0,
        "source": 'camels_pt',
    },
]


def _safe_float(value: object) -> float | None:
    """Safely convert a value to float."""
    if value is None:
        return None
    try:
        return float(str(value))
    except (ValueError, TypeError):
        return None


@register("caravan")
class CaravanConnector(BaseConnector):
    """Connector for the Caravan global hydrology dataset on Zenodo.

    Configuration options (via ``config`` dict):
        data_dir : str | Path
            Directory containing Caravan CSV time series files.
            Expected layout: ``timeseries/csv/{basin_id}.csv`` or
            flat ``{basin_id}.csv``.
        seed_only : bool
            If True (default), return the curated seed catalogue.
            If False, attempt to discover stations from Zenodo.
    """

    slug = "caravan"
    display_name = "Caravan (Global Large-Sample Hydrology Dataset)"
    base_url = "https://zenodo.org/api"
    country_codes: list[str] = ["global"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return stations from seed list or Zenodo discovery.

        By default, the curated seed list is returned (fast, no network).
        Set ``config["seed_only"] = False`` to attempt Zenodo metadata
        discovery (falls back to seed on error).
        """
        seed_only = self.config.get("seed_only", True)

        if not seed_only:
            try:
                return await self._fetch_stations_zenodo()
            except Exception as exc:
                logger.warning(
                    "caravan_zenodo_fallback_to_seed",
                    error=str(exc),
                )

        stations = self._build_seed_stations()
        logger.info(
            "stations_fetched",
            provider=self.slug,
            count=len(stations),
            source="seed",
        )
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Read observations from local Caravan CSV files.

        Caravan CSV naming: ``timeseries/csv/{basin_id}.csv`` or
        ``{basin_id}.csv`` with columns ``date, streamflow``.

        The Caravan dataset is auto-downloaded and cached on first use (see
        :func:`csfs.core.downloads.ensure_dataset`) — note this is a large
        (~12.5 GB) archive. Set ``config['data_dir']`` to use a pre-downloaded
        copy, or ``config['auto_download'] = False`` to disable the download.
        If the data is unavailable, returns an empty chunk.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = await ensure_dataset(self.slug, self.config)

        if data_dir is None:
            logger.info(
                "caravan_no_data_dir",
                station=native_id,
                hint=(
                    "Caravan data unavailable (auto-download disabled or "
                    f"failed). Download from {_ZENODO_DOWNLOAD_URL}"
                ),
            )
            return self._empty_chunk(station_id)

        data_path = Path(data_dir)
        file_path = self._find_data_file(data_path, native_id)

        if file_path is None:
            logger.info(
                "caravan_file_not_found",
                station=native_id,
                data_dir=str(data_path),
                hint=(
                    f"Download Caravan CSV for basin {native_id} "
                    f"from {_ZENODO_DOWNLOAD_URL}"
                ),
            )
            return self._empty_chunk(station_id)

        start_aware = (
            start if start.tzinfo else start.replace(tzinfo=UTC)
        )
        end_aware = (
            end if end.tzinfo else end.replace(tzinfo=UTC)
        )

        observations = self._parse_csv_file(
            file_path, station_id, start_aware, end_aware,
        )

        logger.info(
            "caravan_observations_loaded",
            station=native_id,
            count=len(observations),
            file=str(file_path),
        )

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # ------------------------------------------------------------------
    # Zenodo metadata discovery
    # ------------------------------------------------------------------

    async def _fetch_stations_zenodo(self) -> list[Station]:
        """Fetch Zenodo record metadata and discover available files.

        This provides file-level discovery rather than per-station
        metadata.  Falls back to the seed list for actual station data.
        """
        try:
            resp = await self._get(
                f"/records/{_ZENODO_RECORD_ID}",
            )
            data = resp.json()
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch Zenodo record "
                f"{_ZENODO_RECORD_ID}: {exc}",
            ) from exc

        files = data.get("files", [])
        if not files:
            raise DataFormatError(
                self.slug,
                f"Zenodo record {_ZENODO_RECORD_ID} has no files",
            )

        logger.info(
            "caravan_zenodo_files_discovered",
            provider=self.slug,
            file_count=len(files),
        )

        # Zenodo metadata doesn't provide per-station info, so
        # fall back to seed stations after verifying the record
        return self._build_seed_stations()

    # ------------------------------------------------------------------
    # Seed catalogue
    # ------------------------------------------------------------------

    def _build_seed_stations(self) -> list[Station]:
        """Build Station objects from the curated seed list."""
        stations: list[Station] = []
        for entry in _SEED_STATIONS:
            stations.append(
                Station(
                    id=self._station_id(entry["id"]),
                    provider=self.slug,
                    native_id=entry["id"],
                    name=entry["name"],
                    latitude=float(str(entry["lat"])),
                    longitude=float(str(entry["lon"])),
                    country_code=entry["country"],
                    river=entry.get("river"),
                    catchment_area_km2=(
                        float(str(entry["area"]))
                        if entry.get("area") is not None
                        else None
                    ),
                )
            )
        return stations

    # ------------------------------------------------------------------
    # Local file parsing
    # ------------------------------------------------------------------

    def _find_data_file(
        self, data_dir: Path, basin_id: str,
    ) -> Path | None:
        """Locate Caravan CSV file for a given basin.

        Searches in multiple locations matching Caravan layout:
          {data_dir}/{basin_id}.csv
          {data_dir}/timeseries/csv/{basin_id}.csv
        """
        # Caravan ZIP naming: camels_us_01013500 -> camels/camels_01013500.csv
        # Map seed ID to Caravan directory and filename
        parts = basin_id.split("_")
        subset = "_".join(parts[:2]) if len(parts) >= 3 else ""
        # Caravan dirs: camels (US), camelsgb, camelsaus, camelsbr, hysets, lamah
        caravan_dir = parts[0] if parts[0] != "camels" else parts[0]
        if len(parts) >= 3 and parts[0] == "camels" and parts[1] != "us":
            caravan_dir = parts[0] + parts[1]  # camelsgb, camelsaus, camelsbr
        caravan_file = f"{caravan_dir}_{'_'.join(parts[2:])}" if len(parts) >= 3 else basin_id
        if len(parts) >= 3 and parts[0] == "camels" and parts[1] == "us":
            caravan_file = f"camels_{'_'.join(parts[2:])}"

        candidates = [
            data_dir / f"{basin_id}.csv",
            data_dir / "timeseries" / "csv" / f"{basin_id}.csv",
            data_dir / "timeseries" / "csv" / subset / f"{basin_id}.csv",
            data_dir / "Caravan" / "timeseries" / "csv" / caravan_dir / f"{caravan_file}.csv",
            data_dir / "Caravan" / "timeseries" / "csv" / subset / f"{basin_id}.csv",
        ]
        for candidate in candidates:
            if candidate.is_file():
                return candidate
        # The auto-downloaded Caravan.zip extracts to a nested tree
        # (Caravan/timeseries/csv/<source>/<file>.csv); search recursively
        # for either the raw basin id or the mapped Caravan filename.
        for name in (basin_id, caravan_file):
            match = next(
                (p for p in data_dir.rglob(f"{name}.csv") if p.is_file()),
                None,
            )
            if match is not None:
                return match
        return None

    def _parse_csv_file(
        self,
        file_path: Path,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse a Caravan CSV with date and streamflow columns.

        Expected columns: date, streamflow (mm/d or m3/s depending
        on the sub-dataset).
        """
        observations: list[Observation] = []

        try:
            text = file_path.read_text(encoding="utf-8")
        except OSError as exc:
            raise ConnectorError(
                self.slug,
                f"Cannot read file {file_path}: {exc}",
            ) from exc

        reader = csv.DictReader(io.StringIO(text))
        if reader.fieldnames is None:
            return observations

        field_map = {
            f.lower().strip(): f for f in reader.fieldnames
        }

        date_col = field_map.get("date") or field_map.get("time")
        value_col = (
            field_map.get("streamflow")
            or field_map.get("discharge")
            or field_map.get("streamflow_mmd")
            or field_map.get("discharge_m3s")
        )

        if not date_col or not value_col:
            return observations

        for row in reader:
            obs = self._parse_csv_row(
                row, date_col, value_col, station_id, start, end,
            )
            if obs is not None:
                observations.append(obs)

        return observations

    def _parse_csv_row(
        self,
        row: dict[str, str],
        date_col: str,
        value_col: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> Observation | None:
        """Parse a single CSV row into an Observation."""
        date_str = row.get(date_col, "").strip()
        value_str = row.get(value_col, "").strip()

        if not date_str:
            return None

        try:
            ts = datetime.strptime(date_str, "%Y-%m-%d").replace(
                tzinfo=UTC,
            )
        except ValueError:
            return None

        if ts < start or ts > end:
            return None

        discharge: float | None = None
        quality = QualityFlag.RAW

        if value_str:
            parsed = _safe_float(value_str)
            if parsed is not None:
                discharge = parsed
            else:
                quality = QualityFlag.MISSING

        return Observation(
            station_id=station_id,
            timestamp=ts,
            discharge_m3s=discharge,
            quality=quality,
        )

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    def _empty_chunk(self, station_id: str) -> TimeSeriesChunk:
        """Return an empty TimeSeriesChunk for a station."""
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )


@register("caravan_grdc")
class CaravanGRDCConnector(CaravanConnector):
    """Alias for Caravan GRDC-extension (Zenodo 15349031)."""
    slug = "caravan_grdc"
    display_name = "Caravan-GRDC extension (Global)"
    country_codes = ["global"]


# NOTE: camels_de is served by the AUTHORITATIVE standalone connector
# (connectors/camels_de.py — CAMELS-DE Zenodo archive), not a Caravan alias.
# camels_in / camels_co remain Caravan-derived until authoritative standalone
# connectors exist for them.


@register("camels_in")
class CAMELSINConnector(CaravanConnector):
    """Alias for Caravan v1.6 (India sub-dataset)."""
    slug = "camels_in"
    display_name = "CAMELS-IND (India)"
    country_codes = ["IN"]


@register("camels_co")
class CAMELSCOConnector(CaravanConnector):
    """Alias for Caravan v1.6 (Colombia sub-dataset)."""
    slug = "camels_co"
    display_name = "CAMELS-COL (Colombia)"
    country_codes = ["CO"]
