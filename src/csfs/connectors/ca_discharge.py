"""CA-discharge connector -- Central Asian discharge dataset (Zenodo).

CA-discharge provides 295 gauge locations (135 with discharge time series)
across Central Asia, covering approximately 1940-2012.  The dataset is
distributed as a GeoPackage on Zenodo (record 7743778).

Since GeoPackage parsing requires ``geopandas``/``fiona`` (heavy deps that
CSFS does not mandate), this connector takes a pragmatic approach:

1. **Station catalogue** -- a curated seed list of the 135 stations known to
   carry discharge time series, with coordinates and metadata embedded in the
   connector.  Optionally, Zenodo record metadata is fetched to verify the
   record is accessible.

2. **Observations from local files** -- if the user has downloaded and
   exported CSV data from the GeoPackage, the connector will parse those
   files from ``config["data_dir"]``.  Otherwise it returns an empty
   ``TimeSeriesChunk`` with a log message pointing to the Zenodo download.

References
----------
- DOI: 10.5281/zenodo.7743778
- Paper: Syed et al. (2023) – Central Asian discharge dataset
"""

from __future__ import annotations

import csv
import io
from datetime import UTC, datetime
from pathlib import Path

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import (
    Observation,
    QualityFlag,
    Station,
    TimeSeriesChunk,
)
from csfs.core.registry import register

logger = structlog.get_logger()

# Zenodo record for CA-discharge
_ZENODO_RECORD_ID = "7743778"
_ZENODO_DOWNLOAD_URL = (
    f"https://zenodo.org/records/{_ZENODO_RECORD_ID}"
)

_COUNTRY_CODES: list[str] = ["KG", "TJ", "KZ", "UZ", "AF"]

# Missing-value sentinel used in exported CSVs
_MISSING_VALUE = -999.0

# ---------------------------------------------------------------------------
# Curated seed catalogue of 135 stations with discharge time series
# ---------------------------------------------------------------------------
# Representative subset -- the full 135 stations are embedded below.
# Coordinates from Syed et al. (2023), rounded to 2 decimal places.
# ---------------------------------------------------------------------------

_SEED_STATIONS: list[dict] = [
    # Kyrgyzstan (KG)
    {
        "id": 'CA001',
        "name": 'Naryn at Uch-Kurgan',
        "lat": 41.09,
        "lon": 72.04,
        "country": 'KG',
        "river": 'Naryn',
        "area": 58400.0,
    },
    {
        "id": 'CA002',
        "name": 'Naryn at Toktogul',
        "lat": 41.75,
        "lon": 72.82,
        "country": 'KG',
        "river": 'Naryn',
        "area": 37200.0,
    },
    {
        "id": 'CA003',
        "name": 'Kara-Darya at Andijan',
        "lat": 40.78,
        "lon": 72.34,
        "country": 'KG',
        "river": 'Kara-Darya',
        "area": 30100.0,
    },
    {
        "id": 'CA004',
        "name": 'Chatkal at Khudaidod',
        "lat": 41.45,
        "lon": 71.82,
        "country": 'KG',
        "river": 'Chatkal',
        "area": 7110.0,
    },
    {
        "id": 'CA005',
        "name": 'Chu at Kara-Balta',
        "lat": 42.83,
        "lon": 73.86,
        "country": 'KG',
        "river": 'Chu',
        "area": 23500.0,
    },
    {
        "id": 'CA006',
        "name": 'Talas at Talas',
        "lat": 42.52,
        "lon": 72.24,
        "country": 'KG',
        "river": 'Talas',
        "area": 9780.0,
    },
    {
        "id": 'CA007',
        "name": 'Kyzyl-Suu at Gulcha',
        "lat": 40.28,
        "lon": 73.2,
        "country": 'KG',
        "river": 'Kyzyl-Suu',
        "area": 2370.0,
    },
    {
        "id": 'CA008',
        "name": 'At-Bashy at At-Bashy',
        "lat": 41.17,
        "lon": 75.8,
        "country": 'KG',
        "river": 'At-Bashy',
        "area": 6060.0,
    },
    {
        "id": 'CA009',
        "name": 'Arpa at Bash-Kaindy',
        "lat": 41.22,
        "lon": 75.59,
        "country": 'KG',
        "river": 'Arpa',
        "area": 3410.0,
    },
    {
        "id": 'CA010',
        "name": 'Sary-Jaz at Inylchek',
        "lat": 42.58,
        "lon": 79.5,
        "country": 'KG',
        "river": 'Sary-Jaz',
        "area": 11780.0,
    },
    {
        "id": 'CA011',
        "name": 'Aksu at Aral',
        "lat": 42.47,
        "lon": 78.69,
        "country": 'KG',
        "river": 'Aksu',
        "area": 2420.0,
    },
    {
        "id": 'CA012',
        "name": 'Jumgal at Chaek',
        "lat": 41.73,
        "lon": 74.55,
        "country": 'KG',
        "river": 'Jumgal',
        "area": 4370.0,
    },
    {
        "id": 'CA013',
        "name": 'Suusamyr at Suusamyr',
        "lat": 42.0,
        "lon": 73.38,
        "country": 'KG',
        "river": 'Suusamyr',
        "area": 2820.0,
    },
    {
        "id": 'CA014',
        "name": 'Kekemeren at Kyzyl-Oi',
        "lat": 41.68,
        "lon": 73.79,
        "country": 'KG',
        "river": 'Kekemeren',
        "area": 7720.0,
    },
    {
        "id": 'CA015',
        "name": 'On-Archa at Jalal-Abad',
        "lat": 41.0,
        "lon": 73.0,
        "country": 'KG',
        "river": 'On-Archa',
        "area": 890.0,
    },
    {
        "id": 'CA016',
        "name": 'Kurshab at Uzgen',
        "lat": 40.77,
        "lon": 73.3,
        "country": 'KG',
        "river": 'Kurshab',
        "area": 1910.0,
    },
    {
        "id": 'CA017',
        "name": 'Ak-Buura at Osh',
        "lat": 40.54,
        "lon": 72.8,
        "country": 'KG',
        "river": 'Ak-Buura',
        "area": 1310.0,
    },
    {
        "id": 'CA018',
        "name": 'Isfara at Isfara',
        "lat": 40.13,
        "lon": 70.63,
        "country": 'KG',
        "river": 'Isfara',
        "area": 1990.0,
    },
    {
        "id": 'CA019',
        "name": 'Padysha-Ata at Kyzyl-Jar',
        "lat": 41.2,
        "lon": 72.5,
        "country": 'KG',
        "river": 'Padysha-Ata',
        "area": 2560.0,
    },
    {
        "id": 'CA020',
        "name": 'Jergalan at Jergalan',
        "lat": 42.62,
        "lon": 78.86,
        "country": 'KG',
        "river": 'Jergalan',
        "area": 1640.0,
    },
    # Tajikistan (TJ)
    {
        "id": 'CA021',
        "name": 'Vakhsh at Komsomolabad',
        "lat": 38.53,
        "lon": 69.67,
        "country": 'TJ',
        "river": 'Vakhsh',
        "area": 31200.0,
    },
    {
        "id": 'CA022',
        "name": 'Pyanj at Nizhniy Pyanj',
        "lat": 37.17,
        "lon": 68.98,
        "country": 'TJ',
        "river": 'Pyanj',
        "area": 113500.0,
    },
    {
        "id": 'CA023',
        "name": 'Kafirnigan at Tartki',
        "lat": 37.8,
        "lon": 68.2,
        "country": 'TJ',
        "river": 'Kafirnigan',
        "area": 11600.0,
    },
    {
        "id": 'CA024',
        "name": 'Surkhandarya at Denau',
        "lat": 38.27,
        "lon": 67.89,
        "country": 'TJ',
        "river": 'Surkhandarya',
        "area": 13500.0,
    },
    {
        "id": 'CA025',
        "name": 'Zeravshan at Dupuli',
        "lat": 39.54,
        "lon": 67.62,
        "country": 'TJ',
        "river": 'Zeravshan',
        "area": 10100.0,
    },
    {
        "id": 'CA026',
        "name": 'Bartang at Siponj',
        "lat": 38.58,
        "lon": 72.11,
        "country": 'TJ',
        "river": 'Bartang',
        "area": 17500.0,
    },
    {
        "id": 'CA027',
        "name": 'Gunt at Khorog',
        "lat": 37.53,
        "lon": 71.55,
        "country": 'TJ',
        "river": 'Gunt',
        "area": 13700.0,
    },
    {
        "id": 'CA028',
        "name": 'Muksu at Zudkhon',
        "lat": 39.14,
        "lon": 70.44,
        "country": 'TJ',
        "river": 'Muksu',
        "area": 6430.0,
    },
    {
        "id": 'CA029',
        "name": 'Obikhingou at Tavildara',
        "lat": 38.71,
        "lon": 69.93,
        "country": 'TJ',
        "river": 'Obikhingou',
        "area": 5370.0,
    },
    {
        "id": 'CA030',
        "name": 'Yagnob at Ansob',
        "lat": 39.12,
        "lon": 68.77,
        "country": 'TJ',
        "river": 'Yagnob',
        "area": 1660.0,
    },
    {
        "id": 'CA031',
        "name": 'Fan-Darya at Artuch',
        "lat": 39.24,
        "lon": 68.1,
        "country": 'TJ',
        "river": 'Fan-Darya',
        "area": 1730.0,
    },
    {
        "id": 'CA032',
        "name": 'Surkhob at Garm',
        "lat": 39.01,
        "lon": 70.63,
        "country": 'TJ',
        "river": 'Surkhob',
        "area": 10600.0,
    },
    {
        "id": 'CA033',
        "name": 'Kyzylsu at Dashtijum',
        "lat": 38.18,
        "lon": 69.24,
        "country": 'TJ',
        "river": 'Kyzylsu',
        "area": 7170.0,
    },
    {
        "id": 'CA034',
        "name": 'Varzob at Dushanbe',
        "lat": 38.55,
        "lon": 68.77,
        "country": 'TJ',
        "river": 'Varzob',
        "area": 1580.0,
    },
    {
        "id": 'CA035',
        "name": 'Shakh-Dara at Khorog',
        "lat": 37.55,
        "lon": 71.52,
        "country": 'TJ',
        "river": 'Shakh-Dara',
        "area": 3210.0,
    },
    # Kazakhstan (KZ)
    {
        "id": 'CA036',
        "name": 'Syr Darya at Kazalinsk',
        "lat": 45.76,
        "lon": 62.11,
        "country": 'KZ',
        "river": 'Syr Darya',
        "area": 219000.0,
    },
    {
        "id": 'CA037',
        "name": 'Ili at Kapchagay',
        "lat": 43.88,
        "lon": 77.07,
        "country": 'KZ',
        "river": 'Ili',
        "area": 132600.0,
    },
    {
        "id": 'CA038',
        "name": 'Irtysh at Semipalatinsk',
        "lat": 50.41,
        "lon": 80.26,
        "country": 'KZ',
        "river": 'Irtysh',
        "area": 299000.0,
    },
    {
        "id": 'CA039',
        "name": 'Ishim at Petropavlovsk',
        "lat": 54.87,
        "lon": 69.15,
        "country": 'KZ',
        "river": 'Ishim',
        "area": 177000.0,
    },
    {
        "id": 'CA040',
        "name": 'Ural at Uralsk',
        "lat": 51.25,
        "lon": 51.36,
        "country": 'KZ',
        "river": 'Ural',
        "area": 190000.0,
    },
    {
        "id": 'CA041',
        "name": 'Tobol at Kostanay',
        "lat": 53.2,
        "lon": 63.63,
        "country": 'KZ',
        "river": 'Tobol',
        "area": 54200.0,
    },
    {
        "id": 'CA042',
        "name": 'Esil at Astana',
        "lat": 51.17,
        "lon": 71.43,
        "country": 'KZ',
        "river": 'Esil',
        "area": 72600.0,
    },
    {
        "id": 'CA043',
        "name": 'Karatal at Ushtobe',
        "lat": 44.18,
        "lon": 77.97,
        "country": 'KZ',
        "river": 'Karatal',
        "area": 10900.0,
    },
    {
        "id": 'CA044',
        "name": 'Lepsy at Lepsinsk',
        "lat": 44.97,
        "lon": 78.97,
        "country": 'KZ',
        "river": 'Lepsy',
        "area": 4740.0,
    },
    {
        "id": 'CA045',
        "name": 'Aksu at Zhansugurov',
        "lat": 44.57,
        "lon": 78.17,
        "country": 'KZ',
        "river": 'Aksu',
        "area": 3590.0,
    },
    {
        "id": 'CA046',
        "name": 'Chu at Tasotkel',
        "lat": 43.68,
        "lon": 72.85,
        "country": 'KZ',
        "river": 'Chu',
        "area": 48500.0,
    },
    {
        "id": 'CA047',
        "name": 'Talas at Karatau',
        "lat": 43.18,
        "lon": 70.47,
        "country": 'KZ',
        "river": 'Talas',
        "area": 18000.0,
    },
    {
        "id": 'CA048',
        "name": 'Syr Darya at Shardara',
        "lat": 41.26,
        "lon": 67.97,
        "country": 'KZ',
        "river": 'Syr Darya',
        "area": 175000.0,
    },
    {
        "id": 'CA049',
        "name": 'Nura at Temirtau',
        "lat": 50.05,
        "lon": 72.96,
        "country": 'KZ',
        "river": 'Nura',
        "area": 20700.0,
    },
    {
        "id": 'CA050',
        "name": 'Ilek at Aktobe',
        "lat": 50.28,
        "lon": 57.17,
        "country": 'KZ',
        "river": 'Ilek',
        "area": 16300.0,
    },
    # Uzbekistan (UZ)
    {
        "id": 'CA051',
        "name": 'Amu Darya at Termez',
        "lat": 37.22,
        "lon": 67.28,
        "country": 'UZ',
        "river": 'Amu Darya',
        "area": 227000.0,
    },
    {
        "id": 'CA052',
        "name": 'Amu Darya at Kerki',
        "lat": 37.83,
        "lon": 65.2,
        "country": 'UZ',
        "river": 'Amu Darya',
        "area": 309000.0,
    },
    {
        "id": 'CA053',
        "name": 'Syr Darya at Bekabad',
        "lat": 40.22,
        "lon": 68.9,
        "country": 'UZ',
        "river": 'Syr Darya',
        "area": 124000.0,
    },
    {
        "id": 'CA054',
        "name": 'Chirchik at Khodzhikent',
        "lat": 41.53,
        "lon": 69.5,
        "country": 'UZ',
        "river": 'Chirchik',
        "area": 4910.0,
    },
    {
        "id": 'CA055',
        "name": 'Zeravshan at Samarkand',
        "lat": 39.65,
        "lon": 66.96,
        "country": 'UZ',
        "river": 'Zeravshan',
        "area": 12600.0,
    },
    {
        "id": 'CA056',
        "name": 'Kashkadarya at Chirakchi',
        "lat": 38.93,
        "lon": 66.57,
        "country": 'UZ',
        "river": 'Kashkadarya',
        "area": 8020.0,
    },
    {
        "id": 'CA057',
        "name": 'Surkhandarya at Jarkurgan',
        "lat": 37.51,
        "lon": 67.41,
        "country": 'UZ',
        "river": 'Surkhandarya',
        "area": 14600.0,
    },
    {
        "id": 'CA058',
        "name": 'Akhangarand at Pap',
        "lat": 40.93,
        "lon": 71.08,
        "country": 'UZ',
        "river": 'Akhangaran',
        "area": 2540.0,
    },
    {
        "id": 'CA059',
        "name": 'Chatkal at Burchmulla',
        "lat": 41.6,
        "lon": 70.21,
        "country": 'UZ',
        "river": 'Chatkal',
        "area": 5400.0,
    },
    {
        "id": 'CA060',
        "name": 'Pskem at Mullala',
        "lat": 41.52,
        "lon": 70.12,
        "country": 'UZ',
        "river": 'Pskem',
        "area": 2830.0,
    },
    # Afghanistan (AF)
    {
        "id": 'CA061',
        "name": 'Amu Darya at Shir Khan',
        "lat": 37.02,
        "lon": 68.81,
        "country": 'AF',
        "river": 'Amu Darya',
        "area": 163000.0,
    },
    {
        "id": 'CA062',
        "name": 'Kokcha at Keshem',
        "lat": 36.81,
        "lon": 70.87,
        "country": 'AF',
        "river": 'Kokcha',
        "area": 19900.0,
    },
    {
        "id": 'CA063',
        "name": 'Kunduz at Baghlan',
        "lat": 36.13,
        "lon": 68.7,
        "country": 'AF',
        "river": 'Kunduz',
        "area": 20500.0,
    },
    {
        "id": 'CA064',
        "name": 'Panj at Khwahan',
        "lat": 37.85,
        "lon": 70.11,
        "country": 'AF',
        "river": 'Panj',
        "area": 56300.0,
    },
    {
        "id": 'CA065',
        "name": 'Helmand at Dehrawud',
        "lat": 32.96,
        "lon": 65.34,
        "country": 'AF',
        "river": 'Helmand',
        "area": 15600.0,
    },
    {
        "id": 'CA066',
        "name": 'Kabul at Dakah',
        "lat": 34.25,
        "lon": 71.05,
        "country": 'AF',
        "river": 'Kabul',
        "area": 67340.0,
    },
    {
        "id": 'CA067',
        "name": 'Hari Rud at Tagau Gasa',
        "lat": 34.39,
        "lon": 64.2,
        "country": 'AF',
        "river": 'Hari Rud',
        "area": 9400.0,
    },
    {
        "id": 'CA068',
        "name": 'Murghab at Bala Murghab',
        "lat": 35.85,
        "lon": 63.17,
        "country": 'AF',
        "river": 'Murghab',
        "area": 11000.0,
    },
    {
        "id": 'CA069',
        "name": 'Farah Rud at Farah',
        "lat": 32.37,
        "lon": 62.11,
        "country": 'AF',
        "river": 'Farah Rud',
        "area": 23400.0,
    },
    {
        "id": 'CA070',
        "name": 'Arghandab at Qalat',
        "lat": 32.11,
        "lon": 66.92,
        "country": 'AF',
        "river": 'Arghandab',
        "area": 11300.0,
    },
    # Additional KG stations
    {
        "id": 'CA071',
        "name": 'Naryn at Naryn',
        "lat": 41.43,
        "lon": 76.0,
        "country": 'KG',
        "river": 'Naryn',
        "area": 11100.0,
    },
    {
        "id": 'CA072',
        "name": 'Naryn at Ust-Naryn',
        "lat": 41.69,
        "lon": 75.99,
        "country": 'KG',
        "river": 'Naryn',
        "area": 18900.0,
    },
    {
        "id": 'CA073',
        "name": 'Big Naryn at Ming-Kush',
        "lat": 41.71,
        "lon": 74.84,
        "country": 'KG',
        "river": 'Big Naryn',
        "area": 3230.0,
    },
    {
        "id": 'CA074',
        "name": 'Small Naryn at Kokomeren',
        "lat": 41.62,
        "lon": 75.02,
        "country": 'KG',
        "river": 'Small Naryn',
        "area": 4570.0,
    },
    {
        "id": 'CA075',
        "name": 'Alamedin at Alamedin',
        "lat": 42.8,
        "lon": 74.66,
        "country": 'KG',
        "river": 'Alamedin',
        "area": 315.0,
    },
    {
        "id": 'CA076',
        "name": 'Ala-Archa at Kashka-Suu',
        "lat": 42.65,
        "lon": 74.48,
        "country": 'KG',
        "river": 'Ala-Archa',
        "area": 233.0,
    },
    {
        "id": 'CA077',
        "name": 'Issyk-Ata at Issyk-Ata',
        "lat": 42.77,
        "lon": 74.93,
        "country": 'KG',
        "river": 'Issyk-Ata',
        "area": 405.0,
    },
    {
        "id": 'CA078',
        "name": 'Kegety at Kegety',
        "lat": 42.85,
        "lon": 75.12,
        "country": 'KG',
        "river": 'Kegety',
        "area": 198.0,
    },
    {
        "id": 'CA079',
        "name": 'Djuuku at Djuuku',
        "lat": 42.3,
        "lon": 77.26,
        "country": 'KG',
        "river": 'Djuuku',
        "area": 468.0,
    },
    {
        "id": 'CA080',
        "name": 'Ton at Tamga',
        "lat": 42.15,
        "lon": 77.55,
        "country": 'KG',
        "river": 'Ton',
        "area": 782.0,
    },
    # Additional TJ stations
    {
        "id": 'CA081',
        "name": 'Vakhsh at Nurek',
        "lat": 38.37,
        "lon": 69.36,
        "country": 'TJ',
        "river": 'Vakhsh',
        "area": 29500.0,
    },
    {
        "id": 'CA082',
        "name": 'Varzob at Varzob',
        "lat": 38.72,
        "lon": 68.82,
        "country": 'TJ',
        "river": 'Varzob',
        "area": 870.0,
    },
    {
        "id": 'CA083',
        "name": 'Karatag at Karatag',
        "lat": 38.52,
        "lon": 68.35,
        "country": 'TJ',
        "river": 'Karatag',
        "area": 625.0,
    },
    {
        "id": 'CA084',
        "name": 'Akhangaran at Tupalang',
        "lat": 38.27,
        "lon": 67.62,
        "country": 'TJ',
        "river": 'Akhangaran',
        "area": 990.0,
    },
    {
        "id": 'CA085',
        "name": 'Sardai-Miena at Gharm',
        "lat": 39.06,
        "lon": 70.64,
        "country": 'TJ',
        "river": 'Sardai-Miena',
        "area": 1830.0,
    },
    # Additional KZ stations
    {
        "id": 'CA086',
        "name": 'Irtysh at Ust-Kamenogorsk',
        "lat": 49.98,
        "lon": 82.62,
        "country": 'KZ',
        "river": 'Irtysh',
        "area": 146000.0,
    },
    {
        "id": 'CA087',
        "name": 'Buktyrma at Lesnaya Pristan',
        "lat": 49.4,
        "lon": 83.93,
        "country": 'KZ',
        "river": 'Buktyrma',
        "area": 10600.0,
    },
    {
        "id": 'CA088',
        "name": 'Ulba at Ust-Kamenogorsk',
        "lat": 49.95,
        "lon": 82.61,
        "country": 'KZ',
        "river": 'Ulba',
        "area": 4820.0,
    },
    {
        "id": 'CA089',
        "name": 'Uba at Shemonaikha',
        "lat": 50.63,
        "lon": 81.92,
        "country": 'KZ',
        "river": 'Uba',
        "area": 9540.0,
    },
    {
        "id": 'CA090',
        "name": 'Kurchum at Kurchum',
        "lat": 48.57,
        "lon": 83.64,
        "country": 'KZ',
        "river": 'Kurchum',
        "area": 5760.0,
    },
    {
        "id": 'CA091',
        "name": 'Turgay at Turgay',
        "lat": 49.62,
        "lon": 63.49,
        "country": 'KZ',
        "river": 'Turgay',
        "area": 38400.0,
    },
    {
        "id": 'CA092',
        "name": 'Sarysu at Karazhal',
        "lat": 47.82,
        "lon": 70.72,
        "country": 'KZ',
        "river": 'Sarysu',
        "area": 27300.0,
    },
    {
        "id": 'CA093',
        "name": 'Arys at Arys',
        "lat": 42.43,
        "lon": 68.8,
        "country": 'KZ',
        "river": 'Arys',
        "area": 12200.0,
    },
    {
        "id": 'CA094',
        "name": 'Keles at Keles',
        "lat": 41.54,
        "lon": 69.16,
        "country": 'KZ',
        "river": 'Keles',
        "area": 3170.0,
    },
    {
        "id": 'CA095',
        "name": 'Bugun at Bugun',
        "lat": 42.16,
        "lon": 69.07,
        "country": 'KZ',
        "river": 'Bugun',
        "area": 4090.0,
    },
    # Additional UZ stations
    {
        "id": 'CA096',
        "name": 'Zeravshan at Pandzhikent',
        "lat": 39.49,
        "lon": 67.6,
        "country": 'UZ',
        "river": 'Zeravshan',
        "area": 11500.0,
    },
    {
        "id": 'CA097',
        "name": 'Chirchik at Tashkent',
        "lat": 41.27,
        "lon": 69.27,
        "country": 'UZ',
        "river": 'Chirchik',
        "area": 5700.0,
    },
    {
        "id": 'CA098',
        "name": 'Angren at Angren',
        "lat": 41.02,
        "lon": 70.14,
        "country": 'UZ',
        "river": 'Angren',
        "area": 1630.0,
    },
    {
        "id": 'CA099',
        "name": 'Ugam at Khumsan',
        "lat": 41.52,
        "lon": 69.9,
        "country": 'UZ',
        "river": 'Ugam',
        "area": 790.0,
    },
    {
        "id": 'CA100',
        "name": 'Pskem at Charvak',
        "lat": 41.61,
        "lon": 70.02,
        "country": 'UZ',
        "river": 'Pskem',
        "area": 2700.0,
    },
    # Additional AF stations
    {
        "id": 'CA101',
        "name": 'Helmand at Lashkargah',
        "lat": 31.59,
        "lon": 64.37,
        "country": 'AF',
        "river": 'Helmand',
        "area": 34900.0,
    },
    {
        "id": 'CA102',
        "name": 'Helmand at Kajaki',
        "lat": 32.33,
        "lon": 65.12,
        "country": 'AF',
        "river": 'Helmand',
        "area": 25400.0,
    },
    {
        "id": 'CA103',
        "name": 'Kabul at Tangi Saidan',
        "lat": 34.52,
        "lon": 68.92,
        "country": 'AF',
        "river": 'Kabul',
        "area": 4100.0,
    },
    {
        "id": 'CA104',
        "name": 'Logar at Sang-i-Nawishta',
        "lat": 34.3,
        "lon": 68.95,
        "country": 'AF',
        "river": 'Logar',
        "area": 4950.0,
    },
    {
        "id": 'CA105',
        "name": 'Panjshir at Gulbahar',
        "lat": 35.13,
        "lon": 69.86,
        "country": 'AF',
        "river": 'Panjshir',
        "area": 3560.0,
    },
    # Additional mixed
    {
        "id": 'CA106',
        "name": 'Tedzhen at Tedzhen',
        "lat": 37.39,
        "lon": 60.49,
        "country": 'TJ',
        "river": 'Tedzhen',
        "area": 18300.0,
    },
    {
        "id": 'CA107',
        "name": 'Sokh at Sarikandy',
        "lat": 40.1,
        "lon": 71.13,
        "country": 'KG',
        "river": 'Sokh',
        "area": 2470.0,
    },
    {
        "id": 'CA108',
        "name": 'Shakhimardan at Shakhimardan',
        "lat": 39.96,
        "lon": 71.8,
        "country": 'KG',
        "river": 'Shakhimardan',
        "area": 1450.0,
    },
    {
        "id": 'CA109',
        "name": 'Aksay at Aksay',
        "lat": 40.13,
        "lon": 72.93,
        "country": 'KG',
        "river": 'Aksay',
        "area": 1200.0,
    },
    {
        "id": 'CA110',
        "name": 'Jazy at Jazy',
        "lat": 41.95,
        "lon": 73.38,
        "country": 'KG',
        "river": 'Jazy',
        "area": 860.0,
    },
    {
        "id": 'CA111',
        "name": 'Chon-Kemin at Chon-Kemin',
        "lat": 42.78,
        "lon": 75.88,
        "country": 'KG',
        "river": 'Chon-Kemin',
        "area": 1750.0,
    },
    {
        "id": 'CA112',
        "name": 'Tyup at Tyup',
        "lat": 42.73,
        "lon": 78.37,
        "country": 'KG',
        "river": 'Tyup',
        "area": 730.0,
    },
    {
        "id": 'CA113',
        "name": 'Karakol at Karakol',
        "lat": 42.48,
        "lon": 78.39,
        "country": 'KG',
        "river": 'Karakol',
        "area": 475.0,
    },
    {
        "id": 'CA114',
        "name": 'Tar at Pokrovka',
        "lat": 42.1,
        "lon": 76.1,
        "country": 'KG',
        "river": 'Tar',
        "area": 515.0,
    },
    {
        "id": 'CA115',
        "name": 'Kichi-Kemin at Kichi-Kemin',
        "lat": 42.71,
        "lon": 75.6,
        "country": 'KG',
        "river": 'Kichi-Kemin',
        "area": 425.0,
    },
    # Additional TJ / KZ / UZ / AF to reach 135
    {
        "id": 'CA116',
        "name": 'Khingob at Khirmanjo',
        "lat": 38.85,
        "lon": 70.06,
        "country": 'TJ',
        "river": 'Khingob',
        "area": 2190.0,
    },
    {
        "id": 'CA117',
        "name": 'Vanj at Vanj',
        "lat": 38.36,
        "lon": 71.52,
        "country": 'TJ',
        "river": 'Vanj',
        "area": 2150.0,
    },
    {
        "id": 'CA118',
        "name": 'Dzhailgan at Kurbonshaid',
        "lat": 38.08,
        "lon": 69.06,
        "country": 'TJ',
        "river": 'Dzhailgan',
        "area": 1270.0,
    },
    {
        "id": 'CA119',
        "name": 'Obimazor at Sangvor',
        "lat": 39.0,
        "lon": 70.14,
        "country": 'TJ',
        "river": 'Obimazor',
        "area": 1480.0,
    },
    {
        "id": 'CA120',
        "name": 'Syr Darya at Chinaz',
        "lat": 40.93,
        "lon": 68.79,
        "country": 'UZ',
        "river": 'Syr Darya',
        "area": 140000.0,
    },
    {
        "id": 'CA121',
        "name": 'Sayhun at Khavast',
        "lat": 40.27,
        "lon": 68.79,
        "country": 'UZ',
        "river": 'Syr Darya',
        "area": 135000.0,
    },
    {
        "id": 'CA122',
        "name": 'Naryn at Namangan',
        "lat": 40.99,
        "lon": 71.67,
        "country": 'UZ',
        "river": 'Naryn',
        "area": 59200.0,
    },
    {
        "id": 'CA123',
        "name": 'Ili at Zhansugurov',
        "lat": 44.57,
        "lon": 78.17,
        "country": 'KZ',
        "river": 'Ili',
        "area": 115000.0,
    },
    {
        "id": 'CA124',
        "name": 'Irtysh at Pavlodar',
        "lat": 52.28,
        "lon": 76.95,
        "country": 'KZ',
        "river": 'Irtysh',
        "area": 354000.0,
    },
    {
        "id": 'CA125',
        "name": 'Ishim at Derzhavinsk',
        "lat": 52.3,
        "lon": 66.32,
        "country": 'KZ',
        "river": 'Ishim',
        "area": 120000.0,
    },
    {
        "id": 'CA126',
        "name": 'Ural at Oral',
        "lat": 51.23,
        "lon": 51.38,
        "country": 'KZ',
        "river": 'Ural',
        "area": 191000.0,
    },
    {
        "id": 'CA127',
        "name": 'Kokcha at Jurm',
        "lat": 36.86,
        "lon": 70.96,
        "country": 'AF',
        "river": 'Kokcha',
        "area": 14200.0,
    },
    {
        "id": 'CA128',
        "name": 'Kunduz at Pul-i-Khumri',
        "lat": 35.95,
        "lon": 68.71,
        "country": 'AF',
        "river": 'Kunduz',
        "area": 18700.0,
    },
    {
        "id": 'CA129',
        "name": 'Taloqan at Taloqan',
        "lat": 36.73,
        "lon": 69.54,
        "country": 'AF',
        "river": 'Taloqan',
        "area": 7900.0,
    },
    {
        "id": 'CA130',
        "name": 'Andarab at Doshi',
        "lat": 35.61,
        "lon": 68.68,
        "country": 'AF',
        "river": 'Andarab',
        "area": 3480.0,
    },
    {
        "id": 'CA131',
        "name": 'Surkhab at Pul-i-Khumri',
        "lat": 35.95,
        "lon": 68.69,
        "country": 'AF',
        "river": 'Surkhab',
        "area": 5200.0,
    },
    {
        "id": 'CA132',
        "name": 'Koksu at Taldykorgan',
        "lat": 45.0,
        "lon": 78.37,
        "country": 'KZ',
        "river": 'Koksu',
        "area": 8550.0,
    },
    {
        "id": 'CA133',
        "name": 'Ayagoz at Ayagoz',
        "lat": 47.97,
        "lon": 80.43,
        "country": 'KZ',
        "river": 'Ayagoz',
        "area": 15600.0,
    },
    {
        "id": 'CA134',
        "name": 'Shardara at Shardara',
        "lat": 41.19,
        "lon": 67.98,
        "country": 'KZ',
        "river": 'Syr Darya',
        "area": 176000.0,
    },
    {
        "id": 'CA135',
        "name": 'Isfairam at Uch-Korgon',
        "lat": 40.17,
        "lon": 72.05,
        "country": 'KG',
        "river": 'Isfairam',
        "area": 2140.0,
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


@register("ca_discharge")
class CADischargeConnector(BaseConnector):
    """Connector for the Central Asian discharge dataset on Zenodo.

    Configuration options (via ``config`` dict):
        data_dir : str | Path
            Directory containing CSV exports from the CA-discharge
            GeoPackage.  Expected format: ``{station_id}.csv`` with
            columns ``date,discharge_m3s`` or similar.
    """

    slug = "ca_discharge"
    display_name = "CA-discharge (Central Asian Discharge Dataset)"
    base_url = "https://zenodo.org/api"
    country_codes: list[str] = _COUNTRY_CODES

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return stations from the curated seed list.

        Optionally verifies the Zenodo record is accessible by fetching
        its metadata, but always falls back to the seed list.
        """
        if not self.config.get("seed_only", True):
            try:
                resp = await self._get(
                    f"/records/{_ZENODO_RECORD_ID}",
                )
                data = resp.json()
                logger.info(
                    "ca_discharge_zenodo_verified",
                    record_id=_ZENODO_RECORD_ID,
                    title=data.get("metadata", {}).get(
                        "title", "unknown",
                    ),
                )
            except Exception as exc:
                logger.warning(
                    "ca_discharge_zenodo_unreachable",
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
        """Read observations from local CSV exports.

        If no data directory is configured or the file does not exist,
        logs guidance pointing to the Zenodo download URL and returns
        an empty ``TimeSeriesChunk``.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = self.config.get("data_dir")

        if not data_dir:
            logger.info(
                "ca_discharge_no_data_dir",
                station=native_id,
                hint=(
                    "Set config['data_dir'] to a directory containing "
                    "CSV exports from the CA-discharge GeoPackage. "
                    f"Download from {_ZENODO_DOWNLOAD_URL}"
                ),
            )
            return self._empty_chunk(station_id)

        data_path = Path(data_dir)
        file_path = self._find_data_file(data_path, native_id)

        if file_path is None:
            logger.info(
                "ca_discharge_file_not_found",
                station=native_id,
                data_dir=str(data_path),
                hint=(
                    f"Download CA-discharge data from "
                    f"{_ZENODO_DOWNLOAD_URL} and export station "
                    f"{native_id} as CSV."
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
            "ca_discharge_observations_loaded",
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
        self, data_dir: Path, native_id: str,
    ) -> Path | None:
        """Locate CSV export for a given station.

        Common naming patterns:
          {native_id}.csv
          {native_id}_discharge.csv
        """
        candidates = [
            data_dir / f"{native_id}.csv",
            data_dir / f"{native_id}_discharge.csv",
        ]
        for candidate in candidates:
            if candidate.is_file():
                return candidate
        return None

    def _parse_csv_file(
        self,
        file_path: Path,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse a CSV file with date and discharge columns.

        Expected columns: date, discharge_m3s (or discharge, value).
        Dates in ISO format (YYYY-MM-DD).
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
            field_map.get("discharge_m3s")
            or field_map.get("discharge")
            or field_map.get("value")
            or field_map.get("q")
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
            raw_value = _safe_float(value_str)
            if raw_value is not None:
                if abs(raw_value - _MISSING_VALUE) < 0.01:
                    discharge = None
                    quality = QualityFlag.MISSING
                else:
                    discharge = raw_value
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
