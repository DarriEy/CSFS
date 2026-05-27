"""EStreams connector -- European streamflow dataset (Zenodo).

EStreams covers 17,130 catchments across 41 European countries with up to
120 years of data (Serrano-Notivoli et al., 2024).  This connector serves
countries that lack a dedicated national connector in CSFS: Luxembourg,
Albania, Montenegro, and North Macedonia.

The station catalogue is provided as a curated seed list since the
EStreams Zenodo archive (record 13154470) is distributed as a single
11 GB ZIP.  ``fetch_observations`` returns an empty ``TimeSeriesChunk``
because EStreams provides streamflow indices, not raw daily discharge.

References
----------
- DOI: 10.5281/zenodo.13154470
- Paper: https://doi.org/10.1038/s41597-024-03706-1
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.models import Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

_ZENODO_RECORD_URL = "https://zenodo.org/records/13154470"

# Curated seed stations for countries without a dedicated CSFS connector.
# Source: EStreams v1.0 catalogue (Serrano-Notivoli et al., 2024).
_SEED_STATIONS: list[dict] = [
    # Luxembourg (LU)
    {"id": "LU_0001", "name": "Esch-sur-Sûre", "lat": 49.89, "lon": 5.93, "country": "LU", "river": "Sûre", "area": 407.0},
    {"id": "LU_0002", "name": "Diekirch", "lat": 49.87, "lon": 6.16, "country": "LU", "river": "Sûre", "area": 721.0},
    {"id": "LU_0003", "name": "Ettelbruck", "lat": 49.85, "lon": 6.10, "country": "LU", "river": "Alzette", "area": 289.0},
    {"id": "LU_0004", "name": "Hesperange", "lat": 49.57, "lon": 6.15, "country": "LU", "river": "Alzette", "area": 197.0},
    {"id": "LU_0005", "name": "Mersch", "lat": 49.75, "lon": 6.11, "country": "LU", "river": "Alzette", "area": 157.0},
    {"id": "LU_0006", "name": "Pfaffenthal", "lat": 49.62, "lon": 6.13, "country": "LU", "river": "Alzette", "area": 233.0},
    {"id": "LU_0007", "name": "Bissen", "lat": 49.78, "lon": 6.05, "country": "LU", "river": "Attert", "area": 252.0},
    {"id": "LU_0008", "name": "Steinsel", "lat": 49.67, "lon": 6.12, "country": "LU", "river": "Alzette", "area": 213.0},
    # Albania (AL)
    {"id": "AL_0001", "name": "Drini at Kukës", "lat": 42.08, "lon": 20.42, "country": "AL", "river": "Drini", "area": 4675.0},
    {"id": "AL_0002", "name": "Vjosa at Përmet", "lat": 40.23, "lon": 20.35, "country": "AL", "river": "Vjosa", "area": 1520.0},
    {"id": "AL_0003", "name": "Devoll at Banjë", "lat": 40.73, "lon": 20.53, "country": "AL", "river": "Devoll", "area": 1715.0},
    {"id": "AL_0004", "name": "Osumi at Çorovodë", "lat": 40.50, "lon": 20.23, "country": "AL", "river": "Osumi", "area": 945.0},
    {"id": "AL_0005", "name": "Shkumbini at Librazhd", "lat": 41.18, "lon": 20.32, "country": "AL", "river": "Shkumbini", "area": 688.0},
    {"id": "AL_0006", "name": "Mati at Ulëz", "lat": 41.62, "lon": 20.08, "country": "AL", "river": "Mati", "area": 1570.0},
    {"id": "AL_0007", "name": "Erzen at Ndroq", "lat": 41.32, "lon": 19.71, "country": "AL", "river": "Erzen", "area": 668.0},
    {"id": "AL_0008", "name": "Seman at Berat", "lat": 40.70, "lon": 19.85, "country": "AL", "river": "Seman", "area": 4730.0},
    # Montenegro (ME)
    {"id": "ME_0001", "name": "Morača at Podgorica", "lat": 42.44, "lon": 19.26, "country": "ME", "river": "Morača", "area": 2608.0},
    {"id": "ME_0002", "name": "Tara at Kolašin", "lat": 42.82, "lon": 19.52, "country": "ME", "river": "Tara", "area": 437.0},
    {"id": "ME_0003", "name": "Lim at Bijelo Polje", "lat": 43.04, "lon": 19.75, "country": "ME", "river": "Lim", "area": 1335.0},
    {"id": "ME_0004", "name": "Zeta at Danilovgrad", "lat": 42.55, "lon": 19.09, "country": "ME", "river": "Zeta", "area": 1216.0},
    {"id": "ME_0005", "name": "Ćehotina at Pljevlja", "lat": 43.35, "lon": 19.35, "country": "ME", "river": "Ćehotina", "area": 517.0},
    {"id": "ME_0006", "name": "Piva at Šćepan Polje", "lat": 43.28, "lon": 18.78, "country": "ME", "river": "Piva", "area": 1784.0},
    # North Macedonia (MK)
    {"id": "MK_0001", "name": "Vardar at Skopje", "lat": 41.99, "lon": 21.43, "country": "MK", "river": "Vardar", "area": 4585.0},
    {"id": "MK_0002", "name": "Vardar at Gevgelija", "lat": 41.14, "lon": 22.50, "country": "MK", "river": "Vardar", "area": 24236.0},
    {"id": "MK_0003", "name": "Treska at Makedonski Brod", "lat": 41.51, "lon": 21.25, "country": "MK", "river": "Treska", "area": 2030.0},
    {"id": "MK_0004", "name": "Crna at Novaci", "lat": 41.05, "lon": 21.46, "country": "MK", "river": "Crna", "area": 2260.0},
    {"id": "MK_0005", "name": "Bregalnica at Štip", "lat": 41.73, "lon": 22.19, "country": "MK", "river": "Bregalnica", "area": 1800.0},
    {"id": "MK_0006", "name": "Pčinja at Katlanovska Banja", "lat": 41.89, "lon": 21.63, "country": "MK", "river": "Pčinja", "area": 2820.0},
]


@register("estreams")
class EStreamsConnector(BaseConnector):
    """Connector for EStreams European streamflow catalogue on Zenodo."""

    slug = "estreams"
    display_name = "EStreams (European Streamflow Dataset)"
    base_url = "https://zenodo.org/api"
    country_codes = ["LU", "AL", "ME", "MK"]

    async def fetch_stations(self) -> list[Station]:
        """Return curated seed stations for target countries."""
        stations: list[Station] = []
        for s in _SEED_STATIONS:
            stations.append(Station(
                id=self._station_id(s["id"]),
                provider=self.slug,
                native_id=s["id"],
                name=s["name"],
                latitude=s["lat"],
                longitude=s["lon"],
                country_code=s["country"],
                river=s.get("river"),
                catchment_area_km2=s.get("area"),
            ))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Return an empty chunk -- EStreams provides indices, not raw Q."""
        logger.info(
            "estreams_no_raw_observations",
            provider=self.slug,
            station=station_id,
            detail=(
                "EStreams provides streamflow indices, not raw daily "
                "discharge. See " + _ZENODO_RECORD_URL
            ),
        )
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )
