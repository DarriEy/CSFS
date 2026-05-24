"""France Hub'Eau Hydrométrie API v2 connector."""

from __future__ import annotations

from datetime import UTC, datetime

from csfs.connectors.base import BaseConnector
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

_QUAL_MAP = {
    16: QualityFlag.GOOD,       # Bon
    20: QualityFlag.GOOD,       # Bon
    12: QualityFlag.SUSPECT,    # Douteux
    8: QualityFlag.RAW,         # Brut
    4: QualityFlag.MISSING,     # Non qualifié
}


@register("france_hubeau")
class FranceHubEauConnector(BaseConnector):
    slug = "france_hubeau"
    display_name = "Hub'Eau Hydrométrie"
    base_url = "https://hubeau.eaufrance.fr/api/v2/hydrometrie"
    country_codes = ["FR"]

    async def fetch_stations(self) -> list[Station]:
        stations = []
        page = 1
        page_size = 200

        while True:
            resp = await self._get("/referentiel/stations", params={
                "size": page_size,
                "page": page,
                "en_service": "true",
                "format": "json",
            })
            data = resp.json()
            items = data.get("data", [])
            if not items:
                break

            for item in items:
                native_id = item.get("code_station", "")
                lat = item.get("latitude_station")
                lon = item.get("longitude_station")
                if not (native_id and lat and lon):
                    continue
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=item.get("libelle_station", native_id),
                    latitude=float(lat),
                    longitude=float(lon),
                    country_code="FR",
                    river=item.get("libelle_cours_eau"),
                ))

            if len(items) < page_size:
                break
            page += 1

        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        native_id = station_id.removeprefix(f"{self.slug}:")
        observations: list[Observation] = []
        cursor = None

        while True:
            params: dict = {
                "code_entite": native_id,
                "grandeur_hydro": "Q",
                "date_debut_obs": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "date_fin_obs": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "size": 5000,
                "sort": "asc",
            }
            if cursor:
                params["cursor"] = cursor

            resp = await self._get("/observations_tr", params=params)
            data = resp.json()
            items = data.get("data", [])
            if not items:
                break

            for item in items:
                val = item.get("resultat_obs")
                qual_code = item.get("code_qualification_obs")
                observations.append(Observation(
                    station_id=station_id,
                    timestamp=datetime.fromisoformat(item["date_obs"]),
                    discharge_m3s=float(val) / 1000.0 if val is not None else None,
                    quality=_QUAL_MAP.get(qual_code, QualityFlag.RAW) if val is not None else QualityFlag.MISSING,
                ))

            cursor = data.get("next")
            if not cursor:
                break

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )
