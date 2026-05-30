"""France Hub'Eau Hydrométrie API v2 connector."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

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

    # The real-time endpoint (observations_tr) only retains ~1 month of data
    # and returns HTTP 400 ("date can't be < 1 month from now") for any
    # date_debut_obs older than that. Use a safe margin under the limit and
    # route older windows to the elaborated/stored endpoint (obs_elab).
    _REALTIME_DEPTH_DAYS = 28
    _PAGE_SIZE = 5000

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge for a station across real-time and historical data.

        Hub'Eau splits discharge across two endpoints:

        * ``observations_tr`` — instantaneous "temps réel" values, retained for
          only ~1 month.
        * ``obs_elab`` — elaborated daily means (``QmnJ``) with full history.

        The requested window is split at a cutoff ~1 month before now: the
        recent part is served sub-daily from ``observations_tr`` and the older
        part as daily means from ``obs_elab``. The split point is exclusive on
        the historical side (``min(end, cutoff)``) so the two granularities
        don't overlap.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")
        start = start if start.tzinfo else start.replace(tzinfo=UTC)
        end = end if end.tzinfo else end.replace(tzinfo=UTC)

        cutoff = datetime.now(UTC) - timedelta(days=self._REALTIME_DEPTH_DAYS)
        observations: list[Observation] = []

        # Older than the cutoff → daily means from obs_elab.
        if start < cutoff:
            observations.extend(
                await self._fetch_elaborated(
                    native_id, station_id, start, min(end, cutoff),
                )
            )

        # Within the real-time depth → sub-daily values from observations_tr.
        if end >= cutoff:
            observations.extend(
                await self._fetch_realtime(
                    native_id, station_id, max(start, cutoff), end,
                )
            )

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    async def _fetch_realtime(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Fetch instantaneous discharge from the real-time endpoint."""
        observations: list[Observation] = []
        cursor = None

        while True:
            params: dict = {
                "code_entite": native_id,
                "grandeur_hydro": "Q",
                "date_debut_obs": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "date_fin_obs": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "size": self._PAGE_SIZE,
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

        return observations

    async def _fetch_elaborated(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Fetch daily-mean discharge (QmnJ) from the elaborated endpoint."""
        observations: list[Observation] = []
        cursor = None

        while True:
            params: dict = {
                "code_entite": native_id,
                "grandeur_hydro_elab": "QmnJ",
                "date_debut_obs_elab": start.strftime("%Y-%m-%d"),
                "date_fin_obs_elab": end.strftime("%Y-%m-%d"),
                "size": self._PAGE_SIZE,
                "sort": "asc",
            }
            if cursor:
                params["cursor"] = cursor

            resp = await self._get("/obs_elab", params=params)
            data = resp.json()
            items = data.get("data", [])
            if not items:
                break

            for item in items:
                val = item.get("resultat_obs_elab")
                # obs_elab exposes the qualification as `code_qualification`
                # (no `_obs_elab` suffix, unlike observations_tr).
                qual_code = item.get("code_qualification")
                observations.append(Observation(
                    station_id=station_id,
                    timestamp=datetime.fromisoformat(
                        item["date_obs_elab"]
                    ).replace(tzinfo=UTC),
                    discharge_m3s=float(val) / 1000.0 if val is not None else None,
                    quality=_QUAL_MAP.get(qual_code, QualityFlag.RAW) if val is not None else QualityFlag.MISSING,
                ))

            cursor = data.get("next")
            if not cursor:
                break

        return observations
