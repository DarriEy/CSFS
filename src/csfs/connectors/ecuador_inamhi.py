# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Ecuador INAMHI connector via GEOGLOWS ECMWF V2 Streamflow Services.

GEOGLOWS publishes MODELED river discharge keyed by a *river reach id*
(``river_id`` / LINKNO), not a gauge network. This connector exposes a
curated set of major Ecuadorian river reaches as virtual stations and fetches
GEOGLOWS V2 retrospective (daily) discharge -- the same backend as the global
``geoglows`` connector, scoped to Ecuador.

The relevant V2 endpoints (open, keyless)::

    GET /api/v2/retrospectivedaily/<river_id>?format=json
            &start_date=YYYYMMDD&end_date=YYYYMMDD
        -> {"<river_id>": [q, ...], "datetime": [iso, ...], "metadata": {...}}
    GET /api/v2/forecast/<river_id>?format=json
        -> {"datetime": [iso, ...], "flow_median": [q, ...], ...}

Discharge is m3/s. Because GEOGLOWS is model output (a simulated retrospective
reanalysis + forecast), observations are flagged ``ESTIMATED``.

The reach ids below were discovered live via the V2 ``getriverid`` endpoint at
each river's outlet point (the deprecated V1 COMIDs the connector previously
used are NOT valid V2 river_ids).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# Curated GEOGLOWS V2 reaches acting as virtual stations for major Ecuadorian
# rivers. ``id`` is the native GEOGLOWS V2 river_id; lat/lon are the snap point
# used to discover the reach. Operators can override via config
# ``virtual_stations`` (list of dicts with at least ``id``).
_ECUADOR_SEED_STATIONS: list[dict] = [
    {"id": "670049564", "name": "Guayas at Daule", "lat": -1.86, "lon": -79.97, "river": "Guayas"},
    {"id": "670065947", "name": "Guayas at Babahoyo", "lat": -1.80, "lon": -79.53, "river": "Guayas"},
    {"id": "670061841", "name": "Guayas at Vinces", "lat": -1.55, "lon": -79.75, "river": "Guayas"},
    {"id": "621061200", "name": "Napo at Francisco de Orellana", "lat": -0.47, "lon": -76.97, "river": "Napo"},
    {"id": "620649808", "name": "Napo at Tena", "lat": -1.00, "lon": -77.81, "river": "Napo"},
    {"id": "621014444", "name": "Napo at Nuevo Rocafuerte", "lat": -0.92, "lon": -75.39, "river": "Napo"},
    {"id": "620960804", "name": "Pastaza at Banos", "lat": -1.39, "lon": -78.42, "river": "Pastaza"},
    {"id": "620941545", "name": "Pastaza at Shell", "lat": -1.50, "lon": -78.06, "river": "Pastaza"},
    {"id": "621061290", "name": "Pastaza at Copataza", "lat": -2.13, "lon": -76.88, "river": "Pastaza"},
    {"id": "620618267", "name": "Santiago at Santiago", "lat": -3.05, "lon": -78.35, "river": "Santiago"},
    {"id": "620980200", "name": "Santiago at Yantzaza", "lat": -3.83, "lon": -78.76, "river": "Santiago"},
    {"id": "620775136", "name": "Santiago at Yaupi", "lat": -3.11, "lon": -77.94, "river": "Santiago"},
    {"id": "670088398", "name": "Esmeraldas at Quininde", "lat": 0.33, "lon": -79.47, "river": "Esmeraldas"},
    {"id": "670067894", "name": "Esmeraldas at Esmeraldas", "lat": 0.96, "lon": -79.65, "river": "Esmeraldas"},
    {"id": "670029140", "name": "Jubones at Pasaje", "lat": -3.33, "lon": -79.81, "river": "Jubones"},
    {"id": "670098675", "name": "Chone at Chone", "lat": -0.69, "lon": -80.10, "river": "Chone"},
    {"id": "620969060", "name": "Curaray at Curaray", "lat": -1.38, "lon": -76.95, "river": "Curaray"},
    {"id": "670053545", "name": "Mira at San Lorenzo", "lat": 1.28, "lon": -78.84, "river": "Mira"},
    {"id": "620934803", "name": "Zamora at Zamora", "lat": -4.07, "lon": -78.96, "river": "Zamora"},
]


@register("ecuador_inamhi")
class EcuadorINAMHIConnector(BaseConnector):
    """Connector for Ecuador streamflow via GEOGLOWS ECMWF V2.

    GEOGLOWS is a reach-based hydrological model, not a gauge network, so this
    connector exposes a curated set of Ecuadorian mainstem reaches as virtual
    stations. Discharge is GEOGLOWS V2 retrospective (daily) or forecast (median
    ensemble) in m3/s, flagged ``ESTIMATED`` (model output).

    Configuration options:
        virtual_stations : list[dict]
            Override the built-in reaches. Each item must provide ``id`` (the
            GEOGLOWS V2 river_id) and may provide ``name``, ``lat``, ``lon``,
            ``river``.
    """

    slug = "ecuador_inamhi"
    display_name = "Ecuador INAMHI (GEOGLOWS)"
    base_url = "https://geoglows.ecmwf.int/api/v2"
    country_codes = ["EC"]
    # The shared GEOGLOWS data host rate-limits aggressive callers.
    max_concurrent_requests = 2

    def _reaches(self) -> list[dict]:
        """Return the active reach definitions (config override or built-in)."""
        configured = self.config.get("virtual_stations")
        return configured if configured else _ECUADOR_SEED_STATIONS

    async def fetch_stations(self) -> list[Station]:
        """Return the curated Ecuadorian reaches as virtual stations."""
        stations: list[Station] = []
        for reach in self._reaches():
            try:
                native_id = str(reach["id"]).strip()
            except (KeyError, TypeError):
                continue
            if not native_id:
                continue
            stations.append(Station(
                id=self._station_id(native_id),
                provider=self.slug,
                native_id=native_id,
                name=reach.get("name") or f"Reach {native_id}",
                latitude=float(reach.get("lat", 0.0)),
                longitude=float(reach.get("lon", 0.0)),
                country_code="EC",
                river=reach.get("river"),
                is_active=True,
            ))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch simulated discharge for one reach (retrospective or forecast)."""
        reach_id = station_id.removeprefix(f"{self.slug}:")

        now = datetime.now(UTC)
        if end > now:
            # Future window -> latest forecast (median ensemble).
            path = f"/forecast/{reach_id}"
            params = {"format": "json"}
        else:
            # Historical window -> retrospective daily reanalysis.
            path = f"/retrospectivedaily/{reach_id}"
            params = {
                "format": "json",
                "start_date": start.strftime("%Y%m%d"),
                "end_date": end.strftime("%Y%m%d"),
            }

        try:
            resp = await self._get(path, params=params)
            payload = resp.json()
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch GEOGLOWS data for reach {reach_id}: {exc}",
            ) from exc

        if not isinstance(payload, dict) or "datetime" not in payload:
            raise DataFormatError(
                self.slug,
                f"Unexpected GEOGLOWS response for reach {reach_id}",
            )

        times = payload.get("datetime") or []
        values = self._extract_values(payload, reach_id)
        if values is None:
            raise DataFormatError(
                self.slug,
                f"No discharge series in GEOGLOWS response for reach {reach_id}",
            )

        start_utc = start if start.tzinfo else start.replace(tzinfo=UTC)
        end_utc = end if end.tzinfo else end.replace(tzinfo=UTC)
        observations: list[Observation] = []
        for raw_time, raw_val in zip(times, values, strict=False):
            ts = self._parse_ts(raw_time)
            if ts is None or not (start_utc <= ts <= end_utc):
                continue
            discharge = None if raw_val is None else float(raw_val)
            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=discharge,
                quality=QualityFlag.ESTIMATED,
            ))

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent ~30 days of simulated discharge."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(days=30),
            end=now,
        )

    @staticmethod
    def _extract_values(payload: dict, reach_id: str) -> list | None:
        """Locate the discharge series in a GEOGLOWS V2 response.

        Retrospective responses key the series by the river_id string; forecast
        responses use ``flow_median``. Fall back to the first non-metadata list
        parallel to ``datetime``.
        """
        retro = payload.get(reach_id)
        if isinstance(retro, list):
            return retro
        median = payload.get("flow_median")
        if isinstance(median, list):
            return median
        n = len(payload.get("datetime") or [])
        for key, val in payload.items():
            if key in ("datetime", "metadata"):
                continue
            if isinstance(val, list) and len(val) == n:
                return val
        return None

    @staticmethod
    def _parse_ts(raw: object) -> datetime | None:
        if not isinstance(raw, str):
            return None
        try:
            dt = datetime.fromisoformat(raw)
        except ValueError:
            return None
        return dt if dt.tzinfo else dt.replace(tzinfo=UTC)
