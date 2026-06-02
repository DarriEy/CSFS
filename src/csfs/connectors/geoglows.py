# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""GEOGLOWS ECMWF V2 connector -- global simulated streamflow.

GEOGLOWS (https://geoglows.ecmwf.int) publishes MODELED river discharge for
~6-7 million river reaches worldwide. It is a hydrological model keyed by a
*river reach id* (a.k.a. LINKNO / comid / ``river_id``), NOT a gauge network,
so it does not natively fit CSFS's station-based connector contract.

The GEOGLOWS V2 REST API is open and keyless. The relevant endpoints are::

    GET /api/v2/getriverid/?lat=..&lon=..&format=json   -> {"river_id": <int>}
    GET /api/v2/retrospectivedaily/<river_id>?format=json
            &start_date=YYYYMMDD&end_date=YYYYMMDD
        -> {"<river_id>": [q, ...], "datetime": [iso, ...], "metadata": {...}}
    GET /api/v2/forecast/<river_id>?format=json
        -> {"datetime": [iso, ...], "flow_median": [q, ...], ...}

Discharge is in cubic metres per second ("cms"). Because GEOGLOWS is model
output (a simulated retrospective reanalysis + forecast), observations are
flagged ``ESTIMATED``.

Like the GloFAS connector, this treats a curated set of major-river mainstem
reaches as "virtual stations" (overridable via config ``virtual_stations``).
The default reach ids were discovered live by querying ``getriverid`` near each
river's outlet and selecting the highest-discharge mainstem reach.

References
----------
- API docs: https://geoglows.ecmwf.int/documentation
- Portal: https://geoglows.ecmwf.int/
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import (
    Observation,
    QualityFlag,
    Station,
    TimeSeriesChunk,
)
from csfs.core.registry import register

logger = structlog.get_logger()

_GEOGLOWS_BASE_URL = "https://geoglows.ecmwf.int/api/v2"

# Curated GEOGLOWS V2 mainstem reaches that act as virtual stations. Each
# ``id`` is the native GEOGLOWS river_id (LINKNO). lat/lon are the snap point
# used to discover the reach. Operators can override via config
# ``virtual_stations`` (list of dicts with at least ``id``).
# Fields: id, name, lat, lon, country, river.
_DEFAULT_REACHES: list[dict] = [
    {"id": "621130084", "name": "Amazon near Obidos",
     "lat": -2.0, "lon": -55.9, "country": "BR", "river": "Amazon"},
    {"id": "430513656", "name": "Mekong near Stung Treng",
     "lat": 13.58, "lon": 106.02, "country": "KH", "river": "Mekong"},
    {"id": "320284806", "name": "Yenisei near Igarka",
     "lat": 67.43, "lon": 86.48, "country": "RU", "river": "Yenisei"},
    {"id": "340072042", "name": "Lena (lower mainstem)",
     "lat": 70.63, "lon": 127.24, "country": "RU", "river": "Lena"},
    {"id": "310375473", "name": "Ob near Salekhard",
     "lat": 66.63, "lon": 66.50, "country": "RU", "river": "Ob"},
    {"id": "820211239", "name": "Mackenzie at Arctic Red River",
     "lat": 67.56, "lon": -133.74, "country": "CA", "river": "Mackenzie"},
    {"id": "220531907", "name": "Danube near Ceatal Izmail",
     "lat": 45.22, "lon": 28.72, "country": "RO", "river": "Danube"},
]


@register("geoglows")
class GEOGloWSConnector(BaseConnector):
    """Connector for GEOGLOWS ECMWF V2 (global simulated streamflow).

    GEOGLOWS is a reach-based hydrological model, not a gauge network, so this
    connector exposes a curated set of major-river mainstem reaches as virtual
    stations. Discharge is GEOGLOWS V2 retrospective (daily) or forecast (median
    ensemble) in m3/s, flagged ``ESTIMATED`` (model output).

    Configuration options:
        virtual_stations : list[dict]
            Override the built-in reaches. Each item must provide ``id`` (the
            GEOGLOWS river_id) and may provide ``name``, ``lat``, ``lon``,
            ``country``, ``river``.
    """

    slug = "geoglows"
    display_name = "GEOGLOWS ECMWF V2 (Global Simulation)"
    base_url = _GEOGLOWS_BASE_URL
    country_codes: list[str] = ["global"]
    # The shared getriverid/data host rate-limits aggressive callers.
    max_concurrent_requests = 2

    def _reaches(self) -> list[dict]:
        """Return the active reach definitions (config override or built-in)."""
        configured = self.config.get("virtual_stations")
        return configured if configured else _DEFAULT_REACHES

    def _reach_by_native_id(self, native_id: str) -> dict | None:
        for reach in self._reaches():
            if str(reach["id"]) == native_id:
                return reach
        return None

    async def fetch_stations(self) -> list[Station]:
        """Return the configured GEOGLOWS reaches as virtual stations."""
        stations: list[Station] = []
        for reach in self._reaches():
            try:
                native_id = str(reach["id"])
            except (KeyError, TypeError) as exc:
                logger.warning(
                    "geoglows_invalid_reach",
                    provider=self.slug, reach=reach, error=str(exc),
                )
                continue
            stations.append(
                Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=reach.get("name") or f"Reach {native_id}",
                    latitude=float(reach.get("lat", 0.0)),
                    longitude=float(reach.get("lon", 0.0)),
                    country_code=reach.get("country", "global"),
                    river=reach.get("river"),
                    is_active=True,
                )
            )
        logger.info("stations_fetched", provider=self.slug, count=len(stations))
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
            # Future window -> use the latest forecast (median ensemble).
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

        observations: list[Observation] = []
        for raw_time, raw_val in zip(times, values, strict=False):
            ts = self._parse_ts(raw_time)
            if ts is None or not (start <= ts <= end):
                continue
            discharge = None if raw_val is None else float(raw_val)
            observations.append(
                Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    # GEOGLOWS reanalysis/forecast model output, not a gauge.
                    quality=QualityFlag.ESTIMATED,
                )
            )

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    @staticmethod
    def _extract_values(payload: dict, reach_id: str) -> list | None:
        """Locate the discharge series in a GEOGLOWS V2 response.

        Retrospective responses key the series by the river_id string; forecast
        responses use ``flow_median``. Fall back to the first non-metadata list
        parallel to ``datetime``.
        """
        # Retrospective: keyed by reach id.
        if isinstance(payload.get(reach_id), list):
            return payload[reach_id]
        # Forecast: median of the ensemble.
        if isinstance(payload.get("flow_median"), list):
            return payload["flow_median"]
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
