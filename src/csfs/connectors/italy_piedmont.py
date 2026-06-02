"""Italy Piemonte connector -- ARPA Piemonte real-time meteo-hydro network.

VERDICT: LEVEL-ONLY. ARPA Piemonte does **not** openly expose discharge
(portata, m3/s) for its real-time hydrometric network. The public real-time
API and the ArcGIS REST services publish only the hydrometric *level*
(``hydrometric_level`` / "livello idrometrico", in metres). Discharge values
exist only in a manually-issued daily hydrological bulletin (PDF, weekdays
only, selected stations) and in static climatology layers (annual-mean and
peak-flow maps) -- neither is a queryable real-time discharge time series.

This connector is therefore wired to the *real* API: it returns the real
station catalogue (the ~110 hydrometric stations of the regional network) and
the real measurement time series. Because no discharge is available, every
:class:`Observation` carries ``discharge_m3s=None`` with
:class:`QualityFlag.MISSING`. This mirrors the ``poland_imgw`` precedent
(another level-only provider) and keeps the connector honest rather than
fabricating discharge from level via an (unavailable) rating curve.

Real endpoints
--------------
- Registry : ``GET /api_realtime/pie_anag`` -> bare list of station records
  (``station_code``, ``name``, ``lat``, ``lng``, ``river_name``,
  ``station_type`` -- the letter ``I`` flags an *idrometrica* / hydrometric
  station).
- Data     : ``GET /api_realtime/data_pie?station_code=&date_from=&date_to=``
  -> paginated ``{page, page_size, total_pages, total_items, data: [...]}``;
  each record has ``date`` (ISO 8601, local +01/+02 offset) and
  ``hydrometric_level`` (metres).

References
----------
- API docs   : https://utility.arpa.piemonte.it/api_realtime/docs
- OpenAPI    : https://utility.arpa.piemonte.it/api_realtime/openapi.json
- Network    : https://www.arpa.piemonte.it/scheda-informativa/rete-idrometrica
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

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

# ARPA Piemonte real-time FastAPI service.
_PIEMONTE_BASE_URL = "https://utility.arpa.piemonte.it/api_realtime"

# Sensor-type letter that marks an idrometric (water-level) station in the
# registry's ``station_type`` field, e.g. "IPT", "HIPT", "I".
_HYDROMETRIC_FLAG = "I"

# Page size for the (paginated) /data_pie endpoint. The API caps this at 10000.
_DATA_PAGE_SIZE = 10000


@register("italy_piedmont")
class ItalyPiedmontConnector(BaseConnector):
    """Connector for ARPA Piemonte's real-time meteo-hydro network.

    Level-only: the upstream API exposes hydrometric *level* (metres), not
    discharge. Observations are returned with ``discharge_m3s=None``.
    """

    slug = "italy_piedmont"
    display_name = "ARPA Piemonte (Italy)"
    base_url = _PIEMONTE_BASE_URL
    country_codes = ["IT"]

    async def fetch_stations(self) -> list[Station]:
        """Return the real hydrometric station catalogue.

        Filters the full registry down to stations whose ``station_type``
        includes the idrometric flag ``I`` (i.e. they carry a water-level
        sensor on a watercourse or lake).
        """
        resp = await self._get("/pie_anag")
        try:
            data = resp.json()
        except ValueError as exc:
            raise DataFormatError(self.slug, "pie_anag response is not valid JSON") from exc
        return self._parse_stations(data)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch the measurement time series for a station over a range.

        NOTE: ARPA Piemonte does not expose discharge (portata). Each returned
        observation has ``discharge_m3s=None`` / ``QualityFlag.MISSING``; only
        a water *level* (metres) is published upstream.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        params = {
            "station_code": native_id,
            "date_from": start.strftime("%Y-%m-%dT%H:%M"),
            "date_to": end.strftime("%Y-%m-%dT%H:%M"),
            "page_size": _DATA_PAGE_SIZE,
        }
        resp = await self._get("/data_pie", params=params)
        try:
            data = resp.json()
        except ValueError as exc:
            raise ConnectorError(
                self.slug, f"data_pie response is not valid JSON for {native_id}"
            ) from exc

        return self._parse_observations(data, station_id)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_stations(self, data: Any) -> list[Station]:
        """Parse the ``/pie_anag`` registry into hydrometric Station objects."""
        if not isinstance(data, list):
            raise DataFormatError(
                self.slug, "pie_anag did not return a list of stations"
            )

        stations: list[Station] = []
        for entry in data:
            if not isinstance(entry, dict):
                continue

            station_type = entry.get("station_type") or ""
            if _HYDROMETRIC_FLAG not in station_type:
                continue

            native_id = str(entry.get("station_code") or "").strip()
            if not native_id:
                continue

            river = entry.get("river_name")
            if river in ("-", ""):
                river = None

            lat = entry.get("lat")
            lng = entry.get("lng")
            if lat is None or lng is None:
                continue

            quote = entry.get("quote")
            try:
                elevation = float(quote) if quote is not None else None
            except (ValueError, TypeError):
                elevation = None

            try:
                stations.append(
                    Station(
                        id=self._station_id(native_id),
                        provider=self.slug,
                        native_id=native_id,
                        name=entry.get("name", ""),
                        latitude=float(lat),
                        longitude=float(lng),
                        country_code="IT",
                        river=river,
                        elevation_m=elevation,
                    )
                )
            except (ValueError, KeyError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    station=native_id,
                    error=str(exc),
                )
                continue

        return stations

    def _parse_observations(self, data: Any, station_id: str) -> TimeSeriesChunk:
        """Parse a ``/data_pie`` response into a TimeSeriesChunk.

        The endpoint wraps records in a pagination envelope
        ``{page, ..., data: [...]}``; we also tolerate a bare list. Each record
        is mapped to an Observation. Since no discharge is published, every
        observation has ``discharge_m3s=None`` / MISSING. Records carrying a
        usable ``hydrometric_level`` (the only real measurement) are emitted so
        the timestamp/coverage is preserved; records with no level at all are
        skipped.
        """
        if isinstance(data, dict):
            records = data.get("data", [])
        elif isinstance(data, list):
            records = data
        else:
            raise DataFormatError(self.slug, "unexpected data_pie payload shape")

        observations: list[Observation] = []
        for entry in records:
            if not isinstance(entry, dict):
                continue

            raw_ts = entry.get("date")
            if not raw_ts:
                continue
            try:
                ts = datetime.fromisoformat(raw_ts)
            except (ValueError, TypeError) as exc:
                logger.warning(
                    "timestamp_parse_failed",
                    provider=self.slug,
                    station=station_id,
                    raw=raw_ts,
                    error=str(exc),
                )
                continue

            # Only a water LEVEL is available; discharge is not exposed by ARPA
            # Piemonte. Skip rows with no level reading at all (pure gaps), but
            # emit rows that have a level so downstream coverage is recorded.
            if entry.get("hydrometric_level") is None:
                continue

            observations.append(
                Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=None,
                    quality=QualityFlag.MISSING,
                )
            )

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )
