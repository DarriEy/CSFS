"""Italy Emilia-Romagna connector -- ARPAE (discharge / portata).

ARPAE Emilia-Romagna (Servizio Idro-Meteo-Clima) publishes its data through
the DEXT3R portal, but that portal only delivers extractions asynchronously by
e-mail (the ``debra`` API returns a task id, not data), so it is unusable for a
synchronous connector.

Discharge IS, however, exposed through the regional open-data service as a
single rolling JSON file ("dati osservati di portata istantanea"):

    https://dati-simc.arpae.it/opendata/osservati/portata_istantanea/portata_istantanea.json

The file is newline-delimited BUFR-style JSON.  Each line is one observation
for one station and one timestamp::

    {"version":"0.1","network":"simnpr","ident":null,
     "lon":1160807,"lat":4488830,"date":"2026-05-23T05:30:00Z",
     "data":[
        {"vars":{"B01019":{"v":"Pontelagoscuro"}, ... ,
                 "B05001":{"v":44.88830},"B06001":{"v":11.60807}}},
        {"timerange":[254,0,0],"level":[1,null,null,null],
         "vars":{"B13226":{"v":787.18}}}
     ]}

where:

- ``date``  -- observation timestamp (UTC, ISO-8601).
- ``lon`` / ``lat`` -- decimal degrees * 1e5 (integers).
- ``B01019`` -- station name.
- ``B13226`` -- discharge in m3/s (instantaneous "portata").

The feed currently covers the major Po-river gauges (Boretto, Pontelagoscuro,
Piacenza, Cremona, Sermide, Borgoforte, Spessa Po) on a ~10-day rolling window
at 15-minute resolution.  Hydrometric *level* (B13215) is published elsewhere
(Allerta Meteo ER) but is NOT discharge; this connector intentionally serves
only true discharge (m3/s).

References
----------
- Portal: https://simc.arpae.it/dext3r/
- Open data (CKAN): https://dati.arpae.it/dataset/meteo-dati-osservati-di-portata-istantanea
"""

from __future__ import annotations

import json
from datetime import UTC, datetime
from typing import Any

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

# Rolling open-data feed of observed instantaneous discharge (m3/s).
_PORTATA_PATH = "/opendata/osservati/portata_istantanea/portata_istantanea.json"

# BUFR descriptor codes used in the feed.
_VAR_NAME = "B01019"  # station name
_VAR_DISCHARGE = "B13226"  # discharge, m3/s


@register("italy_emilia")
class ItalyEmiliaConnector(BaseConnector):
    """Connector for ARPAE Emilia-Romagna discharge open data."""

    slug = "italy_emilia"
    display_name = "ARPAE Emilia-Romagna (Italy)"
    base_url = "https://dati-simc.arpae.it"
    country_codes = ["IT"]

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        # Cache the parsed feed for the lifetime of the session so that
        # per-station fetch_observations calls don't re-download ~2 MB each.
        self._records: list[dict[str, Any]] | None = None

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all stations present in the discharge feed."""
        records = await self._load_records()
        stations: dict[str, Station] = {}
        for rec in records:
            native_id = self._native_id(rec)
            if native_id is None or native_id in stations:
                continue
            name = self._station_name(rec)
            lat, lon = self._coords(rec)
            if lat is None or lon is None:
                continue
            try:
                stations[native_id] = Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=name or native_id,
                    latitude=lat,
                    longitude=lon,
                    country_code="IT",
                    river="Po",
                )
            except (ValueError, KeyError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    station=native_id,
                    error=str(exc),
                )
                continue
        return list(stations.values())

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Return discharge (m3/s) observations for a station in [start, end]."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        records = await self._load_records()

        observations: list[Observation] = []
        for rec in records:
            if self._native_id(rec) != native_id:
                continue
            ts = self._timestamp(rec)
            if ts is None or not (start <= ts <= end):
                continue
            discharge = self._discharge(rec)
            observations.append(
                Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=(
                        QualityFlag.RAW
                        if discharge is not None
                        else QualityFlag.MISSING
                    ),
                )
            )

        observations.sort(key=lambda o: o.timestamp)
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _load_records(self) -> list[dict[str, Any]]:
        """Download and parse the newline-delimited discharge feed (cached)."""
        if self._records is not None:
            return self._records

        resp = await self._get(_PORTATA_PATH)
        records: list[dict[str, Any]] = []
        for line in resp.text.splitlines():
            line = line.strip()
            if not line:
                continue
            try:
                records.append(json.loads(line))
            except json.JSONDecodeError:
                # Skip malformed lines rather than failing the whole feed.
                continue
        if not records:
            raise ConnectorError(
                self.slug, "ARPAE discharge feed returned no parseable records"
            )
        self._records = records
        return records

    @staticmethod
    def _native_id(rec: dict[str, Any]) -> str | None:
        """Stable, reversible station id from the record's coordinates/network.

        Format: ``<lon>,<lat>,<network>`` (lon/lat are integer micro-degrees,
        matching ARPAE's own station identifiers).
        """
        lon = rec.get("lon")
        lat = rec.get("lat")
        network = rec.get("network")
        if lon is None or lat is None or not network:
            return None
        return f"{lon},{lat},{network}"

    @staticmethod
    def _data_vars(rec: dict[str, Any]) -> list[dict[str, Any]]:
        return rec.get("data", []) or []

    @classmethod
    def _station_name(cls, rec: dict[str, Any]) -> str | None:
        for block in cls._data_vars(rec):
            var = block.get("vars", {}).get(_VAR_NAME)
            if var is not None:
                return var.get("v")
        return None

    @staticmethod
    def _coords(rec: dict[str, Any]) -> tuple[float | None, float | None]:
        lon = rec.get("lon")
        lat = rec.get("lat")
        if lon is None or lat is None:
            return None, None
        # Stored as decimal degrees * 1e5.
        return lat / 1e5, lon / 1e5

    @staticmethod
    def _timestamp(rec: dict[str, Any]) -> datetime | None:
        raw = rec.get("date")
        if not raw:
            return None
        try:
            ts = datetime.fromisoformat(raw.replace("Z", "+00:00"))
        except ValueError:
            return None
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=UTC)
        return ts

    @classmethod
    def _discharge(cls, rec: dict[str, Any]) -> float | None:
        for block in cls._data_vars(rec):
            var = block.get("vars", {}).get(_VAR_DISCHARGE)
            if var is not None and var.get("v") is not None:
                try:
                    return float(var["v"])
                except (TypeError, ValueError):
                    return None
        return None
