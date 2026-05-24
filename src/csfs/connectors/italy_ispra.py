"""Italy ISPRA SINTAI connector — national hydrological data.

ISPRA (Istituto Superiore per la Protezione e la Ricerca Ambientale)
operates the SINTAI/HISCentral system which aggregates hydrological
data from regional agencies across Italy.

Endpoints used
--------------
* Station listing:
  GET /hiscentral/hydromap/getStations?format=json
  Returns station metadata including code, name, coordinates, and river.

* Observations:
  GET /hiscentral/hydromap/getValues?stationCode={code}&variable=Discharge
       &startDate={YYYY-MM-DD}&endDate={YYYY-MM-DD}&format=json
  Returns ``[{DateTime, Value}, ...]``.

The API is known to be fragile; the connector is built defensively with
fallback parsing and structured logging.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
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


@register("italy_ispra")
class ItalyISPRAConnector(BaseConnector):
    """Connector for Italy's ISPRA SINTAI/HISCentral discharge data."""

    slug = "italy_ispra"
    display_name = "ISPRA SINTAI (Italy)"
    base_url = "http://www.hiscentral.isprambiente.gov.it"
    country_codes = ["IT"]

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all stations from the ISPRA HISCentral system."""
        try:
            resp = await self._get(
                "/hiscentral/hydromap/getStations",
                params={"format": "json"},
            )
        except httpx.HTTPStatusError as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch station list: "
                f"HTTP {exc.response.status_code}",
            ) from exc

        return self._parse_stations(resp.json())

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for *station_id*."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        params: dict[str, str] = {
            "stationCode": native_id,
            "variable": "Discharge",
            "startDate": start.strftime("%Y-%m-%d"),
            "endDate": end.strftime("%Y-%m-%d"),
            "format": "json",
        }

        try:
            resp = await self._get(
                "/hiscentral/hydromap/getValues",
                params=params,
            )
        except httpx.HTTPStatusError as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch observations for {native_id}: "
                f"HTTP {exc.response.status_code}",
            ) from exc

        return self._parse_observations(resp.json(), station_id)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch most recent discharge observations (last 24 h)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # -----------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------

    def _parse_stations(
        self,
        data: list[dict] | dict,
    ) -> list[Station]:
        """Parse the ISPRA station-list JSON.

        The response may be a bare list or wrapped under a key
        such as ``stations`` or ``Stations``.
        """
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = (
                data.get("stations")
                or data.get("Stations")
                or data.get("data", [])
            )
        else:
            logger.warning(
                "unexpected_stations_type",
                provider=self.slug,
                type=type(data).__name__,
            )
            return []

        stations: list[Station] = []
        for entry in items:
            native_id = str(
                entry.get("StationCode", "")
            ).strip()
            if not native_id:
                continue

            lat = entry.get("Latitude") or entry.get("latitude")
            lon = entry.get("Longitude") or entry.get("longitude")
            if lat is None or lon is None:
                logger.warning(
                    "station_missing_coords",
                    provider=self.slug,
                    station=native_id,
                )
                continue

            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=entry.get(
                        "StationName", native_id
                    ),
                    latitude=float(lat),
                    longitude=float(lon),
                    country_code="IT",
                    river=entry.get("RiverName"),
                ))
            except (ValueError, KeyError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    station=native_id,
                    error=str(exc),
                )
                continue

        return stations

    def _parse_observations(
        self,
        data: dict | list,
        station_id: str,
    ) -> TimeSeriesChunk:
        """Parse ISPRA observations into a ``TimeSeriesChunk``.

        Expected shape::

            [
                {"DateTime": "2024-06-01T12:00:00", "Value": 34.5},
                ...
            ]

        May also be wrapped as ``{"values": [...]}``.
        """
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = (
                data.get("values")
                or data.get("Values")
                or data.get("data", [])
            )
        else:
            raise DataFormatError(
                self.slug,
                f"Unexpected response type: {type(data).__name__}",
            )

        observations: list[Observation] = []
        for entry in items:
            ts_raw = entry.get("DateTime") or entry.get("dateTime")
            if not ts_raw:
                logger.warning(
                    "observation_missing_timestamp",
                    provider=self.slug,
                    station=station_id,
                )
                continue

            try:
                ts = datetime.fromisoformat(str(ts_raw))
            except ValueError as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid timestamp '{ts_raw}': {exc}",
                ) from exc

            value = entry.get("Value") or entry.get("value")
            discharge = (
                float(value) if value is not None else None
            )
            quality = (
                QualityFlag.MISSING
                if discharge is None
                else QualityFlag.RAW
            )

            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=discharge,
                quality=quality,
            ))

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )
