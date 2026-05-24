"""Spain SAIH/CEDEX connector — Ebro river basin (saihebro.com).

The SAIH (Sistema Automático de Información Hidrológica) is Spain's real-time
hydrological monitoring network.  Each river basin confederation operates its
own SAIH instance.  This connector targets the Ebro confederation, which
exposes a public JSON API, as the initial implementation.

Endpoints used
--------------
* Station listing:
  GET /datos/estaciones?tipo=AF
  Returns a JSON array of gauging stations (tipo AF = aforamiento).

* Observations:
  GET /datos/valores?estacion={codigo}&magnitud=Q
       &fechaInicio={YYYY-MM-DD}&fechaFin={YYYY-MM-DD}
  Returns ``{"valores": [{fecha, valor, validado}, ...]}``.

Both endpoints may evolve; the connector is written defensively with
fallback parsing and clear error messages.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()


def _validated_to_quality(validado: bool | int | None) -> QualityFlag:
    """Map the SAIH *validado* flag to a CSFS quality flag.

    The API may return a boolean or an integer (1 = validated, 0 = raw).
    """
    if validado is None:
        return QualityFlag.RAW
    if isinstance(validado, bool):
        return QualityFlag.GOOD if validado else QualityFlag.RAW
    if int(validado) == 1:
        return QualityFlag.GOOD
    return QualityFlag.RAW


@register("spain_saih")
class SpainSAIHConnector(BaseConnector):
    """Connector for Spain's SAIH Ebro real-time gauging data."""

    slug = "spain_saih"
    display_name = "SAIH Ebro (Spain)"
    base_url = "https://www.saihebro.com/saihebro/api"
    country_codes = ["ES"]

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all active gauging (AF) stations from the Ebro SAIH."""
        try:
            resp = await self._get("/datos/estaciones", params={"tipo": "AF"})
        except httpx.HTTPStatusError as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch station list: HTTP {exc.response.status_code}",
            ) from exc

        return self._parse_stations(resp.json())

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for *station_id* over [start, end]."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        params: dict[str, str] = {
            "estacion": native_id,
            "magnitud": "Q",
            "fechaInicio": start.strftime("%Y-%m-%d"),
            "fechaFin": end.strftime("%Y-%m-%d"),
        }

        try:
            resp = await self._get("/datos/valores", params=params)
        except httpx.HTTPStatusError as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch observations for {native_id}: HTTP {exc.response.status_code}",
            ) from exc

        return self._parse_observations(resp.json(), station_id)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent discharge observations (last 24 h)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # -----------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------

    def _parse_stations(self, data: list[dict] | dict) -> list[Station]:
        """Parse the SAIH station-list JSON into ``Station`` models.

        The API may return a bare list or wrap it under a key.  We handle
        both defensively.
        """
        items: list[dict] = data if isinstance(data, list) else data.get("estaciones", [])

        stations: list[Station] = []
        for entry in items:
            native_id = str(entry.get("codigo", "")).strip()
            if not native_id:
                continue

            lat = entry.get("coordY") or entry.get("latitud")
            lon = entry.get("coordX") or entry.get("longitud")
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
                    name=entry.get("nombre", native_id),
                    latitude=float(lat),
                    longitude=float(lon),
                    country_code="ES",
                    river=entry.get("rio"),
                    catchment_area_km2=entry.get("cuenca"),
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
        """Parse the SAIH observations response into a ``TimeSeriesChunk``.

        Expected shape::

            {
                "valores": [
                    {"fecha": "2024-06-01T12:00:00", "valor": 34.5, "validado": true},
                    ...
                ]
            }

        The response may alternatively be a bare list of value dicts.
        """
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("valores", [])
        else:
            raise DataFormatError(
                self.slug,
                f"Unexpected response type: {type(data).__name__}",
            )

        observations: list[Observation] = []
        for entry in items:
            try:
                ts = datetime.fromisoformat(entry["fecha"])
            except (KeyError, ValueError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid or missing timestamp in observation: {exc}",
                ) from exc

            value = entry.get("valor")
            discharge = float(value) if value is not None else None
            validado = entry.get("validado")
            quality = (
                QualityFlag.MISSING
                if discharge is None
                else _validated_to_quality(validado)
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
