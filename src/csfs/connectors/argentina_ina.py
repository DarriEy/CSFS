"""Argentina INA (Instituto Nacional del Agua) connector — Alerta Hidrologica."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# Variable IDs used by INA's Alerta system.
_DISCHARGE_VAR_NAMES = {"caudal"}
_WATER_LEVEL_VAR_ID = 2  # Altura hidrometrica


@register("argentina_ina")
class ArgentinaINAConnector(BaseConnector):
    slug = "argentina_ina"
    display_name = "INA Alerta Hidrologica (Argentina)"
    base_url = "https://alerta.ina.gob.ar/a5"
    country_codes = ["AR"]

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        # Cache: native station id -> discharge series_id
        self._station_to_series: dict[str, int] = {}

    async def fetch_stations(self) -> list[Station]:
        """Return all stations from the INA Alerta system."""
        resp = await self._get("/obs/puntual/estaciones")
        return self._parse_stations(resp.json())

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for a station over a time range."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        series_id = await self._resolve_series_id(native_id)

        resp = await self._get(
            f"/obs/puntual/series/{series_id}/observaciones",
            params={
                "timestart": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                "timeend": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
            },
        )
        return self._parse_observations(resp.json(), station_id)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_stations(self, data: list[dict]) -> list[Station]:
        """Parse the station list JSON from /obs/puntual/estaciones."""
        stations: list[Station] = []
        for entry in data:
            native_id = entry.get("id")
            if native_id is None:
                continue
            native_id = str(native_id)

            geom = entry.get("geom") or {}
            coords = geom.get("coordinates", [])
            if len(coords) < 2:
                continue

            try:
                lon = float(str(coords[0]))
                lat = float(str(coords[1]))
            except (ValueError, TypeError):
                continue

            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=entry.get("nombre", native_id),
                    latitude=lat,
                    longitude=lon,
                    country_code="AR",
                    river=entry.get("rio"),
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
        self, data: list[dict], station_id: str
    ) -> TimeSeriesChunk:
        """Parse the observations JSON array into a TimeSeriesChunk."""
        observations: list[Observation] = []
        for entry in data:
            try:
                ts = datetime.fromisoformat(entry["timestart"])
            except (KeyError, ValueError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid timestamp in observation: {exc}",
                ) from exc

            value = entry.get("valor")
            discharge = (
                float(str(value)) if value is not None else None
            )

            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=discharge,
                quality=(
                    QualityFlag.RAW
                    if discharge is not None
                    else QualityFlag.MISSING
                ),
            ))

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    async def _build_series_cache(self) -> None:
        """Fetch the series metadata and cache station -> series mappings.

        We look for series whose variable name contains 'caudal'
        (discharge) — case-insensitive.
        """
        resp = await self._get(
            "/obs/puntual/series",
            params={"format": "geojson"},
        )
        data = resp.json()
        features = data.get("features", [])
        for feat in features:
            props = feat.get("properties", {})
            var_name = (props.get("var_nombre") or "").lower()
            if not any(kw in var_name for kw in _DISCHARGE_VAR_NAMES):
                continue
            estacion_id = props.get("estacion_id")
            series_id = props.get("id")
            if estacion_id is not None and series_id is not None:
                key = str(estacion_id)
                # Keep the first discharge series found per station
                if key not in self._station_to_series:
                    self._station_to_series[key] = int(series_id)

    async def _resolve_series_id(self, native_id: str) -> int:
        """Return the discharge series_id for a station.

        Uses the cache first; falls back to fetching the series
        metadata if the mapping is empty.
        """
        if native_id in self._station_to_series:
            return self._station_to_series[native_id]

        await self._build_series_cache()

        if native_id not in self._station_to_series:
            raise DataFormatError(
                self.slug,
                f"No discharge series found for station '{native_id}'",
            )
        return self._station_to_series[native_id]
