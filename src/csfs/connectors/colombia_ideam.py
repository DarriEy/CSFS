"""Colombia IDEAM connector — Instituto de Hidrologia via Socrata SODA API."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# Socrata dataset identifiers on datos.gov.co
_STATIONS_DATASET = "hp9r-jxuu"
_DISCHARGE_DATASET = "sbwg-7ju4"

# Maximum rows per Socrata request
_SODA_PAGE_SIZE = 5000


@register("colombia_ideam")
class ColombiaIDEAMConnector(BaseConnector):
    """Connector for Colombia's IDEAM hydrology data via the Socrata SODA API.

    Data is published on https://www.datos.gov.co.

    * Station catalog:  dataset ``hp9r-jxuu``
    * Discharge data:   dataset ``sbwg-7ju4``
    """

    slug = "colombia_ideam"
    display_name = "IDEAM (Colombia)"
    base_url = "https://www.datos.gov.co/resource"
    country_codes = ["CO"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all hydrological stations from the IDEAM catalog."""
        all_records: list[dict] = []
        offset = 0

        while True:
            params: dict[str, str | int] = {
                "$limit": _SODA_PAGE_SIZE,
                "$offset": offset,
            }
            self._apply_app_token(params)

            try:
                resp = await self._get(
                    f"/{_STATIONS_DATASET}.json",
                    params=params,
                )
            except Exception as exc:
                raise ConnectorError(
                    self.slug, "Failed to fetch station catalog"
                ) from exc

            data = resp.json()
            if not isinstance(data, list):
                logger.warning(
                    "unexpected_stations_format",
                    provider=self.slug,
                    type=type(data).__name__,
                )
                break

            all_records.extend(data)

            # Stop when a page is smaller than the limit (last page)
            if len(data) < _SODA_PAGE_SIZE:
                break
            offset += _SODA_PAGE_SIZE

        return self._parse_stations(all_records)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for a station over a time range."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        all_observations: list[Observation] = []
        offset = 0

        start_str = start.strftime("%Y-%m-%dT%H:%M:%S")
        end_str = end.strftime("%Y-%m-%dT%H:%M:%S")

        where_clause = (
            f"codigoestacion='{native_id}'"
            f" AND fechaobservacion>='{start_str}'"
            f" AND fechaobservacion<='{end_str}'"
        )

        while True:
            params: dict[str, str | int] = {
                "$where": where_clause,
                "$order": "fechaobservacion",
                "$limit": _SODA_PAGE_SIZE,
                "$offset": offset,
            }
            self._apply_app_token(params)

            try:
                resp = await self._get(
                    f"/{_DISCHARGE_DATASET}.json",
                    params=params,
                )
            except Exception as exc:
                raise ConnectorError(
                    self.slug,
                    f"Failed to fetch observations for {native_id}",
                ) from exc

            data = resp.json()
            if not isinstance(data, list):
                logger.warning(
                    "unexpected_observations_format",
                    provider=self.slug,
                    type=type(data).__name__,
                )
                break

            all_observations.extend(
                self._parse_observations(data, station_id)
            )

            if len(data) < _SODA_PAGE_SIZE:
                break
            offset += _SODA_PAGE_SIZE

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=all_observations,
            fetched_at=datetime.now(UTC),
        )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent observations (last 7 days)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(days=7),
            end=now,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _apply_app_token(self, params: dict) -> None:
        """Add the Socrata app token to *params* if configured."""
        token = self.config.get("app_token", "")
        if token:
            params["$$app_token"] = token

    def _parse_stations(self, data: list[dict]) -> list[Station]:
        """Parse the Socrata station catalog into Station models."""
        stations: list[Station] = []
        for entry in data:
            native_id = str(entry.get("codigoestacion", "")).strip()
            if not native_id:
                continue

            try:
                lat = float(str(entry.get("latitud", 0)))
                lon = float(str(entry.get("longitud", 0)))
            except (ValueError, TypeError):
                logger.warning(
                    "station_bad_coords",
                    provider=self.slug,
                    station=native_id,
                )
                continue

            name = str(
                entry.get("nombreestacion", native_id)
            ).strip() or native_id

            river = entry.get("corriente")
            if river is not None:
                river = str(river).strip() or None

            area = entry.get("areacuenca") or entry.get("areaoperativa")
            catchment: float | None = None
            if area is not None:
                try:
                    catchment = float(str(area))
                except (ValueError, TypeError):
                    catchment = None

            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=name,
                    latitude=lat,
                    longitude=lon,
                    country_code="CO",
                    river=river,
                    catchment_area_km2=catchment,
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
        data: list[dict],
        station_id: str,
    ) -> list[Observation]:
        """Parse Socrata observation rows into Observation models."""
        observations: list[Observation] = []
        for entry in data:
            ts_raw = entry.get("fechaobservacion")
            if not ts_raw:
                continue

            try:
                ts = datetime.fromisoformat(str(ts_raw))
            except (ValueError, TypeError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid timestamp in observation: {exc}",
                ) from exc

            value = entry.get("valorobservado")
            discharge: float | None = None
            if value is not None:
                try:
                    discharge = float(str(value))
                except (ValueError, TypeError):
                    discharge = None

            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=discharge,
                quality=(
                    QualityFlag.RAW if discharge is not None
                    else QualityFlag.MISSING
                ),
            ))

        return observations
