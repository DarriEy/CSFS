"""Peru SENAMHI (Servicio Nacional de Meteorología e Hidrología) connector."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# Primary endpoint paths (sea/www layout).
_PATH_STATIONS_SEA = "/site/sea/www/estaciones"
_PATH_DATA_SEA = "/site/sea/www/datos"

# Alternate endpoint paths (mapas layout).
_PATH_STATIONS_MAPAS = "/mapas/mapa-estaciones/_dato_esta_tipo.php"
_PATH_DATA_MAPAS = "/mapas/mapa-estaciones/_dato_esta_datos.php"


@register("peru_senamhi")
class PeruSENAMHIConnector(BaseConnector):
    """Connector for Peru's SENAMHI streamflow data."""

    slug = "peru_senamhi"
    display_name = "Peru SENAMHI"
    base_url = "https://www.senamhi.gob.pe"
    country_codes = ["PE"]

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        self._use_mapas: bool | None = None  # None = not yet probed

    async def __aenter__(self) -> PeruSENAMHIConnector:
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(60.0, connect=10.0),
            headers={
                "User-Agent": "CSFS/0.1 (https://github.com/csfs)",
                "Accept": "application/json",
            },
            follow_redirects=True,
        )
        return self

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Fetch all hydrological stations from SENAMHI."""
        try:
            resp = await self._get(
                _PATH_STATIONS_SEA,
                params={"tipo": "HID", "format": "json"},
            )
            data = self._json_or_raise(resp)
            if isinstance(data, list):
                self._use_mapas = False
                return self._parse_stations(data)
        except DataFormatError:
            raise
        except (ConnectorError, httpx.HTTPStatusError):
            logger.info("peru_senamhi.sea_stations_failed_trying_mapas")

        # Fall back to the mapas layout.
        resp = await self._get(
            _PATH_STATIONS_MAPAS,
            params={"tipo": "HID", "formato": "json"},
        )
        data = self._json_or_raise(resp)
        if not isinstance(data, list):
            data = data.get("estaciones", data.get("data", []))
        self._use_mapas = True
        return self._parse_stations(data)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for *station_id*."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        if self._use_mapas is True:
            return await self._fetch_observations_mapas(
                native_id, station_id, start, end,
            )

        try:
            return await self._fetch_observations_sea(
                native_id, station_id, start, end,
            )
        except DataFormatError:
            raise
        except (ConnectorError, httpx.HTTPStatusError):
            logger.info(
                "peru_senamhi.sea_obs_failed_trying_mapas",
                station=native_id,
            )
            return await self._fetch_observations_mapas(
                native_id, station_id, start, end,
            )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent 24 h of observations."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # ------------------------------------------------------------------
    # SEA layout helpers
    # ------------------------------------------------------------------

    async def _fetch_observations_sea(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        resp = await self._get(_PATH_DATA_SEA, params={
            "estacion": native_id,
            "variable": "caudal",
            "inicio": start.strftime("%Y/%m/%d"),
            "fin": end.strftime("%Y/%m/%d"),
            "format": "json",
        })
        data = self._json_or_raise(resp)
        self._use_mapas = False
        return self._parse_observations(data, station_id)

    # ------------------------------------------------------------------
    # Mapas layout helpers
    # ------------------------------------------------------------------

    async def _fetch_observations_mapas(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        resp = await self._get(_PATH_DATA_MAPAS, params={
            "estacion": native_id,
            "variable": "caudal",
            "inicio": start.strftime("%Y/%m/%d"),
            "fin": end.strftime("%Y/%m/%d"),
            "formato": "json",
        })
        data = self._json_or_raise(resp)
        self._use_mapas = True
        return self._parse_observations(data, station_id)

    # ------------------------------------------------------------------
    # Shared parsers
    # ------------------------------------------------------------------

    def _parse_stations(self, records: list[dict]) -> list[Station]:
        """Parse station records from either endpoint layout."""
        stations: list[Station] = []
        for rec in records:
            try:
                codigo = str(rec.get("codigo", "")).strip()
                if not codigo:
                    continue

                lat = self._to_float(rec.get("latitud"))
                lon = self._to_float(rec.get("longitud"))
                if lat is None or lon is None:
                    continue

                stations.append(Station(
                    id=self._station_id(codigo),
                    provider=self.slug,
                    native_id=codigo,
                    name=rec.get("nombre") or codigo,
                    latitude=lat,
                    longitude=lon,
                    country_code="PE",
                    river=rec.get("rio"),
                    catchment_area_km2=self._to_float(
                        rec.get("cuenca_area", rec.get("area_drenaje")),
                    ),
                    is_active=True,
                ))
            except (ValueError, TypeError) as exc:
                logger.debug(
                    "peru_senamhi.skipping_station", error=str(exc),
                )
                continue
        return stations

    def _parse_observations(
        self,
        data: dict | list,
        station_id: str,
    ) -> TimeSeriesChunk:
        """Parse observation records from either endpoint layout."""
        raw_obs: list[dict] = []
        if isinstance(data, list):
            raw_obs = data
        elif isinstance(data, dict):
            raw_obs = data.get(
                "datos", data.get("data", data.get("observaciones", [])),
            )

        observations: list[Observation] = []
        for item in raw_obs:
            try:
                ts = self._parse_timestamp(
                    item.get("fecha", item.get("timestamp", "")),
                )
                if ts is None:
                    continue
                discharge = self._to_float(
                    item.get("valor", item.get("value")),
                )
                quality = self._map_quality(
                    item.get("calidad", item.get("quality")),
                )
                if discharge is None:
                    quality = QualityFlag.MISSING

                observations.append(Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=quality,
                ))
            except (ValueError, TypeError) as exc:
                logger.debug(
                    "peru_senamhi.skipping_observation", error=str(exc),
                )
                continue

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # ------------------------------------------------------------------
    # Shared utilities
    # ------------------------------------------------------------------

    def _json_or_raise(self, resp: httpx.Response) -> dict | list:
        """Decode JSON from *resp*, raising DataFormatError on failure."""
        try:
            result: dict | list = resp.json()
            return result
        except Exception as exc:
            raise DataFormatError(
                self.slug, f"Invalid JSON in response: {exc}",
            ) from exc

    @staticmethod
    def _parse_timestamp(value: str) -> datetime | None:
        """Parse an ISO-ish or SENAMHI timestamp into a UTC datetime."""
        if not value or not value.strip():
            return None
        text = value.strip()
        for fmt in (
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S",
            "%Y/%m/%d %H:%M:%S",
            "%Y/%m/%d",
            "%Y-%m-%d",
        ):
            try:
                dt = datetime.strptime(text, fmt)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=UTC)
                return dt
            except ValueError:
                continue
        return None

    @staticmethod
    def _to_float(value: object) -> float | None:
        """Coerce *value* to float, returning None for absent values."""
        if value is None:
            return None
        try:
            return float(str(value))
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _map_quality(raw: str | None) -> QualityFlag:
        """Map a SENAMHI quality string to the internal QualityFlag enum."""
        if raw is None:
            return QualityFlag.RAW
        mapping: dict[str, QualityFlag] = {
            "bueno": QualityFlag.GOOD,
            "good": QualityFlag.GOOD,
            "dudoso": QualityFlag.SUSPECT,
            "suspect": QualityFlag.SUSPECT,
            "estimado": QualityFlag.ESTIMATED,
            "estimated": QualityFlag.ESTIMATED,
        }
        return mapping.get(raw.lower().strip(), QualityFlag.RAW)
