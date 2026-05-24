"""Mexico CONAGUA (Comision Nacional del Agua) BANDAS/SINA connector."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# CONAGUA's APIs are poorly documented and endpoints change.
# We try multiple URL patterns for resilience.
_STATION_ENDPOINTS = [
    ("/api/estaciones", {"tipo": "H"}),
    ("/Estaciones.aspx", {"type": "hidrometrica", "format": "json"}),
]

_OBSERVATION_ENDPOINTS_TEMPLATES = [
    "/api/datos",
    "/DatosHidrometricos.aspx",
]

# Quality flag mapping from CONAGUA bandera codes to CSFS flags.
_QUALITY_MAP: dict[str | None, QualityFlag] = {
    None: QualityFlag.RAW,
    "": QualityFlag.RAW,
    "B": QualityFlag.GOOD,
    "E": QualityFlag.ESTIMATED,
    "S": QualityFlag.SUSPECT,
    "M": QualityFlag.MISSING,
}


@register("mexico_conagua")
class MexicoCONAGUAConnector(BaseConnector):
    slug = "mexico_conagua"
    display_name = "Mexico CONAGUA BANDAS/SINA"
    base_url = "https://sina.conagua.gob.mx/sina"
    country_codes = ["MX"]

    async def __aenter__(self) -> MexicoCONAGUAConnector:
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

    async def fetch_stations(self) -> list[Station]:
        """Fetch hydrometric stations from CONAGUA, trying multiple endpoints."""
        for path, params in _STATION_ENDPOINTS:
            try:
                resp = await self._get(path, params=params)
                stations = self._parse_stations(resp)
                if stations:
                    return stations
            except (ConnectorError, httpx.HTTPStatusError) as exc:
                logger.warning(
                    "station_endpoint_failed",
                    provider=self.slug,
                    path=path,
                    error=str(exc),
                )
                continue

        logger.warning("all_station_endpoints_failed", provider=self.slug)
        return []

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations, trying multiple endpoints."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        start_str = start.strftime("%Y-%m-%d")
        end_str = end.strftime("%Y-%m-%d")

        for base_path in _OBSERVATION_ENDPOINTS_TEMPLATES:
            try:
                params = self._build_obs_params(
                    base_path, native_id, start_str, end_str,
                )
                resp = await self._get(base_path, params=params)
                return self._parse_observations(resp, station_id)
            except (ConnectorError, httpx.HTTPStatusError) as exc:
                logger.warning(
                    "observation_endpoint_failed",
                    provider=self.slug,
                    path=base_path,
                    station=native_id,
                    error=str(exc),
                )
                continue

        logger.warning(
            "all_observation_endpoints_failed",
            provider=self.slug,
            station=native_id,
        )
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent 24h of observations."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # ------------------------------------------------------------------
    # Internal parsing helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _build_obs_params(
        path: str,
        native_id: str,
        start_str: str,
        end_str: str,
    ) -> dict[str, str]:
        """Build query parameters depending on the endpoint pattern."""
        if path.endswith(".aspx"):
            return {
                "estacion": native_id,
                "format": "json",
            }
        return {
            "estacion": native_id,
            "variable": "Q",
            "inicio": start_str,
            "fin": end_str,
        }

    def _parse_stations(self, resp: httpx.Response) -> list[Station]:
        """Parse a JSON station listing response into Station models."""
        try:
            data = resp.json()
        except ValueError as exc:
            raise ConnectorError(
                self.slug, f"Non-JSON station response: {exc}"
            ) from exc

        if isinstance(data, dict):
            # Some endpoints wrap the array in a key
            for key in ("estaciones", "data", "results"):
                if key in data and isinstance(data[key], list):
                    data = data[key]
                    break
            else:
                logger.warning(
                    "unexpected_station_json_shape",
                    provider=self.slug,
                    keys=list(data.keys()) if isinstance(data, dict) else None,
                )
                return []

        if not isinstance(data, list):
            return []

        stations: list[Station] = []
        for item in data:
            station = self._parse_single_station(item)
            if station is not None:
                stations.append(station)
        return stations

    def _parse_single_station(self, item: dict) -> Station | None:
        """Convert a single JSON station record to a Station, or None."""
        try:
            native_id = str(
                item.get("clave")
                or item.get("clave_estacion")
                or item.get("id")
                or ""
            ).strip()
            if not native_id:
                return None

            lat = _to_float(item.get("latitud") or item.get("lat"))
            lon = _to_float(item.get("longitud") or item.get("lon"))
            if lat is None or lon is None:
                return None

            name = str(
                item.get("nombre")
                or item.get("nombre_estacion")
                or native_id
            ).strip()

            return Station(
                id=self._station_id(native_id),
                provider=self.slug,
                native_id=native_id,
                name=name,
                latitude=lat,
                longitude=lon,
                country_code="MX",
                river=_str_or_none(
                    item.get("corriente") or item.get("rio")
                ),
                catchment_area_km2=_to_float(
                    item.get("area_cuenca") or item.get("area")
                ),
                is_active=True,
            )
        except (ValueError, TypeError, KeyError) as exc:
            logger.debug(
                "skipping_station",
                provider=self.slug,
                error=str(exc),
            )
            return None

    def _parse_observations(
        self,
        resp: httpx.Response,
        station_id: str,
    ) -> TimeSeriesChunk:
        """Parse a JSON observations response into a TimeSeriesChunk."""
        text = resp.text
        if not text or not text.strip():
            return TimeSeriesChunk(
                station_id=station_id,
                provider=self.slug,
                observations=[],
                fetched_at=datetime.now(UTC),
            )

        try:
            data = resp.json()
        except ValueError as exc:
            raise ConnectorError(
                self.slug,
                f"Non-JSON observation response: {exc}",
            ) from exc

        records = _extract_records(data)
        observations: list[Observation] = []
        for rec in records:
            obs = self._parse_single_observation(rec, station_id)
            if obs is not None:
                observations.append(obs)

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    def _parse_single_observation(
        self,
        rec: dict,
        station_id: str,
    ) -> Observation | None:
        """Convert a single JSON observation record, or None on failure."""
        try:
            raw_date = rec.get("fecha") or rec.get("date")
            if not raw_date:
                return None

            timestamp = _parse_datetime(str(raw_date))
            discharge = _to_float(rec.get("valor") or rec.get("value"))

            bandera = rec.get("bandera") or rec.get("flag")
            quality = _QUALITY_MAP.get(
                bandera, QualityFlag.RAW
            )
            if discharge is None:
                quality = QualityFlag.MISSING

            return Observation(
                station_id=station_id,
                timestamp=timestamp,
                discharge_m3s=discharge,
                quality=quality,
            )
        except (ValueError, TypeError) as exc:
            logger.debug(
                "skipping_observation",
                provider=self.slug,
                error=str(exc),
            )
            return None


# ------------------------------------------------------------------
# Module-level helper functions
# ------------------------------------------------------------------


def _extract_records(data: object) -> list[dict]:
    """Pull the observation records list out of various JSON shapes."""
    if isinstance(data, list):
        return data

    if isinstance(data, dict):
        for key in ("datos", "data", "observations", "results"):
            candidate = data.get(key)
            if isinstance(candidate, list):
                return candidate

    return []


def _to_float(value: object) -> float | None:
    """Safely convert a value to float, returning None on failure."""
    if value is None:
        return None
    try:
        return float(str(value))
    except (ValueError, TypeError):
        return None


def _str_or_none(value: object) -> str | None:
    """Return stripped string or None if empty/absent."""
    if value is None:
        return None
    s = str(value).strip()
    return s if s else None


def _parse_datetime(text: str) -> datetime:
    """Parse CONAGUA datetime strings in various formats."""
    for fmt in (
        "%Y-%m-%d %H:%M:%S",
        "%Y-%m-%dT%H:%M:%S",
        "%d/%m/%Y %H:%M:%S",
        "%Y-%m-%d",
        "%d/%m/%Y",
    ):
        try:
            return datetime.strptime(text.strip(), fmt).replace(tzinfo=UTC)
        except ValueError:
            continue
    raise ValueError(f"Unrecognized CONAGUA datetime format: {text!r}")
