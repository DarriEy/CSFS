"""Chile DGA (Dirección General de Aguas) SNIA connector."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# DGA quality strings mapped to CSFS quality flags.
_QUALITY_MAP: dict[str, QualityFlag] = {
    "bueno": QualityFlag.GOOD,
    "dudoso": QualityFlag.SUSPECT,
    "estimado": QualityFlag.ESTIMATED,
}

# Primary SNIA endpoint layout (legacy BNAConsultas).
_PATH_STATIONS_BNA = "/consultaEstaciones"
_PATH_DATA_BNA = "/consultaDatos"

# Alternate v1 API layout.
_PATH_STATIONS_V1 = "/stations"
_PATH_DATA_V1_TPL = "/stations/{id}/measurements"


@register("chile_dga")
class ChileDGAConnector(BaseConnector):
    """Connector for Chile's DGA streamflow data via SNIA."""

    slug = "chile_dga"
    display_name = "Chile DGA (SNIA)"
    base_url = "https://snia.mop.gob.cl/BNAConsultas/reportes"
    country_codes = ["CL"]

    # Alternate base URL tried when the primary layout fails.
    _alt_base_url = "https://snia.mop.gob.cl/api/v1"

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        self._use_v1: bool | None = None  # None = not yet probed

    async def __aenter__(self) -> ChileDGAConnector:
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
        """Fetch all fluviometric stations from DGA."""
        try:
            resp = await self._get(
                _PATH_STATIONS_BNA, params={"tipo": "FLU"},
            )
            data = self._json_or_raise(resp)
            if isinstance(data, list):
                self._use_v1 = False
                return self._parse_stations_bna(data)
        except DataFormatError:
            raise
        except (ConnectorError, httpx.HTTPStatusError):
            logger.info("chile_dga.bna_stations_failed_trying_v1")

        # Fall back to the v1 API layout.
        resp = await self._get_v1(
            _PATH_STATIONS_V1, params={"type": "fluviometric"},
        )
        data = self._json_or_raise(resp)
        if not isinstance(data, list):
            data = data.get("results", data.get("data", []))
        self._use_v1 = True
        return self._parse_stations_v1(data)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for *station_id* between *start* and *end*."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        if self._use_v1 is True:
            return await self._fetch_observations_v1(native_id, station_id, start, end)

        try:
            return await self._fetch_observations_bna(native_id, station_id, start, end)
        except DataFormatError:
            raise
        except (ConnectorError, httpx.HTTPStatusError):
            logger.info("chile_dga.bna_obs_failed_trying_v1", station=native_id)
            return await self._fetch_observations_v1(native_id, station_id, start, end)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent 24 h of observations."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # ------------------------------------------------------------------
    # BNA layout helpers
    # ------------------------------------------------------------------

    async def _fetch_observations_bna(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        resp = await self._get(_PATH_DATA_BNA, params={
            "estacion": native_id,
            "variable": "Q",
            "fechaInicio": start.strftime("%Y-%m-%d"),
            "fechaFin": end.strftime("%Y-%m-%d"),
        })
        data = self._json_or_raise(resp)
        self._use_v1 = False
        return self._parse_observations_bna(data, station_id)

    def _parse_stations_bna(self, records: list[dict]) -> list[Station]:
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

                vigente = rec.get("vigente")
                is_active = bool(vigente) if vigente is not None else True

                stations.append(Station(
                    id=self._station_id(codigo),
                    provider=self.slug,
                    native_id=codigo,
                    name=rec.get("nombre") or codigo,
                    latitude=lat,
                    longitude=lon,
                    country_code="CL",
                    river=rec.get("rio"),
                    catchment_area_km2=self._to_float(rec.get("area_drenaje")),
                    is_active=is_active,
                ))
            except (ValueError, TypeError) as exc:
                logger.debug("chile_dga.skipping_station", error=str(exc))
                continue
        return stations

    def _parse_observations_bna(self, data: dict | list, station_id: str) -> TimeSeriesChunk:
        raw_obs: list[dict] = []
        if isinstance(data, list):
            raw_obs = data
        elif isinstance(data, dict):
            raw_obs = data.get("datos", data.get("data", []))

        observations: list[Observation] = []
        for item in raw_obs:
            try:
                ts = self._parse_timestamp(item.get("fecha", ""))
                if ts is None:
                    continue
                discharge = self._to_float(item.get("valor"))
                quality = self._map_quality(item.get("calidad"))
                if discharge is None:
                    quality = QualityFlag.MISSING

                observations.append(Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=quality,
                ))
            except (ValueError, TypeError) as exc:
                logger.debug("chile_dga.skipping_observation", error=str(exc))
                continue

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # ------------------------------------------------------------------
    # V1 API layout helpers
    # ------------------------------------------------------------------

    async def _get_v1(self, path: str, params: dict | None = None) -> httpx.Response:
        """Issue a GET against the alternate v1 base URL."""
        url = self._alt_base_url + path
        resp = await self.client.get(url, params=params)
        if resp.status_code == 429:
            from csfs.core.exceptions import RateLimitError
            raise RateLimitError(self.slug, "Rate limited")
        if resp.status_code not in (200, 206):
            resp.raise_for_status()
        return resp

    async def _fetch_observations_v1(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        path = _PATH_DATA_V1_TPL.format(id=native_id)
        resp = await self._get_v1(path, params={
            "variable": "discharge",
            "from": start.strftime("%Y-%m-%d"),
            "to": end.strftime("%Y-%m-%d"),
        })
        data = self._json_or_raise(resp)
        self._use_v1 = True
        return self._parse_observations_v1(data, station_id)

    def _parse_stations_v1(self, records: list[dict]) -> list[Station]:
        stations: list[Station] = []
        for rec in records:
            try:
                sid = str(rec.get("id", rec.get("code", rec.get("codigo", "")))).strip()
                if not sid:
                    continue

                lat = self._to_float(rec.get("latitude", rec.get("latitud")))
                lon = self._to_float(rec.get("longitude", rec.get("longitud")))
                if lat is None or lon is None:
                    continue

                active = rec.get("active", rec.get("vigente"))
                is_active = bool(active) if active is not None else True

                stations.append(Station(
                    id=self._station_id(sid),
                    provider=self.slug,
                    native_id=sid,
                    name=rec.get("name", rec.get("nombre", sid)),
                    latitude=lat,
                    longitude=lon,
                    country_code="CL",
                    river=rec.get("river", rec.get("rio")),
                    catchment_area_km2=self._to_float(
                        rec.get("catchment_area_km2", rec.get("area_drenaje")),
                    ),
                    is_active=is_active,
                ))
            except (ValueError, TypeError) as exc:
                logger.debug("chile_dga.skipping_station_v1", error=str(exc))
                continue
        return stations

    def _parse_observations_v1(self, data: dict | list, station_id: str) -> TimeSeriesChunk:
        raw_obs: list[dict] = []
        if isinstance(data, list):
            raw_obs = data
        elif isinstance(data, dict):
            raw_obs = data.get("measurements", data.get("datos", data.get("data", [])))

        observations: list[Observation] = []
        for item in raw_obs:
            try:
                ts = self._parse_timestamp(
                    item.get("timestamp", item.get("fecha", "")),
                )
                if ts is None:
                    continue
                discharge = self._to_float(item.get("value", item.get("valor")))
                quality = self._map_quality(item.get("quality", item.get("calidad")))
                if discharge is None:
                    quality = QualityFlag.MISSING

                observations.append(Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=quality,
                ))
            except (ValueError, TypeError) as exc:
                logger.debug("chile_dga.skipping_observation_v1", error=str(exc))
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
                self.slug, f"Invalid JSON in response: {exc}"
            ) from exc

    @staticmethod
    def _parse_timestamp(value: str) -> datetime | None:
        """Parse an ISO-ish timestamp string into a UTC datetime."""
        if not value or not value.strip():
            return None
        text = value.strip()
        for fmt in (
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%dT%H:%M:%SZ",
            "%Y-%m-%dT%H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S",
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
        """Coerce *value* to float, returning None for absent / non-numeric values."""
        if value is None:
            return None
        try:
            return float(str(value))
        except (ValueError, TypeError):
            return None

    @staticmethod
    def _map_quality(raw: str | None) -> QualityFlag:
        """Map a DGA quality string to the internal QualityFlag enum."""
        if raw is None:
            return QualityFlag.RAW
        return _QUALITY_MAP.get(raw.lower().strip(), QualityFlag.RAW)
