"""WRA connector — Taiwan Water Resources Agency open-data API."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# Field-name mappings: English → Chinese fallback
_STATION_FIELDS = {
    "id": ("StationIdentifier", "測站代碼"),
    "name": ("StationName", "測站名稱"),
    "lat": ("Latitude", "緯度"),
    "lon": ("Longitude", "經度"),
    "river": ("RiverName", "河川名稱"),
    "basin": ("BasinName", "流域名稱"),
    "catchment_area": ("CatchmentArea", "集水區面積"),
    "obs_start": ("ObservationStartDate", "觀測開始日期"),
}

_OBS_FIELDS = {
    "date": ("RecordDate", "日期"),
    "discharge": ("Discharge", "流量"),
    "water_level": ("WaterLevel", "水位"),
}


def _get(entry: dict, *keys: str):
    """Return the first non-None value for the given keys."""
    for k in keys:
        val = entry.get(k)
        if val is not None:
            return val
    return None


@register("taiwan_wra")
class TaiwanWRAConnector(BaseConnector):
    slug = "taiwan_wra"
    display_name = "WRA (Taiwan)"
    base_url = "https://opendata.wra.gov.tw/api/v1"
    country_codes = ["TW"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all river-flow stations from the WRA API."""
        resp = await self._get(
            "/RiverFlowStation", params={"format": "json"},
        )
        payload = self._safe_json(resp)
        items = self._extract_items(payload)
        return self._parse_stations(items)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge / water-level observations for one station."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        params: dict[str, str] = {
            "StationIdentifier": native_id,
            "StartDate": start.strftime("%Y-%m-%d"),
            "EndDate": end.strftime("%Y-%m-%d"),
            "format": "json",
        }

        resp = await self._get("/RiverFlowData", params=params)
        payload = self._safe_json(resp)
        items = self._extract_items(payload)
        return self._parse_observations(items, station_id)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent observations (last 24 h)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _safe_json(self, resp) -> dict:
        """Decode JSON from a response, wrapping errors."""
        try:
            result: dict = resp.json()
            return result
        except Exception as exc:
            raise DataFormatError(
                self.slug, f"Response is not valid JSON: {exc}"
            ) from exc

    def _extract_items(self, payload) -> list[dict]:
        """Navigate the WRA envelope to reach the records list.

        The API may return the data in several shapes: a bare list,
        ``{"data": [...]}``, ``{"records": [...]}``, or an object with
        top-level ``RecordCount`` alongside a list field.
        """
        if isinstance(payload, list):
            return payload

        # Check for auth/error responses
        if isinstance(payload, dict) and payload.get("success") is False:
            logger.error(
                "wra_api_error",
                provider=self.slug,
                code=payload.get("code"),
                message=payload.get("s_message"),
            )
            return []

        for key in ("payload", "data", "records", "items", "content"):
            candidate = payload.get(key)
            if isinstance(candidate, list):
                return candidate

        # Some WRA endpoints nest inside "responseData"
        inner = payload.get("responseData")
        if isinstance(inner, dict):
            for key in ("data", "records", "items"):
                candidate = inner.get(key)
                if isinstance(candidate, list):
                    return candidate
            if isinstance(inner, list):
                return inner

        logger.warning(
            "wra_unexpected_payload",
            provider=self.slug,
            keys=(
                list(payload.keys())
                if isinstance(payload, dict)
                else type(payload).__name__
            ),
        )
        return []

    def _parse_stations(self, items: list[dict]) -> list[Station]:
        """Parse station records into Station models."""
        stations: list[Station] = []
        for entry in items:
            en, zh = _STATION_FIELDS["id"]
            native_id = str(_get(entry, en, zh) or "").strip()
            if not native_id:
                continue

            en_lat, zh_lat = _STATION_FIELDS["lat"]
            en_lon, zh_lon = _STATION_FIELDS["lon"]
            lat = self._to_float(_get(entry, en_lat, zh_lat))
            lon = self._to_float(_get(entry, en_lon, zh_lon))
            if lat is None or lon is None:
                logger.warning(
                    "station_missing_coords",
                    provider=self.slug,
                    station=native_id,
                )
                continue

            en_name, zh_name = _STATION_FIELDS["name"]
            en_river, zh_river = _STATION_FIELDS["river"]
            en_area, zh_area = _STATION_FIELDS["catchment_area"]
            en_start, zh_start = _STATION_FIELDS["obs_start"]

            name = _get(entry, en_name, zh_name) or native_id
            river = _get(entry, en_river, zh_river)
            catchment = self._to_float(
                _get(entry, en_area, zh_area)
            )
            obs_start = self._parse_date(
                _get(entry, en_start, zh_start)
            )

            try:
                stations.append(
                    Station(
                        id=self._station_id(native_id),
                        provider=self.slug,
                        native_id=native_id,
                        name=name,
                        latitude=lat,
                        longitude=lon,
                        country_code="TW",
                        river=river,
                        catchment_area_km2=catchment,
                        record_start=obs_start,
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

    def _parse_observations(
        self, items: list[dict], station_id: str,
    ) -> TimeSeriesChunk:
        """Parse observation records into a TimeSeriesChunk."""
        observations: list[Observation] = []
        en_date, zh_date = _OBS_FIELDS["date"]
        en_q, zh_q = _OBS_FIELDS["discharge"]
        en_wl, zh_wl = _OBS_FIELDS["water_level"]

        for entry in items:
            ts = self._parse_obs_datetime(
                _get(entry, en_date, zh_date) or ""
            )
            if ts is None:
                continue

            discharge = self._to_float(_get(entry, en_q, zh_q))
            water_level = self._to_float(_get(entry, en_wl, zh_wl))
            value = (
                discharge if discharge is not None else water_level
            )
            quality = (
                QualityFlag.RAW
                if value is not None
                else QualityFlag.MISSING
            )

            observations.append(
                Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=value,
                    quality=quality,
                )
            )

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # ------------------------------------------------------------------
    # Parsing utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_obs_datetime(raw: str) -> datetime | None:
        """Parse WRA observation timestamps.

        Handles ISO-8601 dates (``YYYY-MM-DD``), compact dates
        (``YYYYMMDD``), and datetime with time component.
        """
        raw = str(raw).strip()
        if not raw:
            return None

        for fmt in ("%Y-%m-%d", "%Y-%m-%dT%H:%M:%S", "%Y%m%d"):
            try:
                return datetime.strptime(raw, fmt).replace(
                    tzinfo=UTC,
                )
            except ValueError:
                continue

        # Last resort: fromisoformat (handles e.g. +08:00 offsets)
        try:
            return datetime.fromisoformat(raw).replace(tzinfo=UTC)
        except ValueError:
            return None

    @staticmethod
    def _parse_date(raw) -> datetime | None:
        """Parse a date string into a datetime, or return None."""
        if raw is None:
            return None
        raw = str(raw).strip()
        if not raw:
            return None
        for fmt in ("%Y-%m-%d", "%Y/%m/%d", "%Y%m%d"):
            try:
                return datetime.strptime(raw, fmt).replace(
                    tzinfo=UTC,
                )
            except ValueError:
                continue
        return None

    @staticmethod
    def _to_float(val) -> float | None:
        """Convert a value to float, returning None on failure."""
        if val is None or val == "":
            return None
        try:
            return float(val)
        except (ValueError, TypeError):
            return None
