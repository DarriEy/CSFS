"""WAMIS connector — South Korea Water Management Information System."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()


@register("south_korea_wamis")
class SouthKoreaWamisConnector(BaseConnector):
    slug = "south_korea_wamis"
    display_name = "WAMIS (South Korea)"
    base_url = "http://www.wamis.go.kr/openapi"
    country_codes = ["KR"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all river water-level / discharge stations."""
        params: dict[str, str] = {
            "stn_type": "1",
            "output": "json",
        }
        api_key = self.config.get("api_key", "")
        if api_key:
            params["apikey"] = api_key

        resp = await self._get("/wkw/rf_dubrfobs", params=params)
        payload = self._safe_json(resp)
        items = self._extract_items(payload)
        return self._parse_stations(items)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge / water-level observations for a station over a time range."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        params: dict[str, str] = {
            "stn_id": native_id,
            "start_dt": start.strftime("%Y%m%d"),
            "end_dt": end.strftime("%Y%m%d"),
            "output": "json",
        }
        api_key = self.config.get("api_key", "")
        if api_key:
            params["apikey"] = api_key

        resp = await self._get("/wkw/rf_dubrfobs", params=params)
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

    def _safe_json(self, resp) -> dict:  # type: ignore[type-arg]
        """Decode JSON from a response, wrapping errors."""
        try:
            result: dict = resp.json()  # type: ignore[type-arg]
            return result
        except Exception as exc:
            raise DataFormatError(
                self.slug, f"Response is not valid JSON: {exc}"
            ) from exc

    def _extract_items(self, payload: dict) -> list[dict]:
        """Navigate the WAMIS envelope to reach the items list.

        Expected structure: ``{"header": {...}, "body": {"items": [...]}}``
        but we defensively handle several alternative shapes the API may
        return (flat list, ``{"content": [...]}``, etc.).
        """
        if isinstance(payload, list):
            return payload

        # Standard path
        body = payload.get("body")
        if isinstance(body, dict):
            items = body.get("items")
            if isinstance(items, list):
                return items

        # Fallback: top-level "items"
        items = payload.get("items")
        if isinstance(items, list):
            return items

        # Fallback: top-level "content"
        content = payload.get("content")
        if isinstance(content, list):
            return content

        # Fallback: top-level "data"
        data = payload.get("data")
        if isinstance(data, list):
            return data

        # Nothing found — return empty rather than crashing
        logger.warning(
            "wamis_unexpected_payload",
            provider=self.slug,
            keys=list(payload.keys()) if isinstance(payload, dict) else type(payload).__name__,
        )
        return []

    def _parse_stations(self, items: list[dict]) -> list[Station]:
        """Parse station items into Station models."""
        stations: list[Station] = []
        for entry in items:
            native_id = str(entry.get("stn_id", "")).strip()
            if not native_id:
                continue

            lat = self._to_float(entry.get("lat"))
            lon = self._to_float(entry.get("lon"))
            if lat is None or lon is None:
                logger.warning(
                    "station_missing_coords",
                    provider=self.slug,
                    station=native_id,
                )
                continue

            try:
                stations.append(
                    Station(
                        id=self._station_id(native_id),
                        provider=self.slug,
                        native_id=native_id,
                        name=entry.get("stn_nm", native_id),
                        latitude=lat,
                        longitude=lon,
                        country_code="KR",
                        river=entry.get("river_nm"),
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
        self, items: list[dict], station_id: str
    ) -> TimeSeriesChunk:
        """Parse observation items into a TimeSeriesChunk."""
        observations: list[Observation] = []
        for entry in items:
            ts = self._parse_obs_datetime(entry.get("obs_dt", ""))
            if ts is None:
                continue

            discharge = self._to_float(entry.get("fw_flux"))
            water_level = self._to_float(entry.get("fw_lvl"))
            # Prefer discharge; fall back to water level if discharge is absent
            value = discharge if discharge is not None else water_level
            quality = QualityFlag.RAW if value is not None else QualityFlag.MISSING

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

    @staticmethod
    def _parse_obs_datetime(raw: str) -> datetime | None:
        """Parse WAMIS observation timestamps (YYYYMMDD or YYYYMMDDHH)."""
        raw = str(raw).strip()
        if not raw:
            return None
        try:
            if len(raw) == 10:
                # YYYYMMDDHH
                return datetime.strptime(raw, "%Y%m%d%H").replace(tzinfo=UTC)
            if len(raw) == 8:
                # YYYYMMDD
                return datetime.strptime(raw, "%Y%m%d").replace(tzinfo=UTC)
        except ValueError:
            pass
        # Last resort: try ISO format
        try:
            return datetime.fromisoformat(raw).replace(tzinfo=UTC)
        except ValueError:
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
