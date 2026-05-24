"""ELWAS NRW connector — North Rhine-Westphalia water data service.

ELWAS (Elektronisches wasserwirtschaftliches Verbundsystem) provides
hydrological data for NRW via https://www.elwasweb.nrw.de.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()


@register("germany_nrw")
class GermanyNRWConnector(BaseConnector):
    """Connector for ELWAS NRW discharge monitoring network."""

    slug = "germany_nrw"
    display_name = "ELWAS NRW"
    base_url = "https://www.elwasweb.nrw.de"
    country_codes = ["DE"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all discharge (pegel) stations from ELWAS NRW."""
        try:
            resp = await self._get(
                "/elwas/stations",
                params={"type": "pegel", "format": "json"},
            )
            data = resp.json()
        except (
            ConnectorError,
            httpx.HTTPStatusError,
            httpx.HTTPError,
        ) as exc:
            logger.warning(
                "fetch_stations_failed",
                provider=self.slug,
                error=str(exc),
            )
            return []

        if not isinstance(data, list):
            if isinstance(data, dict):
                data = (
                    data.get("stations")
                    or data.get("data")
                    or data.get("features", [])
                )
            if not isinstance(data, list):
                logger.warning(
                    "stations_unexpected_format",
                    provider=self.slug,
                )
                return []

        return self._parse_stations(data)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for a station."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        try:
            resp = await self._get(
                "/elwas/data",
                params={
                    "station": native_id,
                    "param": "abfluss",
                    "from": start.strftime("%Y-%m-%d"),
                    "to": end.strftime("%Y-%m-%d"),
                    "format": "json",
                },
            )
            data = resp.json()
        except (
            ConnectorError,
            httpx.HTTPStatusError,
            httpx.HTTPError,
        ) as exc:
            logger.warning(
                "fetch_observations_failed",
                provider=self.slug,
                station=native_id,
                error=str(exc),
            )
            return TimeSeriesChunk(
                station_id=station_id,
                provider=self.slug,
                observations=[],
                fetched_at=datetime.now(UTC),
            )

        return self._parse_observations(data, station_id)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent 24 hours of observations."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # ------------------------------------------------------------------
    # Parsing helpers
    # ------------------------------------------------------------------

    def _parse_stations(self, data: list[dict]) -> list[Station]:
        """Parse station entries from JSON array."""
        stations: list[Station] = []
        for entry in data:
            try:
                native_id = str(
                    entry.get("id")
                    or entry.get("station_id")
                    or entry.get("messstelle_nr")
                    or ""
                )
                if not native_id:
                    continue

                name = str(
                    entry.get("name")
                    or entry.get("station_name")
                    or entry.get("messstellenname")
                    or ""
                )
                lat = _safe_float(
                    entry.get("latitude") or entry.get("lat"),
                )
                lon = _safe_float(
                    entry.get("longitude") or entry.get("lon"),
                )
                river = (
                    entry.get("river")
                    or entry.get("gewaesser")
                    or entry.get("water")
                )
                area = _safe_float(
                    entry.get("catchment_area_km2")
                    or entry.get("einzugsgebiet"),
                )

                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=name,
                    latitude=lat if lat is not None else 0.0,
                    longitude=lon if lon is not None else 0.0,
                    country_code="DE",
                    river=river,
                    catchment_area_km2=area,
                ))
            except (ValueError, KeyError, TypeError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    entry=str(entry)[:200],
                    error=str(exc),
                )
                continue
        return stations

    def _parse_observations(
        self,
        data: dict | list,
        station_id: str,
    ) -> TimeSeriesChunk:
        """Parse observation data from JSON response."""
        obs_list: list[dict] = []
        if isinstance(data, dict):
            obs_list = (
                data.get("values")
                or data.get("data")
                or data.get("measurements", [])
            )
        elif isinstance(data, list):
            obs_list = data

        if not isinstance(obs_list, list):
            obs_list = []

        observations: list[Observation] = []
        for entry in obs_list:
            try:
                ts = _parse_timestamp(entry)
                if ts is None:
                    continue
                value = (
                    entry.get("value")
                    or entry.get("wert")
                    or entry.get("abfluss")
                )
                discharge = _safe_float(value)
                quality = (
                    QualityFlag.RAW
                    if discharge is not None
                    else QualityFlag.MISSING
                )
                observations.append(Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=quality,
                ))
            except (ValueError, TypeError):
                continue

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------


def _safe_float(value: object) -> float | None:
    """Safely convert a value to float."""
    if value is None:
        return None
    try:
        return float(str(value))
    except (ValueError, TypeError):
        return None


def _parse_timestamp(entry: dict) -> datetime | None:
    """Try multiple field names and date formats."""
    raw = (
        entry.get("timestamp")
        or entry.get("datetime")
        or entry.get("date")
        or entry.get("zeit")
    )
    if raw is None:
        return None

    raw_str = str(raw).strip()
    if not raw_str:
        return None

    try:
        return datetime.fromisoformat(raw_str)
    except ValueError:
        pass

    for fmt in ("%Y-%m-%d %H:%M:%S", "%d.%m.%Y %H:%M", "%d.%m.%Y"):
        try:
            return datetime.strptime(raw_str, fmt)
        except ValueError:
            continue

    return None
