"""IMGW connector — Polish Institute of Meteorology and Water Management."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()


@register("poland_imgw")
class PolandImgwConnector(BaseConnector):
    slug = "poland_imgw"
    display_name = "IMGW (Poland)"
    base_url = "https://danepubliczne.imgw.pl"
    country_codes = ["PL"]

    async def fetch_stations(self) -> list[Station]:
        """Return all hydrological stations discovered from real-time data."""
        data = await self._fetch_hydro_data()
        return self._parse_stations(data)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch current observations for a station from the real-time endpoint.

        IMGW does not expose a historical query API with date parameters.
        This fetches the latest snapshot; repeated cron runs accumulate a
        time series in the CSFS store.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")
        data = await self._fetch_hydro_data()
        return self._parse_observations(data, station_id, native_id)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent observation for a station."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        data = await self._fetch_hydro_data()
        return self._parse_observations(data, station_id, native_id)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _fetch_hydro_data(self) -> list[dict]:
        """GET /api/data/hydro/ and return the parsed JSON array."""
        try:
            resp = await self._get("/api/data/hydro/")
        except Exception as exc:
            raise ConnectorError(self.slug, f"Failed to fetch hydro data: {exc}") from exc
        try:
            result: list[dict] = resp.json()
            return result
        except ValueError as exc:
            raise DataFormatError(self.slug, "Response is not valid JSON") from exc

    def _parse_stations(self, data: list[dict]) -> list[Station]:
        """Parse the real-time hydro JSON into Station objects."""
        stations: list[Station] = []
        seen: set[str] = set()

        for entry in data:
            native_id = entry.get("id_stacji", "")
            if not native_id or native_id in seen:
                continue
            seen.add(native_id)

            name = entry.get("stacja", "")
            river = entry.get("rzeka") or None

            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=name,
                    latitude=0.0,
                    longitude=0.0,
                    country_code="PL",
                    river=river,
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
        native_id: str,
    ) -> TimeSeriesChunk:
        """Extract observations for *native_id* from the real-time JSON array."""
        observations: list[Observation] = []

        for entry in data:
            if entry.get("id_stacji") != native_id:
                continue

            # Discharge — the IMGW API key is "przeplyw" (ASCII, no Polish ł);
            # it is null for water-level-only gauges.
            raw_discharge = entry.get("przeplyw")
            if raw_discharge is None or str(raw_discharge).strip() == "":
                discharge = None
                quality = QualityFlag.MISSING
            else:
                try:
                    discharge = float(raw_discharge)
                    quality = QualityFlag.RAW
                except (ValueError, TypeError):
                    discharge = None
                    quality = QualityFlag.MISSING

            # Prefer the discharge reading's own timestamp; fall back to the
            # water-level timestamp for level-only gauges.
            raw_ts = (
                (entry.get("przeplyw_data") if discharge is not None else None)
                or entry.get("stan_wody_data_pomiaru")
                or entry.get("data_pomiaru")
            )
            if not raw_ts:
                continue
            try:
                ts = datetime.fromisoformat(raw_ts)
            except (ValueError, TypeError) as exc:
                logger.warning(
                    "timestamp_parse_failed",
                    provider=self.slug,
                    station=native_id,
                    raw=raw_ts,
                    error=str(exc),
                )
                continue

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
