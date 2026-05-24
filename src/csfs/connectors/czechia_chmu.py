"""CHMU connector — Czech Hydrometeorological Institute hydrological data."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()


@register("czechia_chmu")
class CzechiaChmuConnector(BaseConnector):
    slug = "czechia_chmu"
    display_name = "CHMU Hydrology (Czechia)"
    base_url = "https://hydro.chmi.cz/hppsoldv"
    country_codes = ["CZ"]

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)

    async def fetch_stations(self) -> list[Station]:
        """Return all active hydrological stations from CHMU."""
        try:
            resp = await self._get(
                "/hpps_act_rain.php",
                params={"output": "json"},
            )
        except Exception as exc:
            raise ConnectorError(
                self.slug, f"Failed to fetch stations: {exc}"
            ) from exc

        return self._parse_stations(resp.json())

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for a station.

        Note: the CHMU endpoint returns current/recent data for a given
        station; the start/end parameters are retained for interface
        compatibility but the API may not support arbitrary date ranges.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        try:
            resp = await self._get(
                "/hpps_prutoky.php",
                params={"DBCN": native_id, "output": "json"},
            )
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch observations for station '{native_id}': {exc}",
            ) from exc

        return self._parse_observations(resp.json(), station_id, start, end)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent discharge observations (last 24 h)."""
        from datetime import timedelta

        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_stations(self, data: list | dict) -> list[Station]:
        """Parse the CHMU station list JSON response.

        Expected shape: a list of dicts or a dict with a 'stations' key
        containing dicts with fields DBCN, NAZEV, ZEMEPISNASIRKA,
        ZEMEPISNADELKA, TOK.
        """
        entries: list[dict] = []
        if isinstance(data, list):
            entries = data
        elif isinstance(data, dict):
            entries = data.get("stations", [])

        stations: list[Station] = []
        for entry in entries:
            try:
                native_id = str(entry.get("DBCN", ""))
                if not native_id:
                    continue

                name = entry.get("NAZEV", "")
                lat_raw = entry.get("ZEMEPISNASIRKA")
                lon_raw = entry.get("ZEMEPISNADELKA")
                lat = float(lat_raw) if lat_raw is not None else 0.0
                lon = float(lon_raw) if lon_raw is not None else 0.0
                river = entry.get("TOK")

                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=name or "",
                    latitude=lat,
                    longitude=lon,
                    country_code="CZ",
                    river=river,
                ))
            except (ValueError, KeyError, TypeError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    station=entry.get("DBCN", "unknown"),
                    error=str(exc),
                )
                continue
        return stations

    def _parse_observations(
        self,
        data: list | dict,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Parse the CHMU discharge observations JSON response.

        Expected shape: a list of dicts or a dict with a 'data' key
        containing dicts with fields DTM (datetime) and PRUTOK
        (discharge in m3/s).
        """
        entries: list[dict] = []
        if isinstance(data, list):
            entries = data
        elif isinstance(data, dict):
            entries = data.get("data", [])

        observations: list[Observation] = []
        for entry in entries:
            try:
                ts_str = entry.get("DTM", "")
                if not ts_str:
                    continue
                ts = datetime.fromisoformat(ts_str)
            except (ValueError, TypeError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid timestamp in observation: {exc}",
                ) from exc

            raw_value = entry.get("PRUTOK")
            try:
                discharge = float(raw_value) if raw_value is not None else None
            except (ValueError, TypeError):
                discharge = None

            quality = QualityFlag.RAW if discharge is not None else QualityFlag.MISSING

            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=discharge,
                quality=quality,
            ))

        # Filter to the requested time range if timestamps are tz-aware/naive consistently
        filtered = []
        for obs in observations:
            obs_ts = obs.timestamp.replace(tzinfo=None) if obs.timestamp.tzinfo else obs.timestamp
            s = start.replace(tzinfo=None) if start.tzinfo else start
            e = end.replace(tzinfo=None) if end.tzinfo else end
            if s <= obs_ts <= e:
                filtered.append(obs)

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=filtered,
            fetched_at=datetime.now(UTC),
        )
