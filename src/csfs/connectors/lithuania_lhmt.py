"""Lithuania LHMT hydrology connector — Lithuanian Hydrometeorological Service."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()


@register("lithuania_lhmt")
class LithuaniaLHMTConnector(BaseConnector):
    """Connector for the Lithuanian Hydrometeorological Service (LHMT) API.

    API docs: https://api.meteo.lt/v1
    The hydrology endpoint was launched in November 2023.

    Note: this API provides water *level* observations, not discharge
    directly. Many Lithuanian stations have level-to-discharge rating
    curves applied externally.
    """

    slug = "lithuania_lhmt"
    display_name = "LHMT (Lithuania)"
    base_url = "https://api.meteo.lt/v1"
    country_codes = ["LT"]

    async def fetch_stations(self) -> list[Station]:
        """Return all hydrology stations from LHMT."""
        try:
            resp = await self._get("/hydro/stations")
        except Exception as exc:
            raise ConnectorError(
                self.slug, "Failed to fetch station list"
            ) from exc

        data = resp.json()
        if not isinstance(data, list):
            logger.warning(
                "unexpected_stations_format",
                provider=self.slug,
                type=type(data).__name__,
            )
            return []

        return self._parse_stations(data)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch water-level observations for a station over a date range.

        The LHMT API serves observations one day at a time, so this
        method iterates day-by-day from *start* to *end* (inclusive).
        """
        native_id = station_id.removeprefix(f"{self.slug}:")
        all_observations: list[Observation] = []

        current = start.date() if isinstance(start, datetime) else start
        end_date = end.date() if isinstance(end, datetime) else end

        while current <= end_date:
            date_str = current.isoformat()
            try:
                resp = await self._get(
                    f"/hydro/stations/{native_id}/observations/{date_str}",
                )
                body = resp.json()
                obs_list = self._extract_observations(body)
                all_observations.extend(
                    self._parse_observations(obs_list, station_id, start, end)
                )
            except (ConnectorError, DataFormatError):
                raise
            except Exception as exc:
                logger.warning(
                    "day_fetch_failed",
                    provider=self.slug,
                    station=native_id,
                    date=date_str,
                    error=str(exc),
                )
            current += timedelta(days=1)

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=all_observations,
            fetched_at=datetime.now(UTC),
        )

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

    def _parse_stations(self, data: list[dict]) -> list[Station]:
        """Parse the station list JSON into Station models."""
        stations: list[Station] = []
        for entry in data:
            code = entry.get("code")
            if not code:
                logger.warning(
                    "station_missing_code",
                    provider=self.slug,
                )
                continue

            native_id = str(code)
            coords = entry.get("coordinates") or {}

            try:
                lat = float(str(coords.get("latitude", 0)))
                lon = float(str(coords.get("longitude", 0)))
            except (ValueError, TypeError):
                lat, lon = 0.0, 0.0

            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=entry.get("name", native_id),
                    latitude=lat,
                    longitude=lon,
                    country_code="LT",
                    river=entry.get("waterBody"),
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

    @staticmethod
    def _extract_observations(body: dict | list) -> list[dict]:
        """Pull the observations list from a daily response.

        The LHMT API wraps observations in a JSON object with an
        ``observations`` key. Handle both the wrapper dict and a bare
        list defensively.
        """
        if isinstance(body, list):
            return body
        if isinstance(body, dict):
            obs = body.get("observations")
            if isinstance(obs, list):
                return obs
        return []

    def _parse_observations(
        self,
        data: list[dict],
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse observation records, filtering to the requested range."""
        observations: list[Observation] = []
        for entry in data:
            ts_raw = entry.get("observationTimeUtc")
            if not ts_raw:
                continue

            try:
                ts = datetime.fromisoformat(str(ts_raw))
            except (ValueError, TypeError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid timestamp in observation: {exc}",
                ) from exc

            # Make comparable: strip tz from ts when start/end are naive
            ts_cmp = ts.replace(tzinfo=None) if ts.tzinfo and not start.tzinfo else ts
            start_cmp = start.replace(tzinfo=None) if start.tzinfo is None and ts.tzinfo else start
            end_cmp = end.replace(tzinfo=None) if end.tzinfo is None and ts.tzinfo else end

            if not (start_cmp <= ts_cmp <= end_cmp):
                continue

            level = entry.get("waterLevel")
            discharge: float | None = None
            if level is not None:
                try:
                    discharge = float(str(level))
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
