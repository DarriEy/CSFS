"""BAFU Hydrodaten connector — Swiss federal hydrological gauging stations."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()


@register("switzerland_bafu")
class SwitzerlandBafuConnector(BaseConnector):
    slug = "switzerland_bafu"
    display_name = "BAFU Hydrodaten (Switzerland)"
    base_url = "https://www.hydrodaten.admin.ch"
    country_codes = ["CH"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all stations that measure discharge (Abfluss)."""
        try:
            resp = await self._get("/graphs/messstationen_uebersicht.json")
        except (httpx.HTTPStatusError, ConnectorError) as exc:
            logger.error(
                "station_listing_failed",
                provider=self.slug,
                error=str(exc),
            )
            raise ConnectorError(
                self.slug, f"Failed to fetch station listing: {exc}"
            ) from exc

        try:
            data = resp.json()
        except ValueError as exc:
            raise DataFormatError(
                self.slug, "Station listing response is not valid JSON"
            ) from exc

        return self._parse_stations(data)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge measurements for a station over a time range.

        BAFU's JSON endpoints return recent data (typically the last few days).
        The *start* / *end* window is used to filter the returned records
        client-side; BAFU does not support server-side date range queries on
        its public JSON API.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")
        observations = await self._fetch_discharge_json(native_id, station_id)

        # Client-side date filtering
        filtered = [
            obs for obs in observations
            if start <= obs.timestamp <= end
        ]

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=filtered,
            fetched_at=datetime.now(UTC),
        )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent discharge observations."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_stations(self, data: dict) -> list[Station]:
        """Parse the station overview JSON.

        The response is a JSON object keyed by station ID, each value
        containing metadata and a *parameters* list.  Only stations whose
        parameters include ``"Abfluss"`` (discharge) are returned.
        """
        stations: list[Station] = []

        if not isinstance(data, dict):
            logger.warning(
                "unexpected_station_format",
                provider=self.slug,
                type=type(data).__name__,
            )
            return stations

        for native_id, entry in data.items():
            if not isinstance(entry, dict):
                continue

            # Only keep stations that measure discharge
            parameters = entry.get("parameters", [])
            if not isinstance(parameters, list):
                continue
            if "Abfluss" not in parameters:
                continue

            try:
                coords = entry.get("Koordinaten", {}) or {}
                lat = float(coords.get("lat", 0.0))
                lng = float(coords.get("lng", 0.0))

                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=str(native_id),
                    name=entry.get("Name", ""),
                    latitude=lat,
                    longitude=lng,
                    country_code="CH",
                    river=entry.get("GewässerName"),
                ))
            except (ValueError, KeyError, TypeError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    station=native_id,
                    error=str(exc),
                )
                continue

        return stations

    async def _fetch_discharge_json(
        self, native_id: str, station_id: str
    ) -> list[Observation]:
        """Try known BAFU endpoint patterns for discharge data.

        BAFU's public API is undocumented and endpoint patterns can vary
        across stations.  We attempt several URL patterns and return
        results from the first one that succeeds.
        """
        url_patterns = [
            f"/graphs/messwerte/{native_id}_Abfluss_m3s_10min.json",
            f"/graphs/messwerte/lhg_{native_id}_AbflussPegel_10min.json",
        ]

        last_exc: Exception | None = None
        for pattern in url_patterns:
            try:
                resp = await self._get(pattern)
                return self._parse_timeseries(resp.json(), station_id)
            except (httpx.HTTPStatusError, ConnectorError) as exc:
                logger.debug(
                    "endpoint_pattern_failed",
                    provider=self.slug,
                    station=native_id,
                    pattern=pattern,
                    error=str(exc),
                )
                last_exc = exc
                continue
            except (ValueError, DataFormatError) as exc:
                logger.debug(
                    "parse_failed_for_pattern",
                    provider=self.slug,
                    station=native_id,
                    pattern=pattern,
                    error=str(exc),
                )
                last_exc = exc
                continue

        # All patterns exhausted — log a warning and return empty
        logger.warning(
            "no_discharge_data",
            provider=self.slug,
            station=native_id,
            last_error=str(last_exc),
        )
        return []

    def _parse_timeseries(
        self, data: dict | list, station_id: str
    ) -> list[Observation]:
        """Parse BAFU timeseries JSON into a list of Observations.

        BAFU typically returns either:
        - A JSON object with a ``"data"`` key containing ``[[epoch_ms, value], ...]``
        - A bare JSON array of ``[epoch_ms, value]`` pairs
        """
        # Extract the data array from whichever shape we received
        if isinstance(data, dict):
            series = data.get("data") or data.get("values") or data.get("measurements")
            if series is None:
                # Try the first list-like value in the dict
                for val in data.values():
                    if isinstance(val, list):
                        series = val
                        break
            if series is None:
                raise DataFormatError(
                    self.slug,
                    "Timeseries JSON has no recognisable data array",
                )
        elif isinstance(data, list):
            series = data
        else:
            raise DataFormatError(
                self.slug,
                f"Unexpected timeseries type: {type(data).__name__}",
            )

        observations: list[Observation] = []
        for record in series:
            try:
                if isinstance(record, (list, tuple)) and len(record) >= 2:
                    epoch_ms, value = record[0], record[1]
                    ts = datetime.fromtimestamp(epoch_ms / 1000.0, tz=UTC)
                    discharge = float(value) if value is not None else None
                elif isinstance(record, dict):
                    raw_ts = record.get("timestamp") or record.get("time")
                    if raw_ts is None:
                        continue
                    if isinstance(raw_ts, (int, float)):
                        ts = datetime.fromtimestamp(raw_ts / 1000.0, tz=UTC)
                    else:
                        ts = datetime.fromisoformat(str(raw_ts))
                    value = record.get("value") or record.get("discharge")
                    discharge = float(value) if value is not None else None
                else:
                    continue

                observations.append(Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=QualityFlag.RAW if discharge is not None else QualityFlag.MISSING,
                ))
            except (ValueError, TypeError, OverflowError) as exc:
                logger.debug(
                    "observation_parse_skipped",
                    provider=self.slug,
                    record=str(record)[:200],
                    error=str(exc),
                )
                continue

        return observations
