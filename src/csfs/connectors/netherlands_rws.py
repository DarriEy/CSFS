"""Rijkswaterstaat connector — Dutch national water management authority."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# Maximum hours span per request.  The waterinfo chart API uses a relative
# hour window (e.g. ``-168,0``).  We cap individual requests to 7 days and
# stitch larger ranges together in :pymethod:`fetch_observations`.
_MAX_HOURS_PER_REQUEST = 168


@register("netherlands_rws")
class NetherlandsRWSConnector(BaseConnector):
    slug = "netherlands_rws"
    display_name = "Rijkswaterstaat Waterinfo (NL)"
    base_url = "https://waterinfo.rws.nl/api"
    country_codes = ["NL"]

    # The parameter ID for volumetric discharge on the waterinfo chart API.
    _PARAMETER_ID = "Qvol"

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all RWS stations that expose discharge data."""
        resp = await self._get(
            "/chart/stations",
            params={"parameterIds": self._PARAMETER_ID},
        )
        try:
            data = resp.json()
        except ValueError as exc:
            raise DataFormatError(self.slug, "Station response is not valid JSON") from exc
        return self._parse_stations(data)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for *station_id* between *start* and *end*.

        The waterinfo chart API uses relative hour offsets from "now", so we
        convert the absolute window into the required ``values=-H,0`` format.
        When the requested range exceeds the per-request cap we issue
        multiple calls and merge the results.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")
        now = datetime.now(UTC)

        # Clamp end to now — the API cannot return future data.
        effective_end = min(end, now)
        if start >= effective_end:
            return TimeSeriesChunk(
                station_id=station_id,
                provider=self.slug,
                observations=[],
                fetched_at=now,
            )

        all_observations: list[Observation] = []
        chunk_end = effective_end

        # Walk backwards from *effective_end* in _MAX_HOURS_PER_REQUEST steps.
        while chunk_end > start:
            chunk_start = max(start, chunk_end - timedelta(hours=_MAX_HOURS_PER_REQUEST))
            hours_back = int((now - chunk_start).total_seconds() / 3600)
            hours_forward = int((now - chunk_end).total_seconds() / 3600)
            values_param = f"-{hours_back},{-hours_forward if hours_forward else 0}"

            resp = await self._get(
                "/chart/get",
                params={
                    "locationSlug": native_id,
                    "parameterIds": self._PARAMETER_ID,
                    "values": values_param,
                },
            )

            try:
                data = resp.json()
            except ValueError as exc:
                raise DataFormatError(
                    self.slug, "Observation response is not valid JSON"
                ) from exc

            all_observations.extend(self._parse_observations(data, station_id))
            chunk_end = chunk_start

        # De-duplicate by timestamp (overlapping windows can repeat points).
        seen: set[datetime] = set()
        unique: list[Observation] = []
        for obs in sorted(all_observations, key=lambda o: o.timestamp):
            if obs.timestamp not in seen:
                seen.add(obs.timestamp)
                unique.append(obs)

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=unique,
            fetched_at=datetime.now(UTC),
        )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent 48 hours of discharge data."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=48),
            end=now,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_stations(self, data: dict | list) -> list[Station]:
        """Parse the station-listing JSON into :class:`Station` objects.

        The expected shape is either a bare list of station dicts or a dict
        with a top-level key (e.g. ``"stations"``) containing that list.
        We accept both forms defensively.
        """
        if isinstance(data, dict):
            # Try common wrapper keys.
            for key in ("stations", "features", "results"):
                if key in data:
                    entries = data[key]
                    break
            else:
                # Fallback: take the first list-typed value.
                entries = next(
                    (v for v in data.values() if isinstance(v, list)), []
                )
        elif isinstance(data, list):
            entries = data
        else:
            logger.warning("unexpected_station_payload", provider=self.slug, type=type(data).__name__)
            return []

        stations: list[Station] = []
        for entry in entries:
            try:
                slug = entry.get("slug") or entry.get("locationSlug") or ""
                name = entry.get("name") or entry.get("locationName") or slug

                coords = entry.get("coordinates", {})
                lat = float(coords.get("latitude", entry.get("latitude", 0.0)))
                lon = float(coords.get("longitude", entry.get("longitude", 0.0)))

                if not slug:
                    continue

                stations.append(Station(
                    id=self._station_id(slug),
                    provider=self.slug,
                    native_id=slug,
                    name=name,
                    latitude=lat,
                    longitude=lon,
                    country_code="NL",
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

    def _parse_observations(self, data: dict | list, station_id: str) -> list[Observation]:
        """Extract observation points from the chart/get response.

        The response is typically a dict containing one or more series.
        Each series has a ``data`` list of ``[timestamp_ms, value]`` pairs,
        or alternatively dicts with ``dateTime`` / ``value`` keys.  We try
        several shapes to stay resilient against minor API changes.
        """
        series_list = self._extract_series(data)

        observations: list[Observation] = []
        for point in series_list:
            try:
                ts, value = self._parse_point(point)
            except (ValueError, TypeError, KeyError) as exc:
                logger.debug(
                    "point_parse_skipped",
                    provider=self.slug,
                    error=str(exc),
                )
                continue

            discharge = float(value) if value is not None else None
            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=discharge,
                quality=QualityFlag.RAW if discharge is not None else QualityFlag.MISSING,
            ))
        return observations

    # ------------------------------------------------------------------
    # Low-level parsing utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _extract_series(data: dict | list) -> list:
        """Dig into the response structure and return the flat point list."""
        if isinstance(data, list):
            return data

        # Common patterns: {series: [{data: [...]}]}, {data: [...]}, etc.
        for key in ("series", "data", "values", "results"):
            if key in data:
                inner = data[key]
                if isinstance(inner, list) and inner and isinstance(inner[0], dict) and "data" in inner[0]:
                    # [{data: [points...]}, ...] — take first series
                    result: list = inner[0].get("data", [])
                    return result
                if isinstance(inner, list):
                    return inner

        return []

    @staticmethod
    def _parse_point(point) -> tuple[datetime, float | None]:
        """Return ``(timestamp, value)`` from a single data point.

        Supports:
        * ``[epoch_ms, value]`` — two-element list/tuple
        * ``{"dateTime": "...", "value": ...}`` — dict form
        * ``{"timestamp": "...", "value": ...}`` — dict form
        """
        if isinstance(point, (list, tuple)):
            if len(point) < 2:
                raise ValueError("Point list too short")
            raw_ts, raw_val = point[0], point[1]
            if isinstance(raw_ts, (int, float)):
                ts = datetime.fromtimestamp(raw_ts / 1000, tz=UTC)
            else:
                ts = datetime.fromisoformat(str(raw_ts))
            value = None if raw_val is None else float(raw_val)
            return ts, value

        if isinstance(point, dict):
            raw_ts = point.get("dateTime") or point.get("timestamp") or point["dateTime"]
            if isinstance(raw_ts, (int, float)):
                ts = datetime.fromtimestamp(raw_ts / 1000, tz=UTC)
            else:
                ts = datetime.fromisoformat(str(raw_ts))
            raw_val = point.get("value")
            value = None if raw_val is None else float(raw_val)
            return ts, value

        raise TypeError(f"Unsupported point type: {type(point).__name__}")
