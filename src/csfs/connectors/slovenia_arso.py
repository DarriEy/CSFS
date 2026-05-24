"""Slovenia ARSO connector — Agencija Republike Slovenije za okolje.

ARSO operates Slovenia's environmental monitoring network, including
hydrological data via https://vode.arso.gov.si.

Endpoints used
--------------
* Station listing:
  GET /hidarhiv/pov_arhiv_tab.php?output=json
  Returns a JSON array of gauging stations.

* Observations:
  GET /hidarhiv/pov_arhiv_data.php?id={sifra}&output=json
  Returns ``[{datum, pretok}, ...]``.  Date filtering is done client-side.

Both endpoints may evolve; the connector is written defensively with
fallback parsing and clear error messages.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()


@register("slovenia_arso")
class SloveniaArsoConnector(BaseConnector):
    """Connector for Slovenia's ARSO hydrological data."""

    slug = "slovenia_arso"
    display_name = "ARSO Vode (Slovenia)"
    base_url = "https://vode.arso.gov.si"
    country_codes = ["SI"]

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all gauging stations from ARSO."""
        try:
            resp = await self._get(
                "/hidarhiv/pov_arhiv_tab.php",
                params={"output": "json"},
            )
        except httpx.HTTPStatusError as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch station list: "
                f"HTTP {exc.response.status_code}",
            ) from exc

        return self._parse_stations(resp.json())

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for *station_id* over [start, end].

        The ARSO archive endpoint does not support server-side date
        filtering, so we fetch the full dataset and filter in Python.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        try:
            resp = await self._get(
                "/hidarhiv/pov_arhiv_data.php",
                params={"id": native_id, "output": "json"},
            )
        except httpx.HTTPStatusError as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch observations for {native_id}: "
                f"HTTP {exc.response.status_code}",
            ) from exc

        return self._parse_observations(
            resp.json(), station_id, start, end,
        )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent discharge observations (last 24 h)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # -----------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------

    def _parse_stations(self, data: list[dict] | dict) -> list[Station]:
        """Parse the ARSO station-list JSON into ``Station`` models.

        The API may return a bare list or wrap it under a key.
        Both forms are handled defensively.
        """
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("stations", data.get("postaje", []))
        else:
            return []

        stations: list[Station] = []
        for entry in items:
            native_id = str(entry.get("sifra", "")).strip()
            if not native_id:
                continue

            lat = entry.get("lat") or entry.get("latitude")
            lon = entry.get("lon") or entry.get("longitude")
            if lat is None or lon is None:
                logger.warning(
                    "station_missing_coords",
                    provider=self.slug,
                    station=native_id,
                )
                continue

            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=entry.get("ime", native_id),
                    latitude=float(lat),
                    longitude=float(lon),
                    country_code="SI",
                    river=entry.get("vodotok"),
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
        data: dict | list,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Parse the ARSO observations response into a ``TimeSeriesChunk``.

        Expected shape::

            [
                {"datum": "2024-06-01T12:00:00", "pretok": 34.5},
                ...
            ]

        The response may also be wrapped in a dict.  Client-side date
        filtering is applied since the endpoint returns the full archive.
        """
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("podatki", data.get("data", []))
        else:
            raise DataFormatError(
                self.slug,
                f"Unexpected response type: {type(data).__name__}",
            )

        # Ensure start/end are offset-aware for comparison
        start_aware = (
            start if start.tzinfo else start.replace(tzinfo=UTC)
        )
        end_aware = (
            end if end.tzinfo else end.replace(tzinfo=UTC)
        )

        observations: list[Observation] = []
        for entry in items:
            try:
                ts = datetime.fromisoformat(entry["datum"])
            except (KeyError, ValueError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid or missing timestamp: {exc}",
                ) from exc

            # Ensure ts is offset-aware for comparison
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)

            if ts < start_aware or ts > end_aware:
                continue

            value = entry.get("pretok")
            discharge = float(value) if value is not None else None
            quality = (
                QualityFlag.MISSING
                if discharge is None
                else QualityFlag.RAW
            )

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
