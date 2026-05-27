"""Netherlands Rijkswaterstaat (RWS) connector — WFS-based hydrological data.

Rijkswaterstaat provides water-management data for the Netherlands via an
OGC WFS service.  No authentication is required.

Endpoints used
--------------
* Station listing:
  GET https://geo.rijkswaterstaat.nl/services/ogc/hws/DDAPI20/ows
      ?service=WFS&request=GetFeature
      &typeName=DDAPI20:locaties
      &outputFormat=application/json&maxFeatures=500
  Returns a GeoJSON FeatureCollection with station points.

* Stations with latest observations:
  GET https://geo.rijkswaterstaat.nl/services/ogc/hws/DDAPI20/ows
      ?service=WFS&request=GetFeature
      &typeName=DDAPI20:locatiesmetlaatstewaarneming
      &outputFormat=application/json&maxFeatures=500
  Returns a GeoJSON FeatureCollection whose properties include
  WAARDE_LAATSTE_METING (latest measurement value) and
  TIJDSTIP_LAATSTE_METING (timestamp of latest measurement).

Note: this is real-time latest-value only. The WFS endpoint does not
support historical range queries.
"""

from __future__ import annotations

import contextlib
from datetime import UTC, datetime

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import (
    Observation,
    QualityFlag,
    Station,
    TimeSeriesChunk,
)
from csfs.core.registry import register

logger = structlog.get_logger()

# Pre-built query strings to avoid httpx URL-encoding the colon in
# typeName (DDAPI20:locaties), which the WFS server rejects.
_WFS_QS_STATIONS = (
    "/services/ogc/hws/DDAPI20/ows"
    "?service=WFS&request=GetFeature"
    "&typeName=DDAPI20:locaties"
    "&outputFormat=application/json&maxFeatures=500"
)

_WFS_QS_LATEST = (
    "/services/ogc/hws/DDAPI20/ows"
    "?service=WFS&request=GetFeature"
    "&typeName=DDAPI20:locatiesmetlaatstewaarneming"
    "&outputFormat=application/json&maxFeatures=500"
)


@register("netherlands_rws")
class NetherlandsRwsConnector(BaseConnector):
    """Connector for the Netherlands Rijkswaterstaat WFS service."""

    slug = "netherlands_rws"
    display_name = "Rijkswaterstaat (Netherlands)"
    base_url = (
        "https://geo.rijkswaterstaat.nl"
    )
    country_codes = ["NL"]

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all stations from the DDAPI20:locaties WFS layer."""
        try:
            resp = await self._get(_WFS_QS_STATIONS)
        except httpx.HTTPStatusError as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch station list: "
                f"HTTP {exc.response.status_code}",
            ) from exc

        data = resp.json()
        features = data.get("features", [])
        return self._parse_station_features(features)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch the latest observation for *station_id*.

        The RWS WFS endpoint only provides the most recent measurement,
        not historical time series.  The *start* and *end* parameters
        are accepted for interface compatibility but are not used for
        server-side filtering.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        try:
            resp = await self._get(_WFS_QS_LATEST)
        except httpx.HTTPStatusError as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch observations: "
                f"HTTP {exc.response.status_code}",
            ) from exc

        data = resp.json()
        features = data.get("features", [])
        return self._parse_latest_features(
            features, native_id, station_id,
        )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent observation for a station."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id, start=now, end=now,
        )

    # -----------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------

    def _parse_station_features(
        self,
        features: list[dict],
    ) -> list[Station]:
        """Parse GeoJSON features from the locaties layer."""
        stations: list[Station] = []
        for feat in features:
            props = feat.get("properties", {})
            geom = feat.get("geometry") or {}

            native_id = str(
                props.get("LOCATIE_CODE", props.get("NAAM", "")),
            ).strip()
            if not native_id:
                continue

            coords = geom.get("coordinates")
            if (
                not isinstance(coords, (list, tuple))
                or len(coords) < 2
            ):
                logger.warning(
                    "station_missing_coords",
                    provider=self.slug,
                    station=native_id,
                )
                continue

            try:
                lon = float(str(coords[0]))
                lat = float(str(coords[1]))
            except (ValueError, TypeError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    station=native_id,
                    error=str(exc),
                )
                continue

            display_name = str(
                props.get("LOCATIE_NAAM", props.get("NAAM", "")),
            ).strip()

            stations.append(Station(
                id=self._station_id(native_id),
                provider=self.slug,
                native_id=native_id,
                name=display_name or native_id,
                latitude=lat,
                longitude=lon,
                country_code="NL",
            ))

        return stations

    def _parse_latest_features(
        self,
        features: list[dict],
        native_id: str,
        station_id: str,
    ) -> TimeSeriesChunk:
        """Extract the latest observation for *native_id* from WFS features."""
        observations: list[Observation] = []

        for feat in features:
            props = feat.get("properties", {})
            name = str(
                props.get("LOCATIE_CODE", props.get("NAAM", ""))
            ).strip()
            if name != native_id:
                continue

            ts_raw = props.get("TIJDSTIP_LAATSTE_METING")
            value_raw = props.get("WAARDE_LAATSTE_METING")

            if ts_raw is None:
                logger.warning(
                    "observation_missing_timestamp",
                    provider=self.slug,
                    station=native_id,
                )
                continue

            try:
                ts = datetime.fromisoformat(str(ts_raw))
            except ValueError:
                logger.warning(
                    "observation_invalid_timestamp",
                    provider=self.slug,
                    station=native_id,
                    timestamp=ts_raw,
                )
                continue

            discharge: float | None = None
            if value_raw is not None:
                with contextlib.suppress(ValueError, TypeError):
                    discharge = float(str(value_raw))

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
            break  # only one feature per station

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )
