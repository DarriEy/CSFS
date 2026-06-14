"""Chile DGA connector -- Direccion General de Aguas (ArcGIS REST).

The Chilean DGA publishes hydrometric station metadata through an
ArcGIS REST MapServer.  No authentication is required.

Endpoint used
-------------
* Station metadata:
  GET https://rest-sit.mop.gob.cl/arcgis/rest/services/DGA/Red_Hidrometrica/MapServer/0/query
      ?where=1%3D1&outFields=*&f=json&resultRecordCount=1000
  Returns JSON with a ``features`` array containing station attributes
  (name, coordinates, type, drainage area, installation dates, etc.).
  1,330+ hydrometric stations.

**Limitation:** Only station metadata is available through this
confirmed endpoint.  No time-series observations endpoint has been
verified; ``fetch_observations()`` returns an empty ``TimeSeriesChunk``.
"""

from __future__ import annotations

import contextlib
from datetime import UTC, datetime

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

_QUERY_PARAMS = {
    "where": "1=1",
    "outFields": "*",
    "f": "json",
    "resultRecordCount": "1000",
}


@register("chile_dga")
class ChileDgaConnector(BaseConnector):
    """Connector for Chile's DGA hydrometric station metadata.

    Provides station listing only -- no time-series observations
    endpoint has been confirmed working.
    """

    slug = "chile_dga"
    display_name = "DGA (Chile)"
    base_url = "https://rest-sit.mop.gob.cl"
    country_codes = ["CL"]

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return hydrometric stations from the DGA ArcGIS service.

        Paginates using resultOffset to fetch all ~5,000 stations.
        """
        all_features: list[dict] = []
        offset = 0
        page_size = 1000
        # The DGA network has ~5,000 stations (~5 pages). Cap pagination so a
        # misbehaving ArcGIS endpoint -- e.g. one that ignores resultOffset and
        # keeps returning the first page -- cannot loop indefinitely and stall
        # the acquisition run.
        max_pages = 50
        path = "/arcgis/rest/services/DGA/Red_Hidrometrica/MapServer/0/query"

        for page in range(max_pages):
            params = {
                "where": "1=1",
                "outFields": "*",
                "f": "json",
                "resultRecordCount": str(page_size),
                "resultOffset": str(offset),
            }
            try:
                resp = await self._get(path, params=params)
            except httpx.HTTPStatusError as exc:
                if not all_features:
                    raise ConnectorError(
                        self.slug,
                        f"Failed to fetch stations: HTTP {exc.response.status_code}",
                    ) from exc
                break

            data = resp.json()
            if "error" in data:
                if not all_features:
                    msg = data["error"].get("message", "Unknown ArcGIS error")
                    raise ConnectorError(self.slug, f"ArcGIS error: {msg}")
                break

            features = data.get("features", [])
            if not features:
                break

            all_features.extend(features)
            if len(features) < page_size:
                break
            offset += page_size
        else:
            logger.warning(
                "pagination_cap_reached",
                provider=self.slug,
                max_pages=max_pages,
                fetched=len(all_features),
            )

        logger.info("stations_fetched", provider=self.slug, count=len(all_features))
        return self._parse_features(all_features)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Return an empty chunk -- no observations endpoint confirmed.

        The DGA ArcGIS service provides station metadata only.
        No time-series observations endpoint has been verified as
        working, so this method returns an empty ``TimeSeriesChunk``
        and logs a warning.
        """
        logger.warning(
            "observations_not_available",
            provider=self.slug,
            station=station_id,
            reason=(
                "DGA ArcGIS provides station metadata only; "
                "no confirmed observations endpoint"
            ),
        )
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )

    # -----------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------

    def _parse_features(
        self,
        features: list[dict],
    ) -> list[Station]:
        """Parse ArcGIS JSON features into ``Station`` models."""
        stations: list[Station] = []
        for feat in features:
            attrs = feat.get("attributes", {})
            geom = feat.get("geometry", {})

            native_id = self._extract_native_id(attrs)
            if not native_id:
                continue

            lat, lon = self._extract_coords(attrs, geom)
            if lat is None or lon is None:
                logger.warning(
                    "station_missing_coords",
                    provider=self.slug,
                    station=native_id,
                )
                continue

            name = str(
                attrs.get("nombre_estacion")
                or attrs.get("NombreEstacion")
                or attrs.get("NOMBRE")
                or native_id
            )

            catchment_raw = (
                attrs.get("area_drenaje")
                or attrs.get("AreaDrenaje")
            )
            catchment: float | None = None
            if catchment_raw is not None:
                with contextlib.suppress(ValueError, TypeError):
                    catchment = float(str(catchment_raw))

            elevation_raw = (
                attrs.get("altitud")
                or attrs.get("Altitud")
            )
            elevation: float | None = None
            if elevation_raw is not None:
                with contextlib.suppress(ValueError, TypeError):
                    elevation = float(str(elevation_raw))

            river = (
                attrs.get("rio")
                or attrs.get("Rio")
                or attrs.get("nombre_rio")
            )

            stations.append(Station(
                id=self._station_id(native_id),
                provider=self.slug,
                native_id=native_id,
                name=name,
                latitude=lat,
                longitude=lon,
                country_code="CL",
                river=str(river) if river else None,
                catchment_area_km2=catchment,
                elevation_m=elevation,
            ))

        return stations

    @staticmethod
    def _extract_native_id(attrs: dict) -> str:
        """Extract a non-empty station identifier from attributes."""
        for key in (
            "codigo_estacion",
            "CodigoEstacion",
            "OBJECTID",
            "codigo_bna",
        ):
            val = attrs.get(key)
            if val is not None:
                val_str = str(val).strip()
                if val_str:
                    return val_str
        return ""

    @staticmethod
    def _extract_coords(
        attrs: dict,
        geom: dict,
    ) -> tuple[float | None, float | None]:
        """Extract (lat, lon) from geometry or attribute fields."""
        # ArcGIS geometry: {"x": lon, "y": lat}
        if "x" in geom and "y" in geom:
            try:
                return (
                    float(str(geom["y"])),
                    float(str(geom["x"])),
                )
            except (ValueError, TypeError):
                pass

        # Fallback to attribute fields
        lat_raw = attrs.get("latitud") or attrs.get("Latitud")
        lon_raw = attrs.get("longitud") or attrs.get("Longitud")
        if lat_raw is not None and lon_raw is not None:
            try:
                return (
                    float(str(lat_raw)),
                    float(str(lon_raw)),
                )
            except (ValueError, TypeError):
                pass

        return None, None
