"""Indonesia connector -- PUPR SDA (Ministry of Public Works), SIGI portal.

STATUS: NOT FIXABLE as a discharge connector (research / placeholder).

Investigation (2026-06, ArcGIS REST probing)
--------------------------------------------
The original connector targeted
``https://sigi.pu.go.id/portalpupr/rest/services`` which is a *dead* ArcGIS
Web Adaptor -- it returns HTTP 500 ("This web adaptor is not configured with
an ArcGIS Enterprise component"). That is the source of the original
``Server error '500'``; the request was not malformed, the endpoint simply
does not exist.

The live ArcGIS Enterprise (11.4) portal is at
``https://sigi.pu.go.id/portalpu/sharing/rest`` with hosted services under
``https://sigi.pu.go.id/serverpu/rest/services``. Searching that portal for
hydrology / "pos duga air" / telemetri / debit / TMA turns up only **static
GIS layers** (DAS basins, river lines, station-location point layers). The
only live "Pos Duga Air" service is ``Hosted/PDA_2025_32/FeatureServer/0``,
a 32-record station *inventory* with fields:

    nama_pos, kode_pos, nama_sunga (river), das (basin), jenis,
    jenis_alat (instrument), lintang/bujur (lat/lon), status

There are **no value fields** (no ``debit`` / discharge, no ``tma`` /
``tinggi_muka_air`` / water level) and **no related time-series tables**.
Records are flagged "Otomatis Telemetri" but the telemetry stream itself is
not exposed through any public ArcGIS service. Other PDA items the portal
indexes (``Pos_SDA``, ``Pos_Duga_Air_Tsunami``) are orphaned and 404 on the
server.

Conclusion: SIGI/PUPR publishes station *locations* only, not discharge
observations. There is no public endpoint that yields m3/s (or even water
level) time series, so this cannot back a discharge connector. The connector
is kept registered but degrades gracefully (empty results) instead of
raising, so it never breaks an acquisition cycle.

References
----------
- Portal (live): https://sigi.pu.go.id/portalpu/sharing/rest
- Dead web adaptor: https://sigi.pu.go.id/portalpupr/rest/services (HTTP 500)
- Inventory layer: https://sigi.pu.go.id/serverpu/rest/services/Hosted/PDA_2025_32/FeatureServer/0
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.models import (
    Station,
    TimeSeriesChunk,
)
from csfs.core.registry import register

logger = structlog.get_logger()

# Live ArcGIS Enterprise server context (the only reachable PDA layer). This is
# a station-location inventory with no discharge/level value fields, so it is
# used only to populate station metadata -- never for observations.
_SERVER_BASE_URL = "https://sigi.pu.go.id/serverpu/rest/services"
_PDA_LAYER_PATH = "Hosted/PDA_2025_32/FeatureServer/0"


@register("indonesia_pupr")
class IndonesiaPUPRConnector(BaseConnector):
    """Connector for Indonesia PUPR SDA (SIGI ArcGIS REST).

    PUPR's SIGI portal exposes only station *locations* (no discharge or
    water-level observations) via ArcGIS REST. This connector therefore
    surfaces station metadata when available but always returns empty
    observation chunks -- it cannot provide discharge (m3/s) data.
    """

    slug = "indonesia_pupr"
    display_name = "PUPR SDA (Indonesia)"
    base_url = _SERVER_BASE_URL
    country_codes = ["ID"]

    async def fetch_stations(self) -> list[Station]:
        """Return PDA station locations from the SIGI inventory layer.

        Degrades to an empty list on any error (the layer is metadata-only
        and not essential to the acquisition pipeline).
        """
        params = {
            "where": "1=1",
            "outFields": "*",
            "returnGeometry": "true",
            "outSR": "4326",
            "f": "json",
        }
        try:
            resp = await self._get(f"/{_PDA_LAYER_PATH}/query", params=params)
            data = resp.json()
        except Exception as exc:  # noqa: BLE001 - degrade gracefully, never break a cycle
            logger.warning(
                "fetch_stations_failed",
                provider=self.slug,
                error_type=type(exc).__name__,
                error=str(exc)[:160],
            )
            return []

        if isinstance(data, dict) and data.get("error"):
            logger.warning(
                "fetch_stations_arcgis_error",
                provider=self.slug,
                error=str(data["error"])[:160],
            )
            return []

        stations: list[Station] = []
        for feat in data.get("features", []):
            attrs = feat.get("attributes", {})
            geom = feat.get("geometry", {}) or {}

            native_id = attrs.get("kode_pos") or attrs.get("KODE_POS")
            if not native_id:
                continue
            native_id = str(native_id)

            lat = geom.get("y", attrs.get("lintang"))
            lon = geom.get("x", attrs.get("bujur"))
            try:
                latitude = float(lat) if lat is not None else None
                longitude = float(lon) if lon is not None else None
            except (TypeError, ValueError):
                latitude = longitude = None

            try:
                stations.append(
                    Station(
                        id=self._station_id(native_id),
                        provider=self.slug,
                        native_id=native_id,
                        name=attrs.get("nama_pos") or native_id,
                        latitude=latitude,
                        longitude=longitude,
                        country_code="ID",
                        river=attrs.get("nama_sunga") or attrs.get("nama_sungai"),
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

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Return an empty chunk -- SIGI/PUPR exposes no discharge time series.

        The SIGI ArcGIS portal publishes station locations only; there is no
        public endpoint with discharge (debit) or water-level (TMA) values, so
        no observations can be retrieved.
        """
        logger.debug(
            "no_observations_available",
            provider=self.slug,
            station=station_id,
            reason="SIGI/PUPR publishes station locations only (no discharge time series)",
        )
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )
