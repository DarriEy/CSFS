"""Philippines connector -- PAGASA (GeoRiskPH).

PAGASA (Philippine Atmospheric, Geophysical and Astronomical Services
Administration) publishes real-time hydrometeorological data through the
GeoRiskPH portal (https://portal.georisk.gov.ph) and assorted ArcGIS-hosted
dashboards.

RESEARCH VERDICT (2026-06): NOT FIXABLE as a discharge connector.
--------------------------------------------------------------------
CSFS only ingests streamflow **discharge (m³/s)**.  Every PAGASA / GeoRiskPH
hydro endpoint that exists publishes **water level (m)** and/or **rainfall
(mm)** -- never discharge:

* ``portal.georisk.gov.ph/arcgis/rest/services/PAGASA/PAGASA/MapServer``
  is a *rainfall-forecast* product.  Its "Dams" / "Riverbasins" group layers
  (``dams_frr01..12``, ``River Basin Frr01..12``) carry zonal rainfall
  statistics (``min``/``max``/``mean``/``sum`` in mm) sampled at dam points and
  catchment polygons -- no water level, no reservoir elevation, no discharge.
* The "Real Time Water Level in Major Rivers" dashboard is backed by hosted
  feature services (``gauges_2_view``, ``hydrostations`` on
  ``services3.arcgis.com/J7ZFXmR8rSmQ3FGf``).  Their fields are ``water_level``
  (m), ``rain_fall`` (mm) and alert thresholds (``alertpull`` / ``minorpull`` /
  ``majorpull``) -- again, no discharge column.

Because no discharge data is available, this connector is kept **registered and
importable** but returns an **empty** result set gracefully (no fabricated seed
stations, no synthetic observations).  It remains a research entry pending a
provider that actually exposes flow.

References
----------
- Portal: https://portal.georisk.gov.ph
- PAGASA ArcGIS folder:
  https://portal.georisk.gov.ph/arcgis/rest/services/PAGASA
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.models import Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# Real, reachable GeoRiskPH ArcGIS host (the old georisk.gov.ph/arcgis path 404s).
_BASE_URL = "https://portal.georisk.gov.ph/arcgis/rest/services"


@register("philippines_pagasa")
class PhilippinesPagasaConnector(BaseConnector):
    """Connector for Philippines PAGASA (GeoRiskPH).

    No public PAGASA endpoint exposes streamflow discharge (m³/s); only water
    level (m) and rainfall (mm) are available.  This connector therefore returns
    empty results gracefully while staying registered and importable.
    """

    slug = "philippines_pagasa"
    display_name = "PAGASA (Philippines)"
    base_url = _BASE_URL
    country_codes = ["PH"]

    async def fetch_stations(self) -> list[Station]:
        """Return no stations.

        PAGASA / GeoRiskPH publish only water level and rainfall -- no
        discharge -- so there is nothing to ingest for a streamflow service.
        Returns an empty list rather than fabricating a seed roster.
        """
        logger.info(
            "no_discharge_available",
            provider=self.slug,
            reason="PAGASA/GeoRiskPH expose water level (m) and rainfall (mm) "
            "only; no discharge (m3/s).",
        )
        return []

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Return an empty chunk -- no discharge data is published by PAGASA."""
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )
