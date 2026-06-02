"""Romania connector -- INHGA (National Institute of Hydrology and Water Mgmt).

STATUS: research / not-fixable (no open machine-readable discharge API).

INHGA (Institutul National de Hidrologie si Gospodarire a Apelor), a subunit of
the "Apele Romane" (Romanian Waters) National Administration, publishes Romanian
hydrology online, but **only as human-readable products**:

- ``https://www.hidro.ro`` is a WordPress portal whose data products are PDF
  bulletins (``/bulletin_type/...`` -- daily/monthly hydrological bulletins) and
  hydrological warnings (``/warning_type/...``, ``/avertizari-2/``). The map page
  (``/harta``) is a Leaflet ``leaftile`` plugin that renders static basin
  shapefiles -- it exposes no per-station discharge time series.
- The ``/date-operative`` path that an earlier stub of this connector targeted
  returns HTTP 404; it does not exist.
- ``rowater.ro`` is the parent institution's site (no open data API).

"RoWaterAPI" (Geosciences 2026, doi:10.3390/geosciences16020087) is a *proposed*
Django/Kafka/PostGIS framework benchmarked under a simulated workload. As of
2026-06 it is not deployed as a public, unauthenticated, queryable discharge
endpoint, and the paper publishes no open base URL.

Consequently there is **no open endpoint** from which to harvest discharge
(``debit``, m3/s). This connector therefore stays in ``research`` status: it
remains registered and importable, ``fetch_stations`` returns an empty list, and
``fetch_observations`` returns an empty chunk instead of raising, so the
acquisition scheduler degrades gracefully.

Re-investigate if INHGA / Apele Romane ever publishes the RoWaterAPI (or any
WHOS/WMO-mediated) discharge endpoint openly.

References
----------
- Portal: https://www.hidro.ro/
- Parent institution: https://rowater.ro/
- RoWaterAPI: https://doi.org/10.3390/geosciences16020087
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

_INHGA_BASE_URL = "https://www.hidro.ro"


@register("romania_inhga")
class RomaniaInhgaConnector(BaseConnector):
    """Connector for Romania INHGA / "RoWater".

    No open machine-readable discharge API is available (see module docstring),
    so both fetch methods return empty results gracefully.
    """

    slug = "romania_inhga"
    display_name = "INHGA (Romania)"
    base_url = _INHGA_BASE_URL
    country_codes = ["RO"]

    async def fetch_stations(self) -> list[Station]:
        """Return an empty catalogue.

        INHGA exposes no open discharge station catalogue (only PDF bulletins
        and warnings), so there is nothing to enumerate.
        """
        logger.info(
            "no_open_discharge_api",
            provider=self.slug,
            detail="INHGA publishes only PDF bulletins/warnings; no station API",
        )
        return []

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Return an empty chunk.

        There is no open discharge (debit, m3/s) endpoint to query, so this is a
        graceful no-op rather than an error -- callers get an empty series.
        """
        logger.info(
            "no_open_discharge_api",
            provider=self.slug,
            station=station_id,
        )
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )
