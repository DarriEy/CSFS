"""Germany NRW connector -- ELWAS-WEB / OpenGeodata.NRW (no open discharge API).

NOT FUNCTIONAL by design. North Rhine-Westphalia exposes no open, machine-readable
DISCHARGE (Abfluss, m3/s) endpoint:
- ELWAS-WEB is an interactive JSF map app; its REST paths 404.
- The NRW hydrology portal (hochwasserportal.nrw.de, a KISTERS WISKI/KiWIS app)
  serves only pre-rendered static JSON snapshots whose layers are water level,
  temperature and precipitation -- no discharge.
- The OpenGeodata.NRW "zeitreihen" CSV path some code has guessed at returns 404.

The major NRW federal-waterway discharge gauges are already covered by
``germany_pegelonline``. This connector is therefore kept registered but returns
no data; it is marked ``status: research`` in the inventory. Re-investigate only
if NRW publishes an open Abfluss/Q feed.

References
----------
- ELWAS-WEB: https://www.elwasweb.nrw.de/
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.models import Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()


@register("germany_nrw")
class GermanyNRWConnector(BaseConnector):
    """NRW (Germany) -- no open discharge API; returns empty (see module docstring)."""

    slug = "germany_nrw"
    display_name = "NRW (Germany)"
    base_url = "https://www.elwasweb.nrw.de"
    country_codes = ["DE"]

    async def fetch_stations(self) -> list[Station]:
        """Return no stations: NRW publishes no open discharge catalogue."""
        logger.info("no_open_discharge", provider=self.slug)
        return []

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Return an empty chunk: NRW exposes only level/precip, no discharge."""
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )
