"""Italy Tuscany connector -- SIR Toscana (level-only, no discharge).

NOT FUNCTIONAL by design. SIR Toscana's open real-time hydro feed
(monitoraggio/actions.php) serves only hydrometric LEVEL ("Livello idrometrico",
metres above sea level) -- there is no open discharge (portata, m3/s) endpoint.
Computed discharge / rating curves are not published openly.

Because CSFS tracks discharge only, this connector is kept registered but returns
no data; it is marked ``status: research`` in the inventory. Re-investigate only
if SIR Toscana publishes an open portata feed.

References
----------
- Portal: https://www.sir.toscana.it/
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.models import Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()


@register("italy_tuscany")
class ItalyTuscanyConnector(BaseConnector):
    """SIR Toscana (Italy) -- level-only; returns empty (see module docstring)."""

    slug = "italy_tuscany"
    display_name = "SIR Toscana (Italy)"
    base_url = "https://www.sir.toscana.it"
    country_codes = ["IT"]

    async def fetch_stations(self) -> list[Station]:
        """Return no stations: SIR Toscana publishes no open discharge data."""
        logger.info("no_open_discharge", provider=self.slug)
        return []

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Return an empty chunk: only hydrometric level is published, no discharge."""
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )
