# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Peru connector -- SENAMHI (PHISIS real-time hydro map).

VERDICT (2026-06): NOT FIXABLE for CSFS -- SENAMHI's open real-time
hydrological feed publishes **water level only** (``Nivel promedio diario
(m)``), not discharge (caudal, m3/s).

Investigation
-------------
The public real-time station map lives at::

    https://www.senamhi.gob.pe/?p=estaciones
        -> iframe https://www.senamhi.gob.pe/mapas/mapa-estaciones-2/
           -> js/mapa-estaciones-v.1.0.0.js  (Leaflet app)
           -> station catalog inlined as the ``PruebaTest`` JS array
              (~984 stations, 250 hydrological ``ico:"H"``, 70 real-time)

Clicking a station opens an HTML chart, not a JSON feed::

    .../mapas/mapa-estaciones-2/map_red_graf.php?cod=<code>&estado=REAL
        &tipo_esta=H&cate=<HLM|HLG|...>&cod_old=<code>

Across every probed real-time hydro station (codes 230503, 221106, 250303,
250405, 250308; categories HLM and HLG) the only series returned is::

    name: 'Nivel promedio diario (m)'   valueSuffix: ' m'

with zero occurrences of "caudal" / "m3/s". The CSV download form posts to
an obfuscated ``mH1.php`` endpoint that serves the same level series.

Because the CSFS ``Observation`` model only carries ``discharge_m3s`` and no
discharge is openly served, this connector cannot contribute streamflow.
It therefore returns empty results gracefully (no fake seed) while staying
registered and importable. Keep inventory status ``research``.

References
----------
- Map: https://www.senamhi.gob.pe/?p=estaciones
- Detail: https://www.senamhi.gob.pe/mapas/mapa-estaciones-2/map_red_graf.php
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.models import Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

_SENAMHI_BASE_URL = "https://www.senamhi.gob.pe"


@register("peru_senamhi")
class PeruSenamhiConnector(BaseConnector):
    """Connector for Peru SENAMHI (PHISIS real-time hydro map).

    SENAMHI's open real-time hydrological feed publishes water level only
    (no discharge), so this connector yields no observations. It remains
    registered and importable so the roster stays intact.
    """

    slug = "peru_senamhi"
    display_name = "SENAMHI (Peru)"
    base_url = _SENAMHI_BASE_URL
    country_codes = ["PE"]

    async def fetch_stations(self) -> list[Station]:
        """Return no stations.

        SENAMHI's open real-time hydro feed only exposes water level
        (``Nivel promedio diario (m)``), never discharge (caudal, m3/s).
        CSFS tracks discharge only, so there are no usable stations and we
        emit no fabricated seed.
        """
        logger.info(
            "no_open_discharge",
            provider=self.slug,
            reason="SENAMHI real-time hydro map serves water level (m) only, "
            "no discharge (caudal m3/s)",
        )
        return []

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Return an empty chunk.

        No open discharge feed exists upstream; SENAMHI publishes water
        level only. We return gracefully with zero observations rather than
        raising, so bulk acquisition is not disrupted.
        """
        logger.info(
            "no_open_discharge",
            provider=self.slug,
            station=station_id,
            reason="SENAMHI real-time hydro map serves water level (m) only, "
            "no discharge (caudal m3/s)",
        )
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )
