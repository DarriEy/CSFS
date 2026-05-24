"""Bosnia FHMZ connector — Federalni hidrometeorološki zavod.

The Federal Hydrometeorological Institute of Bosnia and Herzegovina (FHMZ)
has very limited API support.  Most data is published via PDF yearbooks.
This connector uses a seed-station approach (similar to the Japan MLIT
connector) to provide a curated list of major gauging stations.

Endpoints used
--------------
* Station listing:
  Backed by a curated seed list of ~20 known stations.  No reliable live
  discovery endpoint exists.

* Observations (best-effort):
  GET /latinica/HIDRO/api/podaci?stanica={id}&format=json
  May return ``[{datum, protok}, ...]`` or fail entirely.

The connector is written very defensively — this source has minimal
API support and endpoints may be unreliable or unavailable.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Curated seed stations -- major discharge gauging points in Bosnia.
# Format: (native_id, name, latitude, longitude, river)
# ---------------------------------------------------------------------------
_SEED_STATIONS: list[tuple[str, str, float, float, str]] = [
    ("1001", "Sarajevo - Bentbaša", 43.8600, 18.4350, "Miljacka"),
    ("1002", "Reljevo", 43.8800, 18.3200, "Bosna"),
    ("1003", "Doboj", 44.7300, 18.0800, "Bosna"),
    ("1004", "Maglaj", 44.5500, 18.1000, "Bosna"),
    ("1005", "Zenica", 44.2000, 17.9100, "Bosna"),
    ("1006", "Modriča", 44.9500, 18.3000, "Bosna"),
    ("1007", "Banja Luka", 44.7700, 17.1900, "Vrbas"),
    ("1008", "Jajce", 44.3400, 17.2700, "Vrbas"),
    ("1009", "Bihać", 44.8100, 15.8700, "Una"),
    ("1010", "Martin Brod", 44.4800, 16.0800, "Una"),
    ("1011", "Mostar", 43.3400, 17.8100, "Neretva"),
    ("1012", "Konjic", 43.6500, 17.9600, "Neretva"),
    ("1013", "Jablanica", 43.6600, 17.7600, "Neretva"),
    ("1014", "Čapljina", 43.1200, 17.6900, "Neretva"),
    ("1015", "Drvar", 44.3700, 16.3800, "Unac"),
    ("1016", "Livno", 43.8300, 17.0100, "Bistrica"),
    ("1017", "Travnik", 44.2300, 17.6600, "Lašva"),
    ("1018", "Tuzla", 44.5400, 18.6700, "Jala"),
    ("1019", "Goražde", 43.6700, 18.9800, "Drina"),
    ("1020", "Višegrad", 43.7800, 19.2900, "Drina"),
]


@register("bosnia_fhmz")
class BosniaFhmzConnector(BaseConnector):
    """Connector for Bosnia's FHMZ hydrological data.

    Uses a seed-station approach since FHMZ has very limited API
    support.  Observation fetching is best-effort.
    """

    slug = "bosnia_fhmz"
    display_name = "FHMZ (Bosnia and Herzegovina)"
    base_url = "https://www.fhmzbih.gov.ba"
    country_codes = ["BA"]

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return curated discharge stations.

        Always returns the seed list.  No reliable live discovery
        endpoint exists for FHMZ.
        """
        return [
            self._build_seed_station(row) for row in _SEED_STATIONS
        ]

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for *station_id* (best-effort).

        The FHMZ API is unreliable; failures are handled gracefully
        by returning an empty ``TimeSeriesChunk``.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        params: dict[str, str] = {
            "stanica": native_id,
            "format": "json",
        }

        try:
            resp = await self._get(
                "/latinica/HIDRO/api/podaci",
                params=params,
            )
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "observations_fetch_failed",
                provider=self.slug,
                station=native_id,
                status=exc.response.status_code,
            )
            return self._empty_chunk(station_id)
        except Exception as exc:
            logger.warning(
                "observations_fetch_error",
                provider=self.slug,
                station=native_id,
                error=str(exc),
            )
            return self._empty_chunk(station_id)

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

    def _build_seed_station(
        self,
        row: tuple[str, str, float, float, str],
    ) -> Station:
        """Create a Station model from a seed-list tuple."""
        native_id, name, lat, lon, river = row
        return Station(
            id=self._station_id(native_id),
            provider=self.slug,
            native_id=native_id,
            name=name,
            latitude=lat,
            longitude=lon,
            country_code="BA",
            river=river,
        )

    def _empty_chunk(self, station_id: str) -> TimeSeriesChunk:
        """Return an empty TimeSeriesChunk for failed requests."""
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )

    def _parse_observations(
        self,
        data: dict | list,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Parse the FHMZ observations response into a ``TimeSeriesChunk``.

        Expected shape::

            [
                {"datum": "2024-06-01T12:00:00", "protok": 34.5},
                ...
            ]

        The response may also be wrapped in a dict.  Client-side date
        filtering is applied.
        """
        if isinstance(data, list):
            items = data
        elif isinstance(data, dict):
            items = data.get("podaci", data.get("data", []))
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

            value = entry.get("protok")
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
