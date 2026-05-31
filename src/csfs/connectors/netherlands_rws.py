"""Netherlands Rijkswaterstaat (RWS) connector — WaterWebservices DD-API.

Rijkswaterstaat publishes water-management data for the Netherlands via the
WaterWebservices "DD-API". The legacy ``waterwebservices.rijkswaterstaat.nl``
host was decommissioned and replaced by
``ddapi20-waterwebservices.rijkswaterstaat.nl`` (no authentication required).

Data model
----------
* Catalogue (station + parameter metadata):
  ``POST /METADATASERVICES/OphalenCatalogus``
  Returns ``AquoMetadataLijst`` (parameter combinations), ``LocatieLijst``
  (stations), and ``AquoMetadataLocatieLijst`` (which parameter is measured
  at which location). Discharge is Grootheid ``Q`` (Debiet) / Compartiment
  ``OW`` (Oppervlaktewater) / Eenheid ``m3/s``.

* Observations:
  ``POST /ONLINEWAARNEMINGENSERVICES/OphalenWaarnemingen``
  with a single ``Locatie`` (by ``Code``), an ``AquoPlusWaarnemingMetadata``
  describing the parameter, and a ``Periode`` (ISO-8601 with offset).
  The response nests measurements under
  ``WaarnemingenLijst[].MetingenLijst[]`` with ``Tijdstip`` and
  ``Meetwaarde.Waarde_Numeriek``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# Aquo identity for surface-water discharge in m3/s.
_GROOTHEID_Q = "Q"
_COMPARTIMENT_OW = "OW"
_EENHEID_M3S = "m3/s"

# RWS encodes missing/!measured values as a 999999999 sentinel.
_MISSING_SENTINEL = 1e8

# Cap each OphalenWaarnemingen request to a bounded window (native resolution
# is ~10 min, so a year is ~52k points); chunk longer ranges.
_MAX_WINDOW = timedelta(days=28)

_METADATA_PATH = "/METADATASERVICES/OphalenCatalogus"
_OBSERVATIONS_PATH = "/ONLINEWAARNEMINGENSERVICES/OphalenWaarnemingen"


def _status_to_quality(status: str | None) -> QualityFlag:
    """Map an RWS Statuswaarde to a CSFS quality flag."""
    if not status:
        return QualityFlag.RAW
    mapping = {
        "Definitief": QualityFlag.GOOD,
        "Gecontroleerd": QualityFlag.GOOD,
        "Ongecontroleerd": QualityFlag.RAW,
        "Voorlopig": QualityFlag.RAW,
    }
    return mapping.get(status, QualityFlag.RAW)


@register("netherlands_rws")
class NetherlandsRwsConnector(BaseConnector):
    """Connector for the Netherlands Rijkswaterstaat WaterWebservices DD-API."""

    slug = "netherlands_rws"
    display_name = "Rijkswaterstaat (Netherlands)"
    base_url = "https://ddapi20-waterwebservices.rijkswaterstaat.nl"
    country_codes = ["NL"]

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all surface-water discharge (Q, m3/s) stations."""
        catalogue = await self._post(
            _METADATA_PATH,
            {"CatalogusFilter": {
                "Grootheden": True,
                "Compartimenten": True,
                "Eenheden": True,
            }},
            error="Failed to fetch catalogue",
        )
        return self._parse_discharge_stations(catalogue)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for *station_id* over [start, end]."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        start = start if start.tzinfo else start.replace(tzinfo=UTC)
        end = end if end.tzinfo else end.replace(tzinfo=UTC)

        # Dedup by timestamp: the API returns multiple measurement groups
        # (differing only by sampling height) with identical timestamps.
        by_ts: dict[datetime, Observation] = {}
        for chunk_start, chunk_end in self._windows(start, end):
            payload = self._observation_payload(
                native_id, chunk_start, chunk_end,
            )
            data = await self._post(
                _OBSERVATIONS_PATH, payload,
                error=f"Failed to fetch observations for {native_id}",
            )
            for obs in self._parse_observations(data, station_id):
                by_ts.setdefault(obs.timestamp, obs)

        observations = [by_ts[ts] for ts in sorted(by_ts)]
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent discharge observations (last 6 h)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id, start=now - timedelta(hours=6), end=now,
        )

    # -----------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------

    async def _post(
        self, path: str, json_body: dict, error: str, timeout: float = 120.0,
    ) -> dict:
        """POST a JSON body and return the decoded response, raising on error."""
        try:
            resp = await self.client.post(path, json=json_body, timeout=timeout)
            if resp.status_code not in (200, 206):
                resp.raise_for_status()
        except httpx.HTTPStatusError as exc:
            raise ConnectorError(
                self.slug, f"{error}: HTTP {exc.response.status_code}",
            ) from exc

        data: dict = resp.json()
        if data.get("Succesvol") is False:
            raise ConnectorError(
                self.slug, f"{error}: {data.get('Foutmelding')}",
            )
        return data

    def _observation_payload(
        self, native_id: str, start: datetime, end: datetime,
    ) -> dict:
        """Build the OphalenWaarnemingen request body."""
        return {
            "AquoPlusWaarnemingMetadata": {
                "aquoMetadata": {
                    "Compartiment": {"Code": _COMPARTIMENT_OW},
                    "Grootheid": {"Code": _GROOTHEID_Q},
                    "Eenheid": {"Code": _EENHEID_M3S},
                },
            },
            "Locatie": {"Code": native_id},
            "Periode": {
                "Begindatumtijd": self._fmt_dt(start),
                "Einddatumtijd": self._fmt_dt(end),
            },
        }

    @staticmethod
    def _windows(
        start: datetime, end: datetime,
    ) -> list[tuple[datetime, datetime]]:
        """Split [start, end] into <= _MAX_WINDOW chunks."""
        if end <= start:
            return [(start, end)]
        windows: list[tuple[datetime, datetime]] = []
        cursor = start
        while cursor < end:
            chunk_end = min(cursor + _MAX_WINDOW, end)
            windows.append((cursor, chunk_end))
            cursor = chunk_end
        return windows

    @staticmethod
    def _fmt_dt(dt: datetime) -> str:
        """Format a datetime as ISO-8601 with milliseconds and offset."""
        dt = dt.astimezone(UTC)
        return dt.strftime("%Y-%m-%dT%H:%M:%S.000+00:00")

    def _parse_discharge_stations(self, catalogue: dict) -> list[Station]:
        """Resolve the Q/m3/s stations from an OphalenCatalogus response."""
        metadata = catalogue.get("AquoMetadataLijst", [])
        message_ids = {
            a.get("AquoMetadata_MessageID")
            for a in metadata
            if a.get("Grootheid", {}).get("Code") == _GROOTHEID_Q
            and a.get("Eenheid", {}).get("Code") == _EENHEID_M3S
        }
        message_ids.discard(None)
        if not message_ids:
            return []

        # Link list uses the (oddly cased) "AquoMetaData_MessageID" key.
        links = catalogue.get("AquoMetadataLocatieLijst", [])
        discharge_loc_ids = {
            link.get("Locatie_MessageID")
            for link in links
            if link.get("AquoMetaData_MessageID") in message_ids
        }

        stations: list[Station] = []
        for loc in catalogue.get("LocatieLijst", []):
            if loc.get("Locatie_MessageID") not in discharge_loc_ids:
                continue
            native_id = str(loc.get("Code", "")).strip()
            lat, lon = loc.get("Lat"), loc.get("Lon")
            if not native_id or lat is None or lon is None:
                continue
            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=str(loc.get("Naam") or native_id).strip(),
                    latitude=float(lat),
                    longitude=float(lon),
                    country_code="NL",
                ))
            except (ValueError, TypeError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug, station=native_id, error=str(exc),
                )
        return stations

    def _parse_observations(
        self, data: dict, station_id: str,
    ) -> list[Observation]:
        """Extract observations from an OphalenWaarnemingen response."""
        observations: list[Observation] = []
        for group in data.get("WaarnemingenLijst", []):
            for meting in group.get("MetingenLijst", []):
                ts_raw = meting.get("Tijdstip")
                if not ts_raw:
                    continue
                try:
                    ts = datetime.fromisoformat(str(ts_raw)).astimezone(UTC)
                except ValueError:
                    continue

                value = meting.get("Meetwaarde", {}).get("Waarde_Numeriek")
                meta = meting.get("WaarnemingMetadata", {})

                if value is None or abs(float(value)) >= _MISSING_SENTINEL:
                    discharge: float | None = None
                    quality = QualityFlag.MISSING
                else:
                    discharge = float(value)
                    quality = _status_to_quality(meta.get("Statuswaarde"))

                observations.append(Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=quality,
                ))
        return observations
