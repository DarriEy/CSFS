"""Germany Bavaria connector -- GKD Bayern (Gewässerkundlicher Dienst Bayern).

GKD Bayern publishes discharge ("Abfluss") data for ~600 river gauges. The
portal exposes two scrapeable HTML surfaces that this connector relies on:

* The discharge overview table
  ``/de/fluesse/abfluss/tabellen`` lists every station with its name, river
  ("Gewässer"), district and a per-station detail link. The detail link carries
  both the river-basin region slug and the numeric station id, e.g.
  ``/de/fluesse/abfluss/kelheim/achsheim-11944004`` (id ``11944004``).

* The per-station measurement table
  ``/de/fluesse/abfluss/{region}/{slug}-{id}/messwerte?method=tabellen``
  renders an inline HTML table of ``Datum`` / ``Abfluss [m³/s]`` rows (the most
  recent measurements). Timestamps render in German local time (CET/CEST) with
  a trailing " Uhr"; values are already in m³/s and use a German decimal comma.

References
----------
- Portal: https://www.gkd.bayern.de/
- Catalogue: https://www.gkd.bayern.de/de/fluesse/abfluss/tabellen
- Station data: https://www.gkd.bayern.de/de/fluesse/abfluss/{region}/{slug}-{id}/messwerte?method=tabellen
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

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

# GKD renders timestamps in German local time (CET/CEST); convert to UTC.
_BERLIN_TZ = ZoneInfo("Europe/Berlin")

_GKD_BASE_URL = "https://www.gkd.bayern.de"
_CATALOGUE_PATH = "/de/fluesse/abfluss/tabellen"

# Matches a station detail row in the catalogue table. Captures:
#   1: region slug (e.g. "kelheim")
#   2: name slug   (e.g. "achsheim")
#   3: numeric id  (e.g. "11944004")
# followed by the rendered station name, river and district cells.
_ROW_RE = re.compile(
    r'<tr[^>]*>'
    r'<td[^>]*data-text="(?P<name>[^"]*)"[^>]*>'
    # The station link is now wrapped in <ul class="linkliste"><li>, and the
    # href carries a trailing /messwerte?method=tabellen path.
    r'<ul[^>]*>\s*<li[^>]*>\s*'
    r'<a href="https://www\.gkd\.bayern\.de'
    r'/de/fluesse/abfluss/(?P<region>[a-z_]+)/(?P<slug>[a-z0-9-]+)-(?P<id>\d+)'
    r'[^"]*">'
    r'.*?</a>\s*</li>\s*</ul>\s*</td>'
    r'<td[^>]*data-text="[^"]*">(?P<river>[^<]*)</td>',
    re.DOTALL,
)

# Matches a (timestamp, value) pair inside the per-station measurement table.
# Timestamps render with a trailing " Uhr"; values use a German decimal comma.
_OBS_RE = re.compile(
    r'<td[^>]*>(?P<ts>\d{2}\.\d{2}\.\d{4} \d{2}:\d{2})(?:\s*Uhr)?</td>'
    r'\s*<td[^>]*class="center"[^>]*>(?P<val>[^<]*)</td>',
)


@register("germany_bavaria")
class GermanyBavariaConnector(BaseConnector):
    """Connector for GKD Bayern (HTML table scraping)."""

    slug = "germany_bavaria"
    display_name = "GKD Bayern (Germany)"
    # Trailing slash so httpx joins relative measurement paths cleanly.
    base_url = _GKD_BASE_URL
    country_codes = ["DE"]
    # The portal is a single shared host; keep concurrency modest.
    max_concurrent_requests = 4

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        # native_id -> region-qualified detail path (for observation fetch)
        self._station_paths: dict[str, str] = {}

    async def fetch_stations(self) -> list[Station]:
        """Return the full GKD Bayern discharge catalogue."""
        resp = await self._get(_CATALOGUE_PATH)
        return self._parse_stations(resp.text)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations from the per-station measurement table."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        path = await self._resolve_path(native_id)

        params = {
            "method": "tabellen",
            "zr": "individuell",
            "beginn": start.strftime("%d.%m.%Y"),
            "ende": end.strftime("%d.%m.%Y"),
        }
        try:
            resp = await self._get(f"{path}/messwerte", params=params)
        except Exception as exc:  # noqa: BLE001 - normalise to ConnectorError
            raise ConnectorError(
                self.slug, f"Failed to fetch GKD data for {native_id}: {exc}"
            ) from exc

        return self._parse_observations(resp.text, station_id, start, end)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_stations(self, html: str) -> list[Station]:
        """Parse the catalogue table HTML into Station objects."""
        stations: list[Station] = []
        for m in _ROW_RE.finditer(html):
            native_id = m.group("id")
            if native_id in self._station_paths:
                continue  # de-duplicate

            region = m.group("region")
            slug = m.group("slug")
            path = f"/de/fluesse/abfluss/{region}/{slug}-{native_id}"
            self._station_paths[native_id] = path

            name = _unescape(m.group("name")).strip()
            river = _unescape(m.group("river")).strip() or None

            try:
                stations.append(
                    Station(
                        id=self._station_id(native_id),
                        provider=self.slug,
                        native_id=native_id,
                        name=name or native_id,
                        latitude=0.0,
                        longitude=0.0,
                        country_code="DE",
                        river=river,
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

    def _parse_observations(
        self,
        html: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Parse the inline measurement table into a TimeSeriesChunk."""
        observations: list[Observation] = []
        for m in _OBS_RE.finditer(html):
            ts_str = m.group("ts")
            val_str = m.group("val").strip()
            try:
                ts = datetime.strptime(ts_str, "%d.%m.%Y %H:%M").replace(
                    tzinfo=_BERLIN_TZ
                ).astimezone(UTC)
            except ValueError:
                continue
            if ts < start or ts > end:
                continue

            # German decimal comma; missing/"-" values yield None.
            discharge: float | None
            cleaned = val_str.replace(".", "").replace(",", ".")
            try:
                discharge = float(cleaned)
            except ValueError:
                discharge = None

            observations.append(
                Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=QualityFlag.RAW
                    if discharge is not None
                    else QualityFlag.MISSING,
                )
            )

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    async def _resolve_path(self, native_id: str) -> str:
        """Return the region-qualified detail path for a station id."""
        if native_id in self._station_paths:
            return self._station_paths[native_id]

        # Populate the cache from the catalogue.
        await self.fetch_stations()

        if native_id not in self._station_paths:
            raise ConnectorError(
                self.slug, f"Unknown GKD Bayern station id '{native_id}'"
            )
        return self._station_paths[native_id]


def _unescape(text: str) -> str:
    """Decode the handful of HTML entities the GKD pages emit."""
    import html as _html

    return _html.unescape(text)
