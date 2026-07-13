"""Bulgaria connector -- EAEMDR (Exploration and Maintenance of the Danube).

EAEMDR (Изпълнителна агенция "Проучване и поддържане на река Дунав", IAPPD /
APPD) publishes a daily hydrology bulletin for the Bulgarian section of the
Danube. The public "Hidrology" page renders a summary table of the current
water level (cm), discharge (m3/s) and water temperature for the main Danube
gauging towns. A subset of those gauges (the ones with a rating curve) report a
current **discharge in m3/s**; the rest report level only.

There is no queryable historical API and no JSON/CSV feed: the page is a single
live snapshot (re-rendered each load). The static "Open Data" portal only offers
historical PDF reports (1941-2019), which are not machine-readable. This
connector therefore scrapes the live snapshot table and returns, per station,
the current discharge (m3/s), stage (published in cm, emitted in m) and water
temperature (degC) as single observations timestamped at the bulletin date.
The bulletin does not declare an aggregation, so resolution is UNKNOWN.

References
----------
- Live bulletin: https://appd-bg.org/hidrology-en
- Open data (PDF only): https://appd-bg.org/pages-en?id=opendata
"""

from __future__ import annotations

import re
from datetime import UTC, datetime
from zoneinfo import ZoneInfo

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.models import (
    Observation,
    QualityFlag,
    Resolution,
    Station,
    TimeSeriesChunk,
    Variable,
)
from csfs.core.registry import register

logger = structlog.get_logger()

_EAEMDR_BASE_URL = "https://appd-bg.org"
_HIDROLOGY_PATH = "/hidrology-en"

# Known coordinates of the Bulgarian Danube gauging towns that report discharge.
# (Vidin and Nikopol appear in the bulletin but report level only -> excluded.)
_STATION_META: dict[str, dict] = {
    "Novo Selo": {"lat": 44.166, "lon": 22.783, "km": "833.60"},
    "Lom": {"lat": 43.817, "lon": 23.233, "km": "743.30"},
    "Oryahovo": {"lat": 43.733, "lon": 23.967, "km": "678.00"},
    "Svishtov": {"lat": 43.617, "lon": 25.350, "km": "554.30"},
    "Ruse": {"lat": 43.850, "lon": 25.950, "km": "495.60"},
    "Silistra": {"lat": 44.117, "lon": 27.267, "km": "375.50"},
}

# Bulletin date heading, e.g. "... Danube river 02.06.2026 г."
_DATE_RE = re.compile(
    r"Danube river\s*(?P<d>\d{2})\.(?P<m>\d{2})\.(?P<y>\d{4})"
)

# Each data row of the first ("Water levels") summary table. The columns are:
#   station | kilometre | water level (cm) | discharge (m3/s) | 24h diff | t water
# Level / temperature cells are wrapped in <span style="color: ...">VALUE</span>;
# the discharge cell is a bare <td>VALUE</td> and may be empty (level-only gauge).
_ROW_RE = re.compile(
    r"<tr>\s*"
    r"<td>(?P<name>[^<]+?)</td>\s*"            # station name
    r"<td>[^<]*</td>\s*"                        # kilometre
    r"<td>\s*<span[^>]*>(?P<h>[^<]*)</span>\s*</td>\s*"  # water level (cm)
    r"<td>(?P<q>[^<]*)</td>\s*"                 # discharge (m3/s), possibly empty
    r"<td>[^<]*</td>\s*"                        # 24h difference
    r"<td>\s*<span[^>]*>(?P<t>[^<]*)</span>\s*</td>",  # t water (degC)
    re.DOTALL,
)


@register("bulgaria_eaemdr")
class EAEMDRConnector(BaseConnector):
    """Connector for Bulgaria EAEMDR (Danube River discharge snapshot)."""

    slug = "bulgaria_eaemdr"
    display_name = "EAEMDR (Bulgaria)"
    base_url = _EAEMDR_BASE_URL
    country_codes = ["BG"]
    supported_variables = ("discharge", "stage", "water_temperature")

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        # Cache of the parsed bulletin:
        # {native_id: (timestamp, discharge_m3s, stage_m, temperature_c)}.
        self._snapshot: dict[
            str, tuple[datetime, float, float | None, float | None]
        ] | None = None

    async def fetch_stations(self) -> list[Station]:
        """Return Danube gauges that currently report discharge (m3/s)."""
        try:
            html = await self._fetch_bulletin()
        except Exception as exc:  # noqa: BLE001 - host may be down; degrade gracefully
            logger.warning("eaemdr_stations_failed", error=str(exc)[:160])
            return []

        snapshot = self._parse_bulletin(html)
        self._snapshot = snapshot

        stations: list[Station] = []
        for name, meta in _STATION_META.items():
            # Only surface gauges that actually have a discharge reading.
            if name not in snapshot:
                continue
            stations.append(
                Station(
                    id=self._station_id(name),
                    provider=self.slug,
                    native_id=name,
                    name=name,
                    latitude=meta["lat"],
                    longitude=meta["lon"],
                    country_code="BG",
                    river="Danube",
                )
            )
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Return the current discharge/stage/temperature for one station."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        try:
            html = await self._fetch_bulletin()
        except Exception as exc:  # noqa: BLE001 - degrade gracefully if host is down
            logger.warning(
                "eaemdr_fetch_failed", station=native_id, error=str(exc)[:160]
            )
            return self._chunk(station_id, [])

        snapshot = self._parse_bulletin(html)
        self._snapshot = snapshot

        observations: list[Observation] = []
        entry = snapshot.get(native_id)
        if entry is not None:
            ts, discharge, stage_m, temperature = entry
            if start <= ts <= end:
                for variable, value in (
                    (Variable.DISCHARGE, discharge),
                    (Variable.STAGE, stage_m),
                    (Variable.WATER_TEMPERATURE, temperature),
                ):
                    if value is None:
                        continue
                    observations.append(
                        Observation(
                            station_id=station_id,
                            timestamp=ts,
                            variable=variable,
                            resolution=Resolution.UNKNOWN,
                            value=value,
                            quality=QualityFlag.RAW,
                        )
                    )
        return self._chunk(station_id, observations)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _fetch_bulletin(self) -> str:
        """GET the live hydrology bulletin HTML (short timeout; host may hang)."""
        resp = await self._get(_HIDROLOGY_PATH, timeout=15.0)
        return resp.text

    def _parse_bulletin(
        self, html: str
    ) -> dict[str, tuple[datetime, float, float | None, float | None]]:
        """Parse the summary table into {station: (ts, discharge, stage, temp)}.

        Discharge stays in m3/s, water level (cm) is converted to stage in
        metres, and water temperature stays in degC.
        """
        ts = self._parse_date(html)
        result: dict[str, tuple[datetime, float, float | None, float | None]] = {}
        for m in _ROW_RE.finditer(html):
            name = m.group("name").strip()
            if name not in _STATION_META:
                continue
            discharge = self._parse_float(m.group("q"))
            if discharge is None:
                continue  # level-only gauge (e.g. Vidin, Nikopol)
            stage_m = self._parse_float(m.group("h"))
            if stage_m is not None:
                stage_m /= 100.0  # cm -> m
            temperature = self._parse_float(m.group("t"))
            result[name] = (ts, discharge, stage_m, temperature)
        return result

    @staticmethod
    def _parse_float(raw: str | None) -> float | None:
        """Parse a table cell as a float; empty/malformed cells become None."""
        if raw is None:
            return None
        cleaned = raw.strip().replace(" ", "")
        if not cleaned:
            return None
        try:
            return float(cleaned)
        except ValueError:
            return None

    def _parse_date(self, html: str) -> datetime:
        """Extract the bulletin date as midnight Europe/Sofia, in UTC.

        The bulletin is stamped with the Bulgarian calendar date; reading it
        as UTC midnight put the observation up to 3 h in the future during
        the nightly window where Sofia has rolled to the next day but UTC
        has not — and the fetch window filter then dropped it.
        """
        sofia = ZoneInfo("Europe/Sofia")
        m = _DATE_RE.search(html)
        if m:
            try:
                local = datetime(
                    int(m.group("y")),
                    int(m.group("m")),
                    int(m.group("d")),
                    tzinfo=sofia,
                )
                return local.astimezone(UTC)
            except ValueError:
                pass
        now = datetime.now(sofia)
        return datetime(now.year, now.month, now.day, tzinfo=sofia).astimezone(UTC)

    def _chunk(
        self, station_id: str, observations: list[Observation]
    ) -> TimeSeriesChunk:
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )
