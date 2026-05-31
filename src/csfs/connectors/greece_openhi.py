"""Greece OpenHI connector — Open Hydrosystem Information Network.

OpenHI (https://system.openhi.net) is an Enhydris (v3) instance providing open
hydrological data for Greece. Discharge is one variable among many (most
stations only measure stage, rainfall, meteorology, etc.), nested under
per-station timeseries groups.

Enhydris data model
--------------------
* Stations (paginated):
  ``GET /api/stations/`` -> ``{"count", "next", "previous", "results": [...]}``.
  Each station carries ``id``, ``name``, ``geom`` (WKT, e.g.
  ``"SRID=4326;POINT (20.97 39.15)"``) and ``display_timezone``.

* Timeseries groups for a station:
  ``GET /api/stations/{id}/timeseriesgroups/`` -> each group has a ``variable``
  id. Discharge is variable id ``2`` (``Stage`` is ``14``); only ~8 of the 64
  stations have a discharge group.

* Timeseries within a group:
  ``GET /api/stations/{id}/timeseriesgroups/{tg}/timeseries/``.

* Data (CSV, in the station's display timezone):
  ``GET /api/stations/{id}/timeseriesgroups/{tg}/timeseries/{ts}/data/``
  with ``start_date``/``end_date`` (``"YYYY-MM-DD HH:MM"``). Rows are
  ``timestamp,value,flags``; ``value`` may be empty (gap).
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# Enhydris variable id for discharge in the OpenHI instance (Stage is 14).
_DISCHARGE_VARIABLE_ID = 2

# Enhydris data endpoint expects/returns local ("display_timezone") wall-clock.
_TS_DATE_FMT = "%Y-%m-%d %H:%M"

# Timestamp formats seen in the Enhydris CSV data export.
_TS_PARSE_FORMATS = (
    "%Y-%m-%d %H:%M",
    "%Y-%m-%d %H:%M:%S",
    "%Y-%m-%dT%H:%M:%S",
    "%Y-%m-%dT%H:%M",
)


def _flag_to_quality(flag: str | None) -> QualityFlag:
    """Map an OpenHI/Enhydris quality flag to a CSFS quality flag.

    Known flags: "VALIDATED", "RAW", "SUSPECT", "ESTIMATED", "MISSING".
    """
    if flag is None:
        return QualityFlag.RAW
    flag_upper = str(flag).upper().strip()
    mapping: dict[str, QualityFlag] = {
        "VALIDATED": QualityFlag.GOOD,
        "GOOD": QualityFlag.GOOD,
        "RAW": QualityFlag.RAW,
        "SUSPECT": QualityFlag.SUSPECT,
        "ESTIMATED": QualityFlag.ESTIMATED,
        "MISSING": QualityFlag.MISSING,
    }
    return mapping.get(flag_upper, QualityFlag.RAW)


@register("greece_openhi")
class GreeceOpenhiConnector(BaseConnector):
    """Connector for Greece's OpenHI (Enhydris) hydrological data."""

    slug = "greece_openhi"
    display_name = "OpenHI (Greece)"
    base_url = "https://system.openhi.net"
    country_codes = ["GR"]

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        # native_id -> (timeseriesgroup_id, timeseries_id, display_timezone)
        # for the station's discharge series.
        self._discharge_ref: dict[str, tuple[int, int, str]] = {}

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return the OpenHI stations that have a discharge timeseries.

        Most OpenHI stations only measure stage/meteorology, so each station
        is probed for a discharge (variable id 2) timeseries group and only
        those that have one are returned.
        """
        raw_stations = await self._fetch_all_station_dicts()

        discharge_dicts: list[dict] = []
        for entry in raw_stations:
            native_id = str(entry.get("id", "")).strip()
            if not native_id:
                continue
            ref = await self._resolve_discharge_ref(native_id, entry)
            if ref is not None:
                discharge_dicts.append(entry)

        stations = self._parse_stations(discharge_dicts)
        logger.info(
            "greece_stations_fetched",
            provider=self.slug,
            discharge_stations=len(stations),
            total_probed=len(raw_stations),
        )
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for *station_id* over [start, end]."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        ref = await self._resolve_discharge_ref(native_id)
        if ref is None:
            logger.info(
                "greece_no_discharge_timeseries",
                provider=self.slug,
                station=native_id,
            )
            return self._empty_chunk(station_id)

        tg_id, ts_id, tzname = ref
        tz = self._zone(tzname)
        params = {
            "start_date": self._fmt_query_date(start, tz),
            "end_date": self._fmt_query_date(end, tz),
            "fmt": "csv",
        }

        try:
            resp = await self._get(
                f"/api/stations/{native_id}/timeseriesgroups/"
                f"{tg_id}/timeseries/{ts_id}/data/",
                params=params,
            )
        except httpx.HTTPStatusError as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch observations for {native_id}: "
                f"HTTP {exc.response.status_code}",
            ) from exc

        return self._parse_csv(resp.text, station_id, tz)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent discharge observations (last 24 h)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # -----------------------------------------------------------------
    # Internal helpers — station discovery
    # -----------------------------------------------------------------

    async def _fetch_all_station_dicts(self) -> list[dict]:
        """Return all raw station dicts, following DRF pagination."""
        out: list[dict] = []
        page = 1
        while True:
            try:
                resp = await self._get("/api/stations/", params={"page": page})
            except httpx.HTTPStatusError as exc:
                raise ConnectorError(
                    self.slug,
                    f"Failed to fetch station list: "
                    f"HTTP {exc.response.status_code}",
                ) from exc

            data = resp.json()
            items = data.get("results", [])
            if not items:
                break
            out.extend(items)
            if data.get("next") is None:
                break
            page += 1
        return out

    async def _resolve_discharge_ref(
        self,
        native_id: str,
        station_dict: dict | None = None,
    ) -> tuple[int, int, str] | None:
        """Resolve and cache the discharge (tg_id, ts_id, tzname) for a station.

        Returns ``None`` if the station has no discharge timeseries group.
        """
        if native_id in self._discharge_ref:
            return self._discharge_ref[native_id]

        tzname = str((station_dict or {}).get("display_timezone") or "") or "UTC"

        try:
            groups_resp = await self._get(
                f"/api/stations/{native_id}/timeseriesgroups/",
            )
        except httpx.HTTPStatusError:
            return None
        groups = groups_resp.json().get("results", [])

        group = next(
            (
                g for g in groups
                if g.get("variable") == _DISCHARGE_VARIABLE_ID
            ),
            None,
        )
        if group is None or group.get("id") is None:
            return None
        tg_id = int(group["id"])

        try:
            ts_resp = await self._get(
                f"/api/stations/{native_id}/timeseriesgroups/{tg_id}/timeseries/",
            )
        except httpx.HTTPStatusError:
            return None
        ts_list = ts_resp.json().get("results", [])
        if not ts_list or ts_list[0].get("id") is None:
            return None
        ts_id = int(ts_list[0]["id"])

        ref = (tg_id, ts_id, tzname)
        self._discharge_ref[native_id] = ref
        return ref

    def _parse_stations(self, items: list[dict]) -> list[Station]:
        """Parse station dicts into ``Station`` models.

        Coordinates come from a WKT ``geom`` field
        (``"SRID=4326;POINT (lon lat)"``), a GeoJSON ``point``, or flat
        ``latitude``/``longitude`` keys.
        """
        stations: list[Station] = []
        for entry in items:
            native_id = str(entry.get("id", "")).strip()
            if not native_id:
                continue

            lat, lon = self._extract_coords(entry)
            if lat is None or lon is None:
                logger.warning(
                    "station_missing_coords",
                    provider=self.slug,
                    station=native_id,
                )
                continue

            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=entry.get("name", native_id),
                    latitude=float(lat),
                    longitude=float(lon),
                    country_code="GR",
                    river=entry.get("river"),
                ))
            except (ValueError, KeyError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    station=native_id,
                    error=str(exc),
                )
                continue

        return stations

    @staticmethod
    def _extract_coords(entry: dict) -> tuple[float | None, float | None]:
        """Extract (latitude, longitude) from a station dict.

        Supports WKT ``geom`` (optionally SRID-prefixed), GeoJSON ``point``,
        and flat ``latitude``/``longitude`` keys.
        """
        geom = entry.get("geom")
        if isinstance(geom, str) and "POINT" in geom.upper():
            import re
            match = re.search(
                r"POINT\s*\(\s*([\d.eE+-]+)\s+([\d.eE+-]+)\s*\)", geom,
            )
            if match:
                return float(match.group(2)), float(match.group(1))

        point = entry.get("point")
        if isinstance(point, dict):
            coords = point.get("coordinates")
            if isinstance(coords, (list, tuple)) and len(coords) >= 2:
                return float(coords[1]), float(coords[0])

        lat = entry.get("latitude") or entry.get("lat")
        lon = entry.get("longitude") or entry.get("lon")
        if lat is not None and lon is not None:
            return float(lat), float(lon)

        return None, None

    # -----------------------------------------------------------------
    # Internal helpers — observation parsing
    # -----------------------------------------------------------------

    def _parse_csv(
        self,
        text: str,
        station_id: str,
        tz: ZoneInfo,
    ) -> TimeSeriesChunk:
        """Parse an Enhydris CSV data export into a ``TimeSeriesChunk``.

        Each row is ``timestamp,value,flags``. Timestamps are in the station's
        display timezone and are converted to UTC. Empty values are gaps.
        """
        observations: list[Observation] = []
        for line in text.splitlines():
            line = line.strip()
            if not line:
                continue
            parts = line.split(",")
            if len(parts) < 2:
                continue

            ts = self._parse_timestamp(parts[0].strip(), tz)
            if ts is None:
                continue

            value_raw = parts[1].strip()
            flag_raw = parts[2].strip() if len(parts) > 2 else ""

            if value_raw == "" or value_raw.lower() == "nan":
                discharge: float | None = None
                quality = QualityFlag.MISSING
            else:
                try:
                    discharge = float(value_raw)
                    quality = _flag_to_quality(flag_raw or None)
                except ValueError:
                    discharge = None
                    quality = QualityFlag.MISSING

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

    @staticmethod
    def _parse_timestamp(ts_raw: str, tz: ZoneInfo) -> datetime | None:
        """Parse a local-time CSV timestamp and convert it to UTC."""
        for fmt in _TS_PARSE_FORMATS:
            try:
                naive = datetime.strptime(ts_raw, fmt)
            except ValueError:
                continue
            return naive.replace(tzinfo=tz).astimezone(UTC)
        return None

    @staticmethod
    def _fmt_query_date(dt: datetime, tz: ZoneInfo) -> str:
        """Format a query bound in the station's local time for Enhydris."""
        if dt.tzinfo is not None:
            dt = dt.astimezone(tz)
        return dt.strftime(_TS_DATE_FMT)

    @staticmethod
    def _zone(tzname: str) -> ZoneInfo:
        """Resolve an IANA timezone name, falling back to UTC."""
        try:
            return ZoneInfo(tzname)
        except Exception:
            return ZoneInfo("UTC")

    def _empty_chunk(self, station_id: str) -> TimeSeriesChunk:
        """Return an empty TimeSeriesChunk for a station."""
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )
