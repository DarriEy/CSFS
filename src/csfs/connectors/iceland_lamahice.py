"""Iceland LamaH-Ice connector — research dataset with live augmentation.

LamaH-Ice is a research dataset of Icelandic catchment attributes.  This
connector maintains a curated seed list of ~30 known Icelandic gauging
stations (sourced from Vedurstofa Islands / IMO) and attempts to augment
observations from the Vedur.is hydro API.

Built very defensively: the live API is unreliable and may change at any
time.  Empty results are returned on any failure.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Curated seed stations from Vedurstofa Islands / IMO network.
# Format: (native_id, name, latitude, longitude, river)
# ---------------------------------------------------------------------------
_SEED_STATIONS: list[tuple[str, str, float, float, str]] = [
    ("VHM001", "Selfoss", 63.9333, -21.0000, "Olfusa"),
    ("VHM002", "Kirkjubaejarklaustur", 63.7833, -18.0000, "Skafta"),
    ("VHM003", "Irafoss", 64.0833, -21.7500, "Sog"),
    ("VHM004", "Gullfoss", 64.3271, -20.1237, "Hvita"),
    ("VHM005", "Lagarfoss", 65.2500, -14.3833, "Lagarfljot"),
    ("VHM006", "Bruarfoss", 64.2667, -20.5167, "Bruara"),
    ("VHM007", "Dettifoss", 65.8147, -16.3845, "Jokulsa a Fjollum"),
    ("VHM008", "Hofsos", 65.9000, -18.8833, "Hofsa"),
    ("VHM009", "Borg", 64.8000, -21.9167, "Nordura"),
    ("VHM010", "Hredavatn", 64.7500, -21.5333, "Hvita (Borgarfjordur)"),
    ("VHM011", "Thorsa", 63.8833, -20.4667, "Thorsa"),
    ("VHM012", "Kaldakvisl", 64.6167, -18.6000, "Kaldakvisl"),
    ("VHM013", "Thjorsardalur", 64.1500, -19.8333, "Thjorsa"),
    ("VHM014", "Blonduos", 65.6667, -20.2833, "Blanda"),
    ("VHM015", "Akureyri", 65.6833, -18.0833, "Glera"),
    ("VHM016", "Hjaltadalur", 65.7333, -18.5333, "Hjaltadalslaekur"),
    ("VHM017", "Fljotsdalsheidi", 65.0500, -15.0667, "Jokla"),
    ("VHM018", "Egilsstadir", 65.2500, -14.3833, "Lagarfljot (upper)"),
    ("VHM019", "Grimsstadir", 65.6333, -16.1167, "Jokulsa a Fjollum (upper)"),
    ("VHM020", "Reykjavik-Ellidaar", 64.1167, -21.8500, "Ellidaar"),
    ("VHM021", "Hraunfossar", 64.7000, -20.9667, "Hvita (Reykholt)"),
    ("VHM022", "Skagafjordur", 65.7500, -19.3333, "Heradsvotn"),
    ("VHM023", "Oxi", 64.9833, -14.8167, "Berufjardara"),
    ("VHM024", "Medallandssandur", 63.5667, -18.7500, "Kudarfljot"),
    ("VHM025", "Fljotsdalur", 65.1167, -14.6833, "Kelduarfljot"),
    ("VHM026", "Kringla", 65.4833, -18.2667, "Fnjoska"),
    ("VHM027", "Skutustadagigar", 65.5667, -16.9833, "Laxa i Myvatnssveit"),
    ("VHM028", "Vik", 63.4167, -19.0000, "Jokulsa a Solheimasandi"),
    ("VHM029", "Husavik", 66.0500, -17.3333, "Laxargljufur"),
    ("VHM030", "Stykkisholmur", 65.0667, -22.7333, "Hrauns"),
]


@register("iceland_lamahice")
class IcelandLamahIceConnector(BaseConnector):
    """Connector for Icelandic hydrological data (LamaH-Ice / Vedur.is)."""

    slug = "iceland_lamahice"
    display_name = "LamaH-Ice (Iceland)"
    base_url = "https://api.vedur.is"
    country_codes = ["IS"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return curated Icelandic gauging stations.

        Always returns the seed list.  A live discovery call is attempted
        but failures are silently ignored.
        """
        stations = [
            self._build_seed_station(row)
            for row in _SEED_STATIONS
        ]

        try:
            live = await self._discover_stations_live()
            seed_ids = {s.native_id for s in stations}
            for st in live:
                if st.native_id not in seed_ids:
                    stations.append(st)
        except Exception:
            logger.debug(
                "live_station_discovery_skipped",
                provider=self.slug,
            )

        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for a station.

        Attempts the Vedur.is hydro API; returns empty on any failure.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        try:
            resp = await self._get(
                "/hydro/latest.json",
                params={
                    "station": native_id,
                    "start": start.strftime("%Y-%m-%d"),
                    "end": end.strftime("%Y-%m-%d"),
                },
            )
            data = resp.json()
            return self._parse_observations(data, station_id)
        except Exception as exc:
            logger.warning(
                "fetch_observations_failed",
                provider=self.slug,
                station=native_id,
                error=str(exc),
            )
            return TimeSeriesChunk(
                station_id=station_id,
                provider=self.slug,
                observations=[],
                fetched_at=datetime.now(UTC),
            )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent 24 hours of observations."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

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
            country_code="IS",
            river=river,
        )

    async def _discover_stations_live(self) -> list[Station]:
        """Attempt to discover stations from the Vedur.is API."""
        resp = await self._get("/hydro/stations.json")
        data = resp.json()

        if not isinstance(data, list):
            if isinstance(data, dict):
                data = (
                    data.get("stations")
                    or data.get("data", [])
                )
            if not isinstance(data, list):
                return []

        stations: list[Station] = []
        for entry in data:
            try:
                native_id = str(
                    entry.get("id")
                    or entry.get("station_id")
                    or ""
                )
                if not native_id:
                    continue
                name = str(entry.get("name", ""))
                lat = _safe_float(entry.get("lat"))
                lon = _safe_float(entry.get("lon"))
                if lat is None or lon is None:
                    continue
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=name,
                    latitude=lat,
                    longitude=lon,
                    country_code="IS",
                    river=entry.get("river"),
                ))
            except (ValueError, KeyError, TypeError):
                continue
        return stations

    def _parse_observations(
        self,
        data: dict | list,
        station_id: str,
    ) -> TimeSeriesChunk:
        """Parse observation data from Vedur.is JSON response."""
        obs_list: list[dict] = []
        if isinstance(data, dict):
            obs_list = (
                data.get("data")
                or data.get("observations")
                or data.get("values", [])
            )
        elif isinstance(data, list):
            obs_list = data

        if not isinstance(obs_list, list):
            obs_list = []

        observations: list[Observation] = []
        for entry in obs_list:
            try:
                raw_ts = (
                    entry.get("time")
                    or entry.get("timestamp")
                    or entry.get("date")
                )
                if raw_ts is None:
                    continue
                ts = datetime.fromisoformat(str(raw_ts))

                value = (
                    entry.get("discharge")
                    or entry.get("value")
                    or entry.get("flow")
                )
                discharge = _safe_float(value)
                quality = (
                    QualityFlag.RAW
                    if discharge is not None
                    else QualityFlag.MISSING
                )
                observations.append(Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=quality,
                ))
            except (ValueError, TypeError):
                continue

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )


# ------------------------------------------------------------------
# Module-level helpers
# ------------------------------------------------------------------


def _safe_float(value: object) -> float | None:
    """Safely convert a value to float."""
    if value is None:
        return None
    try:
        return float(str(value))
    except (ValueError, TypeError):
        return None
