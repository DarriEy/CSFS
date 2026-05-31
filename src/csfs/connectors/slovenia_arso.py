# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Slovenia ARSO connector — Agencija RS za okolje hydrological network.

ARSO publishes a single real-time XML snapshot of every gauging station,
refreshed every 30 minutes:

    GET https://www.arso.gov.si/xml/vode/hidro_podatki_zadnji.xml

Each ``<postaja>`` element carries the station code (``sifra``), WGS84
coordinates, river name, and the latest reading. Discharge is the
``<pretok>`` element (m³/s); ``<vodostaj>`` is water level (cm). Timestamps
come in two flavours — ``<datum>`` in local time (CET/CEST, DST-aware) and
``<datum_cet>`` which is normalised to CET (UTC+1 year-round). We parse
``datum_cet`` so the conversion to UTC is unambiguous regardless of season.

Like other snapshot providers, this feed exposes only the latest reading per
station, so ``fetch_observations`` returns that single point when it falls
inside the requested window. The scheduler accumulates a series over time.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta, timezone
from xml.etree import ElementTree as ET

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# ARSO normalises ``datum_cet`` to Central European Time (UTC+1), without DST.
_CET = timezone(timedelta(hours=1))

_FEED_PATH = "/xml/vode/hidro_podatki_zadnji.xml"


@register("slovenia_arso")
class SloveniaArsoConnector(BaseConnector):
    """Connector for Slovenia's ARSO real-time hydrological feed."""

    slug = "slovenia_arso"
    display_name = "ARSO (Slovenia)"
    base_url = "https://www.arso.gov.si"
    country_codes = ["SI"]

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        # Parsed snapshot, keyed by station code. Populated once per run.
        self._snapshot: dict[str, dict] | None = None

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all stations currently reporting a discharge value."""
        snapshot = await self._get_snapshot()
        stations = [self._to_station(rec) for rec in snapshot.values()]
        logger.info("stations_fetched", provider=self.slug, count=len(stations))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Return the latest snapshot reading if it falls within the window."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        snapshot = await self._get_snapshot()
        rec = snapshot.get(native_id)

        observations: list[Observation] = []
        if rec is not None:
            ts = rec["timestamp"]
            if start <= ts <= end:
                observations.append(Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=rec["discharge"],
                    quality=QualityFlag.RAW,
                ))

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Return the most recent snapshot reading regardless of age."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        snapshot = await self._get_snapshot()
        rec = snapshot.get(native_id)

        observations: list[Observation] = []
        if rec is not None:
            observations.append(Observation(
                station_id=station_id,
                timestamp=rec["timestamp"],
                discharge_m3s=rec["discharge"],
                quality=QualityFlag.RAW,
            ))

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # -----------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------

    async def _get_snapshot(self) -> dict[str, dict]:
        """Fetch and parse the ARSO feed once, caching the result."""
        if self._snapshot is None:
            resp = await self._get(_FEED_PATH)
            self._snapshot = self._parse_feed(resp.content)
        return self._snapshot

    def _parse_feed(self, content: bytes) -> dict[str, dict]:
        """Parse the ARSO XML into a {code: record} map of discharge stations."""
        try:
            root = ET.fromstring(content)
        except ET.ParseError as exc:
            raise DataFormatError(self.slug, f"Invalid ARSO XML: {exc}") from exc

        records: dict[str, dict] = {}
        for postaja in root.findall("postaja"):
            code = (postaja.get("sifra") or "").strip()
            if not code:
                continue

            discharge = _parse_float(postaja.findtext("pretok"))
            if discharge is None:
                # Level-only station, or discharge sensor not reporting.
                continue

            lat = _parse_float(postaja.get("wgs84_sirina"))
            lon = _parse_float(postaja.get("wgs84_dolzina"))
            if lat is None or lon is None:
                continue

            ts = _parse_cet(postaja.findtext("datum_cet"))
            if ts is None:
                continue

            records[code] = {
                "code": code,
                "name": (
                    postaja.findtext("ime_kratko")
                    or postaja.findtext("merilno_mesto")
                    or code
                ).strip(),
                "river": (postaja.findtext("reka") or "").strip() or None,
                "latitude": lat,
                "longitude": lon,
                "elevation": _parse_float(postaja.get("kota_0")),
                "timestamp": ts,
                "discharge": discharge,
            }

        if not records:
            raise DataFormatError(self.slug, "ARSO feed contained no discharge stations")
        return records

    def _to_station(self, rec: dict) -> Station:
        return Station(
            id=self._station_id(rec["code"]),
            provider=self.slug,
            native_id=rec["code"],
            name=rec["name"],
            latitude=rec["latitude"],
            longitude=rec["longitude"],
            country_code="SI",
            river=rec["river"],
            elevation_m=rec["elevation"],
        )


def _parse_float(value: str | None) -> float | None:
    """Parse a float, returning None for empty/missing/non-numeric values."""
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _parse_cet(value: str | None) -> datetime | None:
    """Parse an ARSO ``datum_cet`` string (CET, UTC+1) into a UTC datetime."""
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        naive = datetime.strptime(text, "%Y-%m-%d %H:%M")
    except ValueError:
        return None
    return naive.replace(tzinfo=_CET).astimezone(UTC)
