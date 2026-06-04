# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Slovenia ARSO connector — Agencija Republike Slovenije za okolje.

ARSO provides real-time hydrological data (water levels, discharge, temperature)
via a public XML feed at 'arso.gov.si'.

Primary source: https://www.arso.gov.si/xml/vode/hidro_podatki_zadnji.xml
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import UTC, datetime, timedelta

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

_XML_URL = "https://www.arso.gov.si/xml/vode/hidro_podatki_zadnji.xml"


@register("slovenia_arso")
class SloveniaArsoConnector(BaseConnector):
    """Connector for Slovenia's ARSO hydrological real-time XML feed."""

    slug = "slovenia_arso"
    display_name = "ARSO (Slovenia)"
    base_url = "https://www.arso.gov.si"
    country_codes = ["SI"]

    async def fetch_stations(self) -> list[Station]:
        """Fetch all stations from the real-time XML feed."""
        try:
            resp = await self._get("/xml/vode/hidro_podatki_zadnji.xml")
            root = ET.fromstring(resp.text)
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch/parse XML feed: {exc}",
            ) from exc

        stations: list[Station] = []
        for postaja in root.findall("postaja"):
            try:
                native_id = postaja.get("sifra", "").strip()
                if not native_id:
                    continue

                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=postaja.findtext("merilno_mesto", "Unknown"),
                    latitude=float(postaja.get("wgs84_sirina", 0.0)),
                    longitude=float(postaja.get("wgs84_dolzina", 0.0)),
                    country_code="SI",
                    river=postaja.findtext("reka"),
                    catchment_area_km2=None, # Not in XML feed
                    is_active=True, # All stations in this feed are active
                ))
            except (ValueError, TypeError):
                continue

        logger.info(
            "stations_fetched",
            provider=self.slug,
            count=len(stations),
        )
        return stations

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the latest observations for a station from the XML feed."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        
        try:
            resp = await self._get("/xml/vode/hidro_podatki_zadnji.xml")
            root = ET.fromstring(resp.text)
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch XML feed: {exc}",
            ) from exc

        observations: list[Observation] = []
        for postaja in root.findall("postaja"):
            if postaja.get("sifra") == native_id:
                raw_time = postaja.findtext("datum") # YYYY-MM-DD HH:mm
                raw_val = postaja.findtext("pretok") # discharge in m3/s
                
                if not raw_time or not raw_val:
                    continue

                try:
                    ts = datetime.fromisoformat(raw_time.replace(" ", "T")).replace(tzinfo=UTC)
                    discharge = float(raw_val)
                    
                    observations.append(Observation(
                        station_id=station_id,
                        timestamp=ts,
                        discharge_m3s=discharge,
                        quality=QualityFlag.RAW,
                    ))
                except (ValueError, TypeError):
                    continue

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch observations for a range (only supports 'latest' via this feed)."""
        now = datetime.now(UTC)
        if start <= now <= (end + timedelta(minutes=30)):
            return await self.fetch_latest(station_id)
            
        logger.warning(
            "historical_data_not_supported_via_xml_feed",
            provider=self.slug,
            station=station_id,
        )
        return self._empty_chunk(station_id)

    def _empty_chunk(self, station_id: str) -> TimeSeriesChunk:
        """Return an empty TimeSeriesChunk."""
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )
