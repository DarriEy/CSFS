# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""New Zealand Hilltop connector — multiple regional council flow data."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import UTC, datetime

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()


_COUNCILS = [
    ("NRC", "https://hilltop.nrc.govt.nz/data.hts"),
    ("Horizons", "https://hilltopserver.horizons.govt.nz/boo.hts"),
    ("GWRC", "https://hilltop.gw.govt.nz/Data.hts"),
    ("ORC", "https://gisdata.orc.govt.nz/hilltop/data.hts"),
]


@register("newzealand_nrc")
class NewZealandNrcConnector(BaseConnector):
    """Connector for NZ regional council Hilltop servers."""

    slug = "newzealand_nrc"
    display_name = "NZ Hilltop (multi-council)"
    base_url = "https://hilltop.nrc.govt.nz"
    country_codes = ["NZ"]

    async def fetch_stations(self) -> list[Station]:
        """Return flow stations from all reachable NZ councils."""
        all_stations: list[Station] = []
        for council_name, url in _COUNCILS:
            try:
                async with httpx.AsyncClient(timeout=15, follow_redirects=True) as c:
                    resp = await c.get(url, params={
                        "Service": "Hilltop",
                        "Request": "SiteList",
                        "Location": "Yes",
                        "Measurement": "Flow",
                    })
                if resp.status_code == 200:
                    stations = self._parse_station_xml(resp.text)
                    logger.info("council_fetched", council=council_name, count=len(stations))
                    all_stations.extend(stations)
            except Exception:
                logger.warning("council_unreachable", council=council_name)
        return all_stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch flow observations for a station over a time range."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        site_name = native_id.replace("_", " ")

        resp = await self._get(
            "/data.hts",
            params={
                "Service": "Hilltop",
                "Request": "GetData",
                "Site": site_name,
                "Measurement": "Flow",
                "from": start.strftime("%Y-%m-%dT%H:%M:%S"),
                "to": end.strftime("%Y-%m-%dT%H:%M:%S"),
            },
        )
        return self._parse_data_xml(resp.text, station_id)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent flow observations (last 24 h)."""
        from datetime import timedelta

        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_station_xml(self, xml_text: str) -> list[Station]:
        """Parse the Hilltop SiteList XML response.

        Expected structure:
        <HilltopServer>
          <Site Name="...">
            <Latitude>...</Latitude>
            <Longitude>...</Longitude>
          </Site>
          ...
        </HilltopServer>
        """
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as exc:
            raise DataFormatError(
                self.slug,
                f"Failed to parse station list XML: {exc}",
            ) from exc

        stations: list[Station] = []
        for site in root.findall(".//Site"):
            try:
                name = site.get("Name", "")
                if not name:
                    continue

                native_id = name.replace(" ", "_")

                lat_el = site.find("Latitude")
                lon_el = site.find("Longitude")
                lat = (
                    float(str(lat_el.text))
                    if lat_el is not None and lat_el.text
                    else 0.0
                )
                lon = (
                    float(str(lon_el.text))
                    if lon_el is not None and lon_el.text
                    else 0.0
                )

                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=name,
                    latitude=lat,
                    longitude=lon,
                    country_code="NZ",
                ))
            except (ValueError, TypeError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    site_name=site.get("Name"),
                    error=str(exc),
                )
                continue
        return stations

    def _parse_data_xml(
        self, xml_text: str, station_id: str,
    ) -> TimeSeriesChunk:
        """Parse the Hilltop GetData XML response.

        Expected structure:
        <Hilltop>
          <Measurement SiteName="...">
            <Data ...>
              <E><T>2024-01-01T00:00:00</T><I1>1.23</I1></E>
              ...
            </Data>
          </Measurement>
        </Hilltop>
        """
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError as exc:
            raise DataFormatError(
                self.slug,
                f"Failed to parse data XML: {exc}",
            ) from exc

        observations: list[Observation] = []
        for entry in root.findall(".//E"):
            t_el = entry.find("T")
            i1_el = entry.find("I1")

            if t_el is None or t_el.text is None:
                continue

            try:
                ts = datetime.fromisoformat(t_el.text)
            except (ValueError, TypeError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid timestamp in data: {t_el.text}",
                ) from exc

            discharge: float | None = None
            quality = QualityFlag.RAW
            if i1_el is not None and i1_el.text is not None:
                try:
                    discharge = float(str(i1_el.text))
                except (ValueError, TypeError):
                    discharge = None

            if discharge is None:
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
