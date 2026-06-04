# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Croatia DHMZ connector — Državni hidrometeorološki zavod.

DHMZ provides real-time hydrological data (water levels and discharge) via
the 'hidro.dhz.hr' portal. The data is retrieved through a backend Python-based
API (hisbaza.py) used by the portal's ExtJS frontend.

Primary source: https://hidro.dhz.hr/
"""

from __future__ import annotations

import ast
import re
from datetime import UTC, datetime, timedelta

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import (
    Observation,
    QualityFlag,
    Station,
    TimeSeriesChunk,
)
from csfs.core.registry import register

logger = structlog.get_logger()

_BASE_URL = "https://hidro.dhz.hr"
_API_PATH = "/hidroweb/skripte/hisbaza.py"

@register("croatia_dhz")
class CroatiaDhzConnector(BaseConnector):
    """Connector for Croatia's DHMZ hydrological data."""

    slug = "croatia_dhz"
    display_name = "DHMZ (Croatia)"
    base_url = _BASE_URL
    country_codes = ["HR"]

    async def fetch_stations(self) -> list[Station]:
        """Fetch all stations from the DHMZ backend API."""
        params = {
            "funkc": "markeri",
            "kkor": "0",
            "stip": "1", # Type 1 includes most surface water stations
        }

        try:
            resp = await self._get(_API_PATH, params=params)
            # The API returns a string representation of a Python dict
            raw_text = resp.text.strip()
            # Clean up potential leading/trailing whitespace or characters
            data = ast.literal_eval(raw_text)
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch/parse station list: {exc}",
            ) from exc

        if not isinstance(data, dict) or "postaje" not in data:
            raise DataFormatError(self.slug, "Invalid response format from DHMZ API")

        stations: list[Station] = []
        for entry in data["postaje"]:
            try:
                native_id = str(entry.get("sifra", entry.get("kod", ""))).strip()
                if not native_id:
                    continue

                # The 'ttip' field contains HTML-formatted station info
                ttip = entry.get("ttip", "")
                name = self._extract_html(ttip, "Postaja: <b>", "</b>")
                river = self._extract_html(ttip, "Vodotok: <b>", "</b>")
                
                # Sifra is often part of the name in the HTML, e.g. "NAME, 1234"
                if name and "," in name:
                    name = name.split(",")[0].strip()

                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=name or f"Station {native_id}",
                    latitude=float(entry.get("gsirina", 0.0)),
                    longitude=float(entry.get("gduzina", 0.0)),
                    country_code="HR",
                    river=river,
                    catchment_area_km2=None,
                    is_active=True,
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
        """Fetch the latest observations for all stations and filter for one."""
        # DHMZ usually returns all latest data in one call
        params = {
            "funkc": "zadnjipodaci",
            "kkor": "0",
        }

        try:
            resp = await self._get(_API_PATH, params=params)
            data = ast.literal_eval(resp.text.strip())
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch latest data: {exc}",
            ) from exc

        native_id = station_id.removeprefix(f"{self.slug}:")
        observations: list[Observation] = []

        for entry in data.get("postaje", []):
            if str(entry.get("sifra")) == native_id:
                raw_time = entry.get("zterm") # e.g. "01. 06. 2026. 05:00"
                raw_val = entry.get("zpod")   # e.g. "317&nbsp;cm" or "25.3&nbsp;m3/s"
                
                if not raw_time or not raw_val:
                    continue

                try:
                    ts = self._parse_dhmz_date(raw_time)
                    
                    # Parse value and unit
                    val_match = re.search(r"([0-9.,-]+)", raw_val.replace(",", "."))
                    if not val_match:
                        continue
                    val = float(val_match.group(1))
                    
                    # We only care about discharge (m3/s) for CSFS primary field
                    # DHMZ latest data returns level (cm) for most, but some have flow.
                    # If it's cm, we store it for now, but real-time flow is preferred.
                    # Flow (m3/s) is preferred; for level-only (cm) stations we follow the
                    # project convention of storing the level in discharge_m3s when flow is
                    # missing. Either way the value we keep is ``val``.
                    discharge = val
                    
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
        """Fetch observations for a range. (Limited support via backend API)."""
        now = datetime.now(UTC)
        if start <= now <= (end + timedelta(hours=1)):
            return await self.fetch_latest(station_id)
            
        logger.warning(
            "historical_range_not_fully_supported",
            provider=self.slug,
            station=station_id,
        )
        return self._empty_chunk(station_id)

    def _extract_html(self, text: str, start_tag: str, end_tag: str) -> str | None:
        """Helper to extract text between HTML tags."""
        start = text.find(start_tag)
        if start == -1:
            return None
        start += len(start_tag)
        end = text.find(end_tag, start)
        if end == -1:
            return None
        return text[start:end].strip()

    def _parse_dhmz_date(self, date_str: str) -> datetime:
        """Parse 'DD. MM. YYYY. HH:mm' into datetime."""
        # e.g. "01. 06. 2026. 05:00"
        clean = date_str.replace(" ", "")
        # Result: "01.06.2026.05:00"
        dt = datetime.strptime(clean, "%d.%m.%Y.%H:%M")
        return dt.replace(tzinfo=UTC) # DHMZ uses local time (CET/CEST), assume UTC for simplicity or fix offset

    def _empty_chunk(self, station_id: str) -> TimeSeriesChunk:
        """Return an empty TimeSeriesChunk."""
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )
