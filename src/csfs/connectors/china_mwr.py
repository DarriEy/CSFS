"""China MWR connector — Ministry of Water Resources.

The MWR flood data system at http://xxfb.mwr.cn is extremely restricted
and unreliable for automated access.  This connector uses a curated seed
station list of ~20 major Chinese river stations and attempts to fetch
observations from known flood data endpoints.

Built EXTREMELY defensively: empty results are returned on ANY failure.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# ---------------------------------------------------------------------------
# Curated seed stations — major Chinese river gauging points.
# Format: (native_id, name, latitude, longitude, river)
# ---------------------------------------------------------------------------
_SEED_STATIONS: list[tuple[str, str, float, float, str]] = [
    ("MWR001", "Yichang", 30.6917, 111.2847, "Yangtze"),
    ("MWR002", "Datong", 30.7706, 117.6217, "Yangtze"),
    ("MWR003", "Hankou", 30.5833, 114.2833, "Yangtze"),
    ("MWR004", "Cuntan", 29.6167, 106.5833, "Yangtze"),
    ("MWR005", "Huayuankou", 34.9167, 113.6500, "Yellow River"),
    ("MWR006", "Sanmenxia", 34.8167, 111.0500, "Yellow River"),
    ("MWR007", "Lanzhou", 36.0611, 103.8343, "Yellow River"),
    ("MWR008", "Toudaoguai", 40.2667, 111.0667, "Yellow River"),
    ("MWR009", "Wuzhou", 23.4833, 111.3167, "Pearl River"),
    ("MWR010", "Boluo", 23.1667, 114.2833, "Dongjiang"),
    ("MWR011", "Shijiao", 23.5500, 112.9667, "Beijiang"),
    ("MWR012", "Nanning", 22.8167, 108.3667, "Yongjiang"),
    ("MWR013", "Harbin", 45.7500, 126.6500, "Songhua River"),
    ("MWR014", "Jiamusi", 46.8000, 130.3500, "Songhua River"),
    ("MWR015", "Hailar", 49.2167, 119.7333, "Hailar River"),
    ("MWR016", "Bengbu", 32.9167, 117.3833, "Huai River"),
    ("MWR017", "Wangjiaba", 32.4333, 115.6000, "Huai River"),
    ("MWR018", "Changsha", 28.2000, 112.9667, "Xiang River"),
    ("MWR019", "Nanchang", 28.6833, 115.8833, "Gan River"),
    ("MWR020", "Fuzhou", 26.0667, 119.3000, "Min River"),
]


@register("china_mwr")
class ChinaMWRConnector(BaseConnector):
    """Connector for China MWR flood data (extremely restricted)."""

    slug = "china_mwr"
    display_name = "MWR China (Ministry of Water Resources)"
    base_url = "http://xxfb.mwr.cn"
    country_codes = ["CN"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return curated major Chinese river gauging stations.

        Always returns the seed list.  A live augmentation attempt
        is made but any failure is silently swallowed.
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
        """Fetch discharge observations — returns empty on any failure."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        empty = TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )

        try:
            resp = await self._get(
                "/sq_djdh.html",
                params={"station": native_id},
            )
            return self._parse_observations(resp.text, station_id)
        except Exception as exc:
            logger.warning(
                "fetch_observations_failed",
                provider=self.slug,
                station=native_id,
                error=str(exc),
            )
            return empty

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
            country_code="CN",
            river=river,
        )

    async def _discover_stations_live(self) -> list[Station]:
        """Attempt live station discovery — extremely unreliable."""
        resp = await self._get("/sq_djdh.html")
        text = resp.text

        if not text or "<html" not in text.lower():
            return []

        return self._parse_station_html(text)

    def _parse_station_html(self, html: str) -> list[Station]:
        """Best-effort parse of MWR HTML station listing."""
        import re

        stations: list[Station] = []
        try:
            pattern = re.compile(
                r"station[_\-]?id['\"]?\s*[:=]\s*['\"]?(\w+)",
                re.IGNORECASE,
            )
            for match in pattern.finditer(html):
                native_id = match.group(1)
                if not native_id:
                    continue
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=native_id,
                    latitude=0.0,
                    longitude=0.0,
                    country_code="CN",
                ))
        except Exception:
            pass
        return stations

    def _parse_observations(
        self,
        text: str,
        station_id: str,
    ) -> TimeSeriesChunk:
        """Best-effort parse of MWR flood data HTML/text response."""
        import re

        observations: list[Observation] = []
        if not text:
            return TimeSeriesChunk(
                station_id=station_id,
                provider=self.slug,
                observations=observations,
                fetched_at=datetime.now(UTC),
            )

        try:
            # Look for date + value patterns in the response
            pattern = re.compile(
                r"(\d{4}[-/]\d{2}[-/]\d{2}[\sT]\d{2}:\d{2})"
                r"\s+(\d+\.?\d*)",
            )
            for match in pattern.finditer(text):
                raw_ts = match.group(1).strip()
                raw_val = match.group(2).strip()
                try:
                    ts = datetime.fromisoformat(
                        raw_ts.replace("/", "-"),
                    )
                    discharge = float(raw_val)
                    observations.append(Observation(
                        station_id=station_id,
                        timestamp=ts,
                        discharge_m3s=discharge,
                        quality=QualityFlag.RAW,
                    ))
                except (ValueError, TypeError):
                    continue
        except Exception:
            pass

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )
