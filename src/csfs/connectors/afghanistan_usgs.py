"""Afghanistan streamflow data via USGS NWIS.

USGS digitized 169 historical Afghan gauging station records and placed
them in the National Water Information System (NWIS).  These stations use
a different site numbering system than US domestic gauges and cannot be
queried with ``stateCd``.  Instead, this connector uses a bounding-box
filter covering Afghanistan (lat 29-38, lon 60-75).

Data are daily-value discharge in cubic feet per second (parameter 00060),
converted to m3/s on ingest.
"""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# USGS quality-code mapping (same as main USGS connector)
_QUALITY_MAP = {
    "A": QualityFlag.GOOD,
    "P": QualityFlag.RAW,
    "e": QualityFlag.ESTIMATED,
}

# Afghanistan bounding box (lon_min, lat_min, lon_max, lat_max)
_AF_BBOX = "60,29,75,38"


@register("afghanistan_usgs")
class AfghanistanUSGSConnector(BaseConnector):
    """USGS NWIS connector scoped to Afghan gauging stations.

    Uses the standard Water Services API with a bounding-box filter
    for Afghanistan instead of ``stateCd``.
    """

    slug = "afghanistan_usgs"
    display_name = "Afghanistan (USGS NWIS)"
    base_url = "https://waterservices.usgs.gov/nwis"
    country_codes: list[str] = ["AF"]

    DISCHARGE_PARAM = "00060"  # cfs
    CFS_TO_M3S = 0.0283168

    # ------------------------------------------------------------------
    # Station catalogue
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Query USGS NWIS site service for Afghan stream gauges.

        Tries the ``bBox`` parameter covering Afghanistan.  Falls back to
        a curated seed list if the API call fails.
        """
        try:
            resp = await self._get("/site/", params={
                "format": "rdb",
                "parameterCd": self.DISCHARGE_PARAM,
                "siteType": "ST",
                "hasDataTypeCd": "dv",
                "bBox": _AF_BBOX,
            })
            stations = self._parse_site_rdb(resp.text)
            logger.info(
                "stations_fetched",
                provider=self.slug,
                count=len(stations),
                source="nwis_bbox",
            )
            return stations
        except Exception as exc:
            logger.warning(
                "afghanistan_usgs_bbox_fallback",
                error=str(exc),
            )

        stations = self._build_seed_stations()
        logger.info(
            "stations_fetched",
            provider=self.slug,
            count=len(stations),
            source="seed",
        )
        return stations

    # ------------------------------------------------------------------
    # Observations
    # ------------------------------------------------------------------

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch daily-value discharge from USGS NWIS for an Afghan site."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        resp = await self._get("/dv/", params={
            "format": "json",
            "sites": native_id,
            "parameterCd": self.DISCHARGE_PARAM,
            "startDT": start.strftime("%Y-%m-%d"),
            "endDT": end.strftime("%Y-%m-%d"),
        })
        return self._parse_dv_json(resp.json(), station_id)

    # ------------------------------------------------------------------
    # Parsers
    # ------------------------------------------------------------------

    def _parse_site_rdb(self, text: str) -> list[Station]:
        """Parse USGS RDB site listing into Station objects."""
        stations: list[Station] = []
        lines = text.strip().splitlines()

        header_idx: int | None = None
        for i, line in enumerate(lines):
            if line.startswith("agency_cd"):
                header_idx = i
                break

        if header_idx is None:
            raise DataFormatError(
                self.slug, "Could not find RDB header in site response",
            )

        headers = lines[header_idx].split("\t")
        col = {name: idx for idx, name in enumerate(headers)}

        for line in lines[header_idx + 2:]:
            parts = line.split("\t")
            if len(parts) < len(headers):
                continue
            native_id = parts[col["site_no"]]
            lat_key = "dec_lat_va" if "dec_lat_va" in col else "lat_va"
            lon_key = "dec_long_va" if "dec_long_va" in col else "long_va"
            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=parts[col.get("station_nm", 0)],
                    latitude=float(str(parts[col[lat_key]])),
                    longitude=float(str(parts[col[lon_key]])),
                    country_code="AF",
                    catchment_area_km2=self._parse_drainage_area(
                        parts[col.get("drain_area_va", 0)],
                    ),
                ))
            except (ValueError, KeyError):
                continue
        return stations

    def _parse_dv_json(
        self, data: dict, station_id: str,
    ) -> TimeSeriesChunk:
        """Parse USGS daily-value JSON into a TimeSeriesChunk."""
        observations: list[Observation] = []
        try:
            ts_list = data["value"]["timeSeries"]
            if not ts_list:
                return TimeSeriesChunk(
                    station_id=station_id,
                    provider=self.slug,
                    observations=[],
                    fetched_at=datetime.now(UTC),
                )
            values = ts_list[0]["values"][0]["value"]
        except (KeyError, IndexError) as exc:
            raise DataFormatError(
                self.slug, f"Unexpected JSON structure: {exc}",
            ) from exc

        for v in values:
            qualifiers = v.get("qualifiers", [])
            qual_code = (
                qualifiers[0]
                if isinstance(qualifiers, list) and qualifiers
                else ""
            )
            q_flag = _QUALITY_MAP.get(qual_code, QualityFlag.RAW)

            raw = v.get("value")
            discharge: float | None = None
            if raw and raw != "-999999":
                discharge = float(str(raw)) * self.CFS_TO_M3S

            observations.append(Observation(
                station_id=station_id,
                timestamp=datetime.fromisoformat(v["dateTime"]),
                discharge_m3s=discharge,
                quality=(
                    q_flag if discharge is not None
                    else QualityFlag.MISSING
                ),
            ))

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # ------------------------------------------------------------------
    # Seed list (selected major Afghan gauging stations in USGS NWIS)
    # ------------------------------------------------------------------

    _SEED_STATIONS: list[dict] = [
        {
            "site_no": "390831069120000",
            "name": "KUNDUZ RIVER AT CHAR DARA",
            "lat": 36.81,
            "lon": 68.80,
            "river": "KUNDUZ",
            "area": 24200.0,
        },
        {
            "site_no": "343000068000000",
            "name": "HELMAND RIVER AT DEHRAWUD",
            "lat": 32.94,
            "lon": 66.07,
            "river": "HELMAND",
            "area": 28700.0,
        },
        {
            "site_no": "342900068300000",
            "name": "HELMAND RIVER AT KAJAKI",
            "lat": 32.33,
            "lon": 65.12,
            "river": "HELMAND",
            "area": 42600.0,
        },
        {
            "site_no": "345800069230000",
            "name": "KABUL RIVER AT TANG-I-GHARU",
            "lat": 34.55,
            "lon": 69.24,
            "river": "KABUL",
            "area": 12750.0,
        },
        {
            "site_no": "363100068050000",
            "name": "KOKCHA RIVER AT KHWAJAGHAR",
            "lat": 36.87,
            "lon": 69.38,
            "river": "KOKCHA",
            "area": 21400.0,
        },
        {
            "site_no": "350700065200000",
            "name": "HARI RUD AT TAGAB GUZAN",
            "lat": 34.45,
            "lon": 64.39,
            "river": "HARI RUD",
            "area": 21900.0,
        },
        {
            "site_no": "360900065200000",
            "name": "MURGHAB RIVER AT BALA MURGHAB",
            "lat": 35.82,
            "lon": 63.70,
            "river": "MURGHAB",
            "area": 22600.0,
        },
        {
            "site_no": "342200062300000",
            "name": "FARAH RUD AT FARAH",
            "lat": 32.37,
            "lon": 62.11,
            "river": "FARAH RUD",
            "area": 28000.0,
        },
        {
            "site_no": "344000069100000",
            "name": "LOGAR RIVER AT SANG-I-NAWISHTA",
            "lat": 34.10,
            "lon": 68.90,
            "river": "LOGAR",
            "area": 6200.0,
        },
        {
            "site_no": "370900067100000",
            "name": "AMU DARYA AT AI KHANOUM",
            "lat": 37.17,
            "lon": 69.40,
            "river": "AMU DARYA",
            "area": 135000.0,
        },
    ]

    def _build_seed_stations(self) -> list[Station]:
        """Build Station objects from the curated seed list."""
        stations: list[Station] = []
        for entry in self._SEED_STATIONS:
            stations.append(Station(
                id=self._station_id(entry["site_no"]),
                provider=self.slug,
                native_id=entry["site_no"],
                name=entry["name"],
                latitude=float(str(entry["lat"])),
                longitude=float(str(entry["lon"])),
                country_code="AF",
                river=entry.get("river"),
                catchment_area_km2=(
                    float(str(entry["area"]))
                    if entry.get("area") is not None
                    else None
                ),
            ))
        return stations

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_drainage_area(val: str) -> float | None:
        """Convert drainage area from square miles to km2."""
        try:
            return float(str(val)) * 2.58999
        except (ValueError, TypeError):
            return None
