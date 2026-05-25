"""USGS National Water Information System (NWIS) connector."""

from __future__ import annotations

from datetime import UTC, datetime

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

USGS_QUALITY_MAP = {
    "A": QualityFlag.GOOD,       # Approved
    "P": QualityFlag.RAW,        # Provisional
    "e": QualityFlag.ESTIMATED,  # Estimated
}


@register("usgs")
class USGSConnector(BaseConnector):
    slug = "usgs"
    display_name = "USGS NWIS"
    base_url = "https://waterservices.usgs.gov/nwis"
    country_codes = ["US"]

    DISCHARGE_PARAM = "00060"  # cubic feet per second
    CFS_TO_M3S = 0.0283168

    # US states + territories for chunked fetching
    US_STATES = [
        "AL", "AK", "AZ", "AR", "CA", "CO", "CT", "DE", "FL", "GA",
        "HI", "ID", "IL", "IN", "IA", "KS", "KY", "LA", "ME", "MD",
        "MA", "MI", "MN", "MS", "MO", "MT", "NE", "NV", "NH", "NJ",
        "NM", "NY", "NC", "ND", "OH", "OK", "OR", "PA", "RI", "SC",
        "SD", "TN", "TX", "UT", "VT", "VA", "WA", "WV", "WI", "WY",
        "DC", "PR", "VI", "GU", "AS",
    ]

    async def fetch_stations(self, state_codes: list[str] | None = None) -> list[Station]:
        states = state_codes or self.config.get("states") or self.US_STATES
        all_stations: list[Station] = []
        for state in states:
            try:
                resp = await self._get("/site/", params={
                    "format": "rdb",
                    "parameterCd": self.DISCHARGE_PARAM,
                    "siteType": "ST",
                    "siteStatus": "active",
                    "hasDataTypeCd": "iv",
                    "stateCd": state,
                })
                all_stations.extend(self._parse_site_rdb(resp.text))
            except Exception:
                continue
        return all_stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        native_id = station_id.removeprefix(f"{self.slug}:")
        resp = await self._get("/iv/", params={
            "format": "json",
            "sites": native_id,
            "parameterCd": self.DISCHARGE_PARAM,
            "startDT": start.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
            "endDT": end.strftime("%Y-%m-%dT%H:%M:%S+00:00"),
        })
        return self._parse_dv_json(resp.json(), station_id)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        native_id = station_id.removeprefix(f"{self.slug}:")
        resp = await self._get("/iv/", params={
            "format": "json",
            "sites": native_id,
            "parameterCd": self.DISCHARGE_PARAM,
            "period": "PT2H",
        })
        return self._parse_dv_json(resp.json(), station_id)

    def _parse_site_rdb(self, text: str) -> list[Station]:
        stations = []
        lines = text.strip().splitlines()
        header_idx = None
        for i, line in enumerate(lines):
            if line.startswith("agency_cd"):
                header_idx = i
                break
        if header_idx is None:
            raise DataFormatError(self.slug, "Could not find RDB header")

        headers = lines[header_idx].split("\t")
        col = {name: idx for idx, name in enumerate(headers)}

        for line in lines[header_idx + 2:]:  # skip header + format line
            parts = line.split("\t")
            if len(parts) < len(headers):
                continue
            native_id = parts[col["site_no"]]
            lat = parts[col.get("dec_lat_va", col.get("lat_va", 0))]
            lon = parts[col.get("dec_long_va", col.get("long_va", 0))]
            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=parts[col.get("station_nm", 0)],
                    latitude=float(lat),
                    longitude=float(lon),
                    country_code="US",
                    catchment_area_km2=self._parse_drainage_area(
                        parts[col.get("drain_area_va", 0)]
                    ),
                ))
            except (ValueError, KeyError):
                continue
        return stations

    def _parse_dv_json(self, data: dict, station_id: str) -> TimeSeriesChunk:
        observations = []
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
        except (KeyError, IndexError) as e:
            raise DataFormatError(self.slug, f"Unexpected JSON structure: {e}") from e

        for v in values:
            q_flag = USGS_QUALITY_MAP.get(
                v.get("qualifiers", [{}])[0] if isinstance(v.get("qualifiers"), list) else "",
                QualityFlag.RAW,
            )
            raw = v.get("value")
            discharge = float(raw) * self.CFS_TO_M3S if raw and raw != "-999999" else None
            observations.append(Observation(
                station_id=station_id,
                timestamp=datetime.fromisoformat(v["dateTime"]),
                discharge_m3s=discharge,
                quality=q_flag if discharge is not None else QualityFlag.MISSING,
            ))

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    @staticmethod
    def _parse_drainage_area(val: str) -> float | None:
        try:
            return float(val) * 2.58999  # sq mi -> km2
        except (ValueError, TypeError):
            return None
