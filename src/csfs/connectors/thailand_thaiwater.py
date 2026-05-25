# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""ThaiWater connector — Thailand Hydro-Informatics Institute public water data."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()


@register("thailand_thaiwater")
class ThailandThaiWaterConnector(BaseConnector):
    """Connector for the ThaiWater public API (real-time water level/discharge)."""

    slug = "thailand_thaiwater"
    display_name = "ThaiWater (Thailand)"
    base_url = "https://api-v3.thaiwater.net/api/v1"
    country_codes = ["TH"]

    async def fetch_stations(self) -> list[Station]:
        """Return stations extracted from the waterlevel_load endpoint."""
        data = await self._fetch_waterlevel_data()
        return self._parse_stations(data)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Return the latest snapshot for the given station.

        ThaiWater's public API only provides real-time latest values,
        not historical range queries. The start/end parameters are
        accepted for interface compatibility but the response contains
        only the most recent reading.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")
        data = await self._fetch_waterlevel_data()
        return self._parse_observations_for_station(
            data, station_id, native_id,
        )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the latest water level / discharge snapshot."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        data = await self._fetch_waterlevel_data()
        return self._parse_observations_for_station(
            data, station_id, native_id,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _fetch_waterlevel_data(self) -> list[dict]:
        """Call the waterlevel_load endpoint and return the record list."""
        resp = await self._get(
            "/thaiwater30/public/waterlevel_load",
        )
        payload = resp.json()

        if isinstance(payload, dict):
            records = payload.get("data", payload.get("result", []))
        elif isinstance(payload, list):
            records = payload
        else:
            raise DataFormatError(
                self.slug,
                f"Unexpected response type: {type(payload).__name__}",
            )

        if not isinstance(records, list):
            raise DataFormatError(
                self.slug,
                "Expected a list of records from waterlevel_load",
            )
        return records

    def _parse_stations(self, records: list[dict]) -> list[Station]:
        """Parse station metadata from waterlevel_load records."""
        stations: list[Station] = []
        seen: set[str] = set()

        for rec in records:
            try:
                station_obj = rec.get("station", rec)
                if isinstance(station_obj, dict):
                    name = str(
                        station_obj.get("tele_station_name", {})
                        .get("en", "")
                        if isinstance(
                            station_obj.get("tele_station_name"),
                            dict,
                        )
                        else station_obj.get(
                            "tele_station_name",
                            station_obj.get("name", ""),
                        )
                    )
                    native_id = str(
                        station_obj.get("id",
                                        station_obj.get("tele_station_id", ""))
                    )
                    lat_raw = station_obj.get(
                        "tele_station_lat",
                        station_obj.get("lat", None),
                    )
                    lon_raw = station_obj.get(
                        "tele_station_long",
                        station_obj.get("long",
                                        station_obj.get("lon", None)),
                    )
                else:
                    name = str(rec.get("name", rec.get("station_name", "")))
                    native_id = str(
                        rec.get("id", rec.get("station_id", ""))
                    )
                    lat_raw = rec.get("lat", None)
                    lon_raw = rec.get("long", rec.get("lon", None))

                if not native_id or native_id in seen:
                    continue
                seen.add(native_id)

                lat = (
                    float(str(lat_raw))
                    if lat_raw is not None
                    else 0.0
                )
                lon = (
                    float(str(lon_raw))
                    if lon_raw is not None
                    else 0.0
                )

                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=name,
                    latitude=lat,
                    longitude=lon,
                    country_code="TH",
                ))
            except (ValueError, TypeError, AttributeError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    record=str(rec)[:200],
                    error=str(exc),
                )
                continue
        return stations

    def _parse_observations_for_station(
        self,
        records: list[dict],
        station_id: str,
        native_id: str,
    ) -> TimeSeriesChunk:
        """Extract the observation for a specific station from records."""
        observations: list[Observation] = []

        for rec in records:
            station_obj = rec.get("station", rec)
            if isinstance(station_obj, dict):
                rec_id = str(
                    station_obj.get("id",
                                    station_obj.get("tele_station_id", ""))
                )
            else:
                rec_id = str(rec.get("id", rec.get("station_id", "")))

            if rec_id != native_id:
                continue

            dt_str = rec.get(
                "datetime",
                rec.get("waterlevel_datetime",
                         rec.get("date", None)),
            )
            if dt_str is None:
                continue

            try:
                ts = datetime.fromisoformat(str(dt_str))
            except (ValueError, TypeError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid timestamp: {dt_str}",
                ) from exc

            discharge_raw = rec.get(
                "discharge",
                rec.get("discharge_value", None),
            )
            discharge: float | None = None
            if discharge_raw is not None:
                try:
                    discharge = float(str(discharge_raw))
                except (ValueError, TypeError):
                    discharge = None

            quality = (
                QualityFlag.RAW if discharge is not None
                else QualityFlag.MISSING
            )

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
