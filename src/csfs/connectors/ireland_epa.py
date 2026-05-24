"""Ireland EPA HydroNet and OPW water level data connector."""

from __future__ import annotations

import csv
from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# EPA HydroNet endpoints
_HYDRONET_BASE = "https://epawebapp.epa.ie/hydronet"
_STATIONS_PATH = "/output/internet/layers/10"
_DATA_PATH = "/output/internet/data"

# OPW waterlevel.ie fallback
_OPW_BASE = "https://waterlevel.ie"
_OPW_DATA_PATH = "/data/month"


def _map_quality(raw: str | None) -> QualityFlag:
    """Map EPA/OPW quality strings to CSFS quality flags."""
    if raw is None:
        return QualityFlag.RAW
    flag = raw.strip().lower()
    if flag in ("good", "valid", "1"):
        return QualityFlag.GOOD
    if flag in ("suspect", "doubtful", "2"):
        return QualityFlag.SUSPECT
    if flag in ("estimated", "3"):
        return QualityFlag.ESTIMATED
    if flag in ("missing", ""):
        return QualityFlag.MISSING
    return QualityFlag.RAW


@register("ireland_epa")
class IrelandEPAConnector(BaseConnector):
    """Connector for Ireland's EPA HydroNet and OPW water level APIs.

    Primary source: EPA HydroNet (flow/level data from EPA stations).
    Fallback source: OPW waterlevel.ie (water levels from OPW stations).
    """

    slug = "ireland_epa"
    display_name = "Ireland EPA HydroNet / OPW"
    base_url = _HYDRONET_BASE
    country_codes = ["IE"]

    async def fetch_stations(self) -> list[Station]:
        """Fetch station metadata from EPA HydroNet GeoJSON endpoint."""
        try:
            stations = await self._fetch_epa_stations()
        except (ConnectorError, httpx.HTTPError) as exc:
            logger.warning(
                "epa_stations_failed_trying_fallback",
                provider=self.slug,
                error=str(exc),
            )
            stations = await self._fetch_epa_stations_fallback()
        return stations

    async def _fetch_epa_stations(self) -> list[Station]:
        """Fetch stations from the HydroNet GeoJSON layers endpoint."""
        resp = await self._get(_STATIONS_PATH)
        data = resp.json()
        return self._parse_geojson_stations(data)

    async def _fetch_epa_stations_fallback(self) -> list[Station]:
        """Fetch stations from the Esri data.ashx fallback endpoint."""
        fallback_url = (
            "https://epawebapp.epa.ie/Esri/data.ashx"
        )
        resp = await self.client.get(
            fallback_url,
            params={"type": "stations", "format": "json"},
        )
        if resp.status_code != 200:
            raise ConnectorError(
                self.slug,
                f"Fallback station endpoint returned {resp.status_code}",
            )
        data = resp.json()
        # Fallback returns a flat list of station dicts
        return self._parse_flat_stations(data)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch flow observations from EPA HydroNet, falling back to OPW."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        try:
            return await self._fetch_epa_observations(
                native_id, start, end, station_id,
            )
        except (ConnectorError, httpx.HTTPError) as exc:
            logger.warning(
                "epa_obs_failed_trying_opw",
                provider=self.slug,
                station=native_id,
                error=str(exc),
            )
            return await self._fetch_opw_observations(
                native_id, start, end, station_id,
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
    # EPA HydroNet observations
    # ------------------------------------------------------------------

    async def _fetch_epa_observations(
        self,
        native_id: str,
        start: datetime,
        end: datetime,
        station_id: str,
    ) -> TimeSeriesChunk:
        """Fetch observations from the EPA HydroNet data endpoint."""
        resp = await self._get(
            f"{_DATA_PATH}/{native_id}",
            params={
                "from": start.strftime("%Y-%m-%d"),
                "to": end.strftime("%Y-%m-%d"),
                "type": "flow",
            },
        )

        try:
            data = resp.json()
        except Exception as exc:
            raise DataFormatError(
                self.slug,
                f"Invalid JSON from EPA for station {native_id}",
            ) from exc

        return self._parse_epa_observations(data, station_id)

    def _parse_epa_observations(
        self, data: list[dict], station_id: str,
    ) -> TimeSeriesChunk:
        """Parse the EPA HydroNet JSON observation array."""
        observations: list[Observation] = []

        for record in data:
            try:
                ts_raw = record.get("datetime") or record.get("date")
                if ts_raw is None:
                    continue
                ts = datetime.fromisoformat(str(ts_raw))
            except (ValueError, TypeError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid timestamp in EPA observation: {exc}",
                ) from exc

            value = record.get("value")
            discharge = float(value) if value is not None else None
            quality_raw = record.get("quality")
            quality = (
                QualityFlag.MISSING
                if discharge is None
                else _map_quality(quality_raw)
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

    # ------------------------------------------------------------------
    # OPW waterlevel.ie fallback
    # ------------------------------------------------------------------

    async def _fetch_opw_observations(
        self,
        native_id: str,
        start: datetime,
        end: datetime,
        station_id: str,
    ) -> TimeSeriesChunk:
        """Fetch observations from OPW waterlevel.ie CSV endpoint.

        OPW provides monthly CSV files, so we iterate over each month
        in the requested range.
        """
        observations: list[Observation] = []
        current = start.replace(day=1)

        while current <= end:
            url = (
                f"{_OPW_BASE}{_OPW_DATA_PATH}/{native_id}"
                f"/{current.year}/{current.month:02d}"
            )
            try:
                resp = await self.client.get(url)
                if resp.status_code == 200:
                    monthly = self._parse_opw_csv(
                        resp.text, station_id, start, end,
                    )
                    observations.extend(monthly)
                else:
                    logger.debug(
                        "opw_month_not_found",
                        station=native_id,
                        year=current.year,
                        month=current.month,
                        status=resp.status_code,
                    )
            except httpx.HTTPError as exc:
                logger.warning(
                    "opw_month_fetch_failed",
                    station=native_id,
                    year=current.year,
                    month=current.month,
                    error=str(exc),
                )

            # Advance to next month
            if current.month == 12:
                current = current.replace(
                    year=current.year + 1, month=1,
                )
            else:
                current = current.replace(month=current.month + 1)

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    @staticmethod
    def _parse_opw_csv(
        text: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse OPW CSV text into observation records.

        Expected CSV columns: timestamp (or datetime), value.
        Lines starting with '#' are treated as comments.
        """
        observations: list[Observation] = []
        lines = [
            line for line in text.strip().splitlines()
            if line and not line.startswith("#")
        ]
        if not lines:
            return observations

        reader = csv.DictReader(lines)
        for row in reader:
            ts_raw = (
                row.get("timestamp")
                or row.get("datetime")
                or row.get("date")
            )
            if ts_raw is None:
                continue

            try:
                ts = datetime.fromisoformat(ts_raw.strip())
            except ValueError:
                continue

            # Filter to requested range
            ts_naive = ts.replace(tzinfo=None)
            start_naive = start.replace(tzinfo=None)
            end_naive = end.replace(tzinfo=None)
            if ts_naive < start_naive or ts_naive > end_naive:
                continue

            val_raw = row.get("value")
            if val_raw is None or val_raw.strip() == "":
                discharge = None
            else:
                try:
                    discharge = float(val_raw.strip())
                except ValueError:
                    continue

            quality = (
                QualityFlag.MISSING if discharge is None
                else QualityFlag.RAW
            )
            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=discharge,
                quality=quality,
            ))

        return observations

    # ------------------------------------------------------------------
    # Station parsers
    # ------------------------------------------------------------------

    def _parse_geojson_stations(
        self, data: dict,
    ) -> list[Station]:
        """Parse a GeoJSON FeatureCollection into Station models."""
        stations: list[Station] = []
        features = data.get("features", [])

        for feature in features:
            props = feature.get("properties", {})
            geometry = feature.get("geometry", {})
            coords = geometry.get("coordinates", [])

            native_id = str(
                props.get("station_ref", ""),
            ).strip()
            if not native_id:
                continue

            try:
                lon = float(coords[0]) if len(coords) > 0 else None
                lat = float(coords[1]) if len(coords) > 1 else None
            except (ValueError, TypeError, IndexError):
                lat, lon = None, None

            if lat is None or lon is None:
                lat = props.get("latitude")
                lon = props.get("longitude")
                if lat is None or lon is None:
                    continue
                try:
                    lat = float(lat)
                    lon = float(lon)
                except (ValueError, TypeError):
                    continue

            name = str(
                props.get("station_name", native_id),
            )

            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=name,
                    latitude=lat,
                    longitude=lon,
                    country_code="IE",
                    river=props.get("river_name"),
                    catchment_area_km2=_safe_float(
                        props.get("catchment_area"),
                    ),
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

    def _parse_flat_stations(
        self, data: list[dict],
    ) -> list[Station]:
        """Parse a flat JSON list of station dicts (Esri fallback)."""
        stations: list[Station] = []

        for entry in data:
            native_id = str(
                entry.get("station_ref", entry.get("ref", "")),
            ).strip()
            if not native_id:
                continue

            lat = _safe_float(entry.get("latitude"))
            lon = _safe_float(entry.get("longitude"))
            if lat is None or lon is None:
                continue

            name = str(
                entry.get("station_name", entry.get("name", native_id)),
            )

            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=name,
                    latitude=lat,
                    longitude=lon,
                    country_code="IE",
                    river=entry.get("river_name"),
                    catchment_area_km2=_safe_float(
                        entry.get("catchment_area"),
                    ),
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


def _safe_float(value: object) -> float | None:
    """Convert a value to float, returning None on failure."""
    if value is None:
        return None
    try:
        return float(value)  # type: ignore[arg-type]
    except (ValueError, TypeError):
        return None
