"""DSI connector — Turkey State Hydraulic Works (Devlet Su Isleri) via FACE portal."""

from __future__ import annotations

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


@register("turkey_dsi")
class TurkeyDsiConnector(BaseConnector):
    slug = "turkey_dsi"
    display_name = "DSI (Devlet Su Isleri / FACE Portal)"
    base_url = "https://akim.faceteknoloji.com.tr"
    country_codes = ["TR"]

    # Candidate endpoint paths for station listing (tried in order)
    _STATION_PATHS = (
        "/api/stations",
        "/istasyonlar",
        "/api/istasyonlar",
    )

    # Candidate endpoint paths for observation data (tried in order)
    _OBSERVATION_PATHS = (
        "/api/data",
        "/api/observations",
        "/api/akim",
    )

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all discharge stations from the DSI FACE portal."""
        last_exc: Exception | None = None

        for path in self._STATION_PATHS:
            try:
                resp = await self._get(
                    path,
                    params={"format": "json"},
                )
                return self._parse_station_response(resp)
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "station_endpoint_failed",
                    provider=self.slug,
                    path=path,
                    error=str(exc),
                )
                last_exc = exc

        raise ConnectorError(
            self.slug,
            f"All station endpoints failed: {last_exc}",
        ) from last_exc

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch daily discharge for a station over a time range."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        last_exc: Exception | None = None

        for path in self._OBSERVATION_PATHS:
            try:
                resp = await self._get(
                    path,
                    params={
                        "station": native_id,
                        "start": start.strftime("%Y-%m-%d"),
                        "end": end.strftime("%Y-%m-%d"),
                        "format": "csv",
                    },
                )
                content_type = resp.headers.get(
                    "content-type", ""
                )
                if "json" in content_type:
                    return self._parse_observations_json(
                        resp.json(),
                        station_id,
                        start,
                        end,
                    )
                return self._parse_observations_csv(
                    resp.text,
                    station_id,
                    start,
                    end,
                )
            except Exception as exc:  # noqa: BLE001
                logger.debug(
                    "observation_endpoint_failed",
                    provider=self.slug,
                    path=path,
                    station=native_id,
                    error=str(exc),
                )
                last_exc = exc

        raise ConnectorError(
            self.slug,
            (
                "All observation endpoints failed for "
                f"station {native_id}: {last_exc}"
            ),
        ) from last_exc

    async def fetch_latest(
        self, station_id: str
    ) -> TimeSeriesChunk:
        """Fetch recent observations (last 365 days — historical archive)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(days=365),
            end=now,
        )

    # ------------------------------------------------------------------
    # Station parsing
    # ------------------------------------------------------------------

    def _parse_station_response(self, resp) -> list[Station]:
        """Parse a station response, trying JSON then HTML/text."""
        content_type = resp.headers.get("content-type", "")
        if "json" in content_type:
            data = resp.json()
            if not isinstance(data, list):
                raise DataFormatError(
                    self.slug,
                    "Station response is not a JSON array",
                )
            return self._parse_stations_json(data)
        # Attempt JSON parse even without content-type hint
        try:
            data = resp.json()
            if isinstance(data, list):
                return self._parse_stations_json(data)
        except Exception:  # noqa: BLE001
            pass
        raise DataFormatError(
            self.slug,
            "Unrecognised station response format",
        )

    def _parse_stations_json(
        self, data: list[dict]
    ) -> list[Station]:
        """Parse DSI station list JSON into Station models."""
        stations: list[Station] = []
        for entry in data:
            native_id = str(
                entry.get("istasyon_no", "")
            ).strip()
            if not native_id:
                continue

            try:
                lat = float(
                    str(entry.get("enlem", 0.0))
                )
                lon = float(
                    str(entry.get("boylam", 0.0))
                )
            except (TypeError, ValueError):
                lat, lon = 0.0, 0.0

            catchment_raw = entry.get("havza_alani_km2")
            catchment_area: float | None = None
            if catchment_raw is not None:
                try:
                    catchment_area = float(
                        str(catchment_raw)
                    )
                except (TypeError, ValueError):
                    catchment_area = None

            try:
                stations.append(
                    Station(
                        id=self._station_id(native_id),
                        provider=self.slug,
                        native_id=native_id,
                        name=entry.get(
                            "istasyon_adi", ""
                        ),
                        latitude=lat,
                        longitude=lon,
                        country_code="TR",
                        river=entry.get("nehir"),
                        catchment_area_km2=catchment_area,
                    )
                )
            except (ValueError, KeyError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    station=native_id,
                    error=str(exc),
                )
                continue
        return stations

    # ------------------------------------------------------------------
    # Observation parsing — JSON
    # ------------------------------------------------------------------

    def _parse_observations_json(
        self,
        data: list | dict,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Parse JSON observation records."""
        if isinstance(data, dict):
            data = data.get("data", data.get("observations", []))
        if not isinstance(data, list):
            raise DataFormatError(
                self.slug,
                "Observations JSON is not a list",
            )

        observations: list[Observation] = []
        start_naive = (
            start.replace(tzinfo=None) if start.tzinfo else start
        )
        end_naive = (
            end.replace(tzinfo=None) if end.tzinfo else end
        )

        for entry in data:
            try:
                raw_date = entry.get(
                    "tarih", entry.get("date", "")
                )
                ts = datetime.fromisoformat(
                    str(raw_date)
                )
                ts_naive = (
                    ts.replace(tzinfo=None)
                    if ts.tzinfo
                    else ts
                )
            except (ValueError, TypeError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid timestamp: {exc}",
                ) from exc

            if ts_naive < start_naive or ts_naive > end_naive:
                continue

            value = entry.get(
                "debi", entry.get("discharge", None)
            )
            discharge = self._safe_float(value)

            observations.append(
                Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=(
                        QualityFlag.RAW
                        if discharge is not None
                        else QualityFlag.MISSING
                    ),
                )
            )

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # ------------------------------------------------------------------
    # Observation parsing — CSV
    # ------------------------------------------------------------------

    def _parse_observations_csv(
        self,
        csv_text: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Parse CSV observation data from DSI FACE portal."""
        observations: list[Observation] = []
        start_naive = (
            start.replace(tzinfo=None) if start.tzinfo else start
        )
        end_naive = (
            end.replace(tzinfo=None) if end.tzinfo else end
        )

        lines = csv_text.strip().splitlines()
        for line in lines:
            line = line.strip()
            if not line:
                continue

            parts = line.split(";")
            if len(parts) < 2:
                parts = line.split(",")
            if len(parts) < 2:
                continue

            date_str = parts[0].strip()
            value_str = parts[1].strip()

            try:
                ts = self._parse_csv_date(date_str)
            except ValueError:
                continue

            ts_naive = (
                ts.replace(tzinfo=None) if ts.tzinfo else ts
            )
            if ts_naive < start_naive or ts_naive > end_naive:
                continue

            discharge: float | None = None
            skip_values = ("", "-", "bos", "eksik")
            if (
                value_str
                and value_str.lower() not in skip_values
            ):
                try:
                    discharge = float(
                        value_str.replace(",", ".")
                    )
                except (TypeError, ValueError):
                    discharge = None

            observations.append(
                Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=(
                        QualityFlag.RAW
                        if discharge is not None
                        else QualityFlag.MISSING
                    ),
                )
            )

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # ------------------------------------------------------------------
    # Utilities
    # ------------------------------------------------------------------

    @staticmethod
    def _parse_csv_date(date_str: str) -> datetime:
        """Parse a date string from DSI CSV in various formats."""
        for fmt in (
            "%Y-%m-%d",
            "%d.%m.%Y",
            "%d/%m/%Y",
            "%Y-%m-%dT%H:%M:%S",
            "%d.%m.%Y %H:%M:%S",
        ):
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        return datetime.fromisoformat(date_str)

    @staticmethod
    def _safe_float(value: object) -> float | None:
        """Safely convert a value to float, returning None on failure."""
        if value is None:
            return None
        try:
            return float(str(value))
        except (TypeError, ValueError):
            return None
