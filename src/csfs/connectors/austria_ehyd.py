"""eHYD connector — Austrian hydrographic archive (Hydrographische Archivdaten)."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()


@register("austria_ehyd")
class AustriaEhydConnector(BaseConnector):
    slug = "austria_ehyd"
    display_name = "eHYD (Hydrographischer Dienst Österreich)"
    base_url = "https://ehyd.gv.at"
    country_codes = ["AT"]

    # Primary and fallback API paths for station listing
    _STATION_LIST_PATH = "/eHYD/api/OGDAbflussMessstellenListe"

    # Primary and fallback API paths for daily discharge observations
    _OBSERVATIONS_PATH = "/eHYD/api/OGDAbflussTagesmittel"
    _OBSERVATIONS_FALLBACK_TEMPLATE = "/eHYD/MessstellenExtra662/QDaily/{hzbnr}/download"

    async def fetch_stations(self) -> list[Station]:
        """Return all discharge stations from the eHYD OGD station list."""
        try:
            resp = await self._get(self._STATION_LIST_PATH)
            data = resp.json()
        except (ConnectorError, Exception) as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch station list: {exc}",
            ) from exc

        if not isinstance(data, list):
            raise DataFormatError(self.slug, "Station list response is not a JSON array")

        return self._parse_stations(data)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch daily mean discharge for a station over a time range."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        try:
            chunk = await self._fetch_observations_primary(native_id, station_id, start, end)
        except (ConnectorError, Exception) as exc:
            logger.info(
                "primary_endpoint_failed_trying_fallback",
                provider=self.slug,
                station=native_id,
                error=str(exc),
            )
            try:
                chunk = await self._fetch_observations_fallback(native_id, station_id, start, end)
            except (ConnectorError, Exception) as fallback_exc:
                raise ConnectorError(
                    self.slug,
                    f"All observation endpoints failed for station {native_id}: {fallback_exc}",
                ) from fallback_exc
        return chunk

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent discharge observations (last 30 days for daily data)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(days=30),
            end=now,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_stations(self, data: list[dict]) -> list[Station]:
        """Parse the eHYD station list JSON into Station models."""
        stations: list[Station] = []
        for entry in data:
            native_id = str(entry.get("hzbnr", "")).strip()
            if not native_id:
                continue

            try:
                lat = float(entry.get("breite", 0.0))
                lon = float(entry.get("laenge", 0.0))
            except (TypeError, ValueError):
                lat, lon = 0.0, 0.0

            catchment = entry.get("flaeche_km2")
            catchment_area: float | None = None
            if catchment is not None:
                try:
                    catchment_area = float(catchment)
                except (TypeError, ValueError):
                    catchment_area = None

            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=entry.get("messstellenname", ""),
                    latitude=lat,
                    longitude=lon,
                    country_code="AT",
                    river=entry.get("gewaesser"),
                    catchment_area_km2=catchment_area,
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

    async def _fetch_observations_primary(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch observations via the OGD JSON API."""
        resp = await self._get(
            self._OBSERVATIONS_PATH,
            params={"hzbnr": native_id},
        )
        data = resp.json()
        if not isinstance(data, list):
            raise DataFormatError(self.slug, "Observations response is not a JSON array")
        return self._parse_observations_json(data, station_id, start, end)

    async def _fetch_observations_fallback(
        self,
        native_id: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch observations via the CSV download fallback endpoint."""
        path = self._OBSERVATIONS_FALLBACK_TEMPLATE.format(hzbnr=native_id)
        resp = await self._get(path)
        return self._parse_observations_csv(resp.text, station_id, start, end)

    def _parse_observations_json(
        self,
        data: list[dict],
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Parse JSON observation records, filtering to the requested time range."""
        observations: list[Observation] = []
        start_naive = start.replace(tzinfo=None) if start.tzinfo else start
        end_naive = end.replace(tzinfo=None) if end.tzinfo else end

        for entry in data:
            try:
                raw_date = entry.get("datum", "")
                ts = datetime.fromisoformat(raw_date)
                ts_naive = ts.replace(tzinfo=None) if ts.tzinfo else ts
            except (ValueError, TypeError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid timestamp in observation: {exc}",
                ) from exc

            if ts_naive < start_naive or ts_naive > end_naive:
                continue

            value = entry.get("wert")
            discharge: float | None = None
            if value is not None:
                try:
                    discharge = float(value)
                except (TypeError, ValueError):
                    discharge = None

            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=discharge,
                quality=QualityFlag.RAW if discharge is not None else QualityFlag.MISSING,
            ))

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    def _parse_observations_csv(
        self,
        csv_text: str,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Parse CSV observation data from the fallback download endpoint."""
        observations: list[Observation] = []
        start_naive = start.replace(tzinfo=None) if start.tzinfo else start
        end_naive = end.replace(tzinfo=None) if end.tzinfo else end

        lines = csv_text.strip().splitlines()
        # Skip header line(s) — first line with a semicolon separator is data
        for line in lines:
            line = line.strip()
            if not line:
                continue

            # Try semicolon separator (common in Austrian/German CSV)
            parts = line.split(";")
            if len(parts) < 2:
                # Try comma separator
                parts = line.split(",")
            if len(parts) < 2:
                continue

            date_str = parts[0].strip()
            value_str = parts[1].strip()

            # Skip header rows
            try:
                ts = self._parse_csv_date(date_str)
            except ValueError:
                continue

            ts_naive = ts.replace(tzinfo=None) if ts.tzinfo else ts
            if ts_naive < start_naive or ts_naive > end_naive:
                continue

            discharge: float | None = None
            if value_str and value_str.lower() not in ("", "lücke", "luecke", "-"):
                try:
                    # Austrian CSVs may use comma as decimal separator
                    discharge = float(value_str.replace(",", "."))
                except (TypeError, ValueError):
                    discharge = None

            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=discharge,
                quality=QualityFlag.RAW if discharge is not None else QualityFlag.MISSING,
            ))

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    @staticmethod
    def _parse_csv_date(date_str: str) -> datetime:
        """Parse a date string from eHYD CSV in various formats."""
        for fmt in ("%d.%m.%Y", "%Y-%m-%d", "%d.%m.%Y %H:%M:%S", "%Y-%m-%dT%H:%M:%S"):
            try:
                return datetime.strptime(date_str, fmt)
            except ValueError:
                continue
        # Last resort: ISO format
        return datetime.fromisoformat(date_str)
