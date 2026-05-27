"""Ireland OPW (Office of Public Works) connector — Water Level data."""

from __future__ import annotations

import csv
import gzip
import io
from datetime import UTC, datetime

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

_EPA_METADATA_URL = (
    "https://epawebapp.epa.ie"
    "/hydronet/output/internet/layers/10/index.json"
)

_QUALITY_MAP = {
    "Good": QualityFlag.GOOD,
    "Suspect": QualityFlag.SUSPECT,
    "Estimated": QualityFlag.ESTIMATED,
    "Missing": QualityFlag.MISSING,
}


@register("ireland_opw")
class IrelandOPWConnector(BaseConnector):
    slug = "ireland_opw"
    display_name = "OPW Water Level (Ireland)"
    base_url = "https://waterlevel.ie"
    country_codes = ["IE"]

    async def fetch_stations(self) -> list[Station]:
        """Return stations from OPW GeoJSON (has coordinates), CSV fallback."""
        try:
            resp = await self._get("/geojson/")
            stations = self._parse_geojson(resp.json())
            if stations:
                return stations
        except Exception:
            pass
        resp = await self._get("/data/station_list.csv")
        return self._parse_station_csv(resp.text)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch daily-mean observations from the gzipped CSV archive."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        path = f"/data/dailymean/{native_id}_dailymean.csv.gz"

        try:
            resp = await self._get(path)
        except httpx.HTTPStatusError as exc:
            logger.warning(
                "dailymean_not_available",
                provider=self.slug,
                station=native_id,
                status=exc.response.status_code,
            )
            return TimeSeriesChunk(
                station_id=station_id,
                provider=self.slug,
                observations=[],
                fetched_at=datetime.now(UTC),
            )

        return self._parse_dailymean_csv(
            resp.content, station_id, start, end,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _get_absolute(self, url: str) -> httpx.Response:
        """GET an absolute URL (outside base_url) using the shared client."""
        async with httpx.AsyncClient(
            timeout=httpx.Timeout(60.0, connect=10.0),
            headers={
                "User-Agent": "CSFS/0.1 (https://github.com/csfs)",
            },
            follow_redirects=True,
        ) as tmp_client:
            resp = await tmp_client.get(url)
            if resp.status_code not in (200, 206):
                resp.raise_for_status()
            return resp

    def _parse_geojson(self, data: dict) -> list[Station]:
        """Parse the OPW GeoJSON station feed with real coordinates."""
        stations: list[Station] = []
        for feat in data.get("features", []):
            props = feat.get("properties", {})
            ref = props.get("ref", "")
            if not ref or len(ref) < 5:
                continue
            native_id = ref[-5:]
            geom = feat.get("geometry", {})
            coords = geom.get("coordinates", [])
            if len(coords) < 2:
                continue
            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=props.get("name") or native_id,
                    latitude=float(coords[1]),
                    longitude=float(coords[0]),
                    country_code="IE",
                ))
            except (ValueError, KeyError):
                continue
        return stations

    def _parse_station_csv(self, text: str) -> list[Station]:
        """Parse station_list.csv: name,label format."""
        stations: list[Station] = []
        for line in text.strip().splitlines()[1:]:
            parts = line.split(",", 1)
            if len(parts) < 2:
                continue
            native_id = parts[0].strip()
            if not native_id or native_id == "name":
                continue
            stations.append(Station(
                id=self._station_id(native_id),
                provider=self.slug,
                native_id=native_id,
                name=parts[1].strip(),
                latitude=0.0,
                longitude=0.0,
                country_code="IE",
            ))
        return stations

    def _parse_epa_stations(self, data: list[dict]) -> list[Station]:
        """Parse the EPA HydroNet JSON station index."""
        stations: list[Station] = []
        for entry in data:
            native_id = entry.get("L1_ts_name") or ""
            name = entry.get("metadata_station_name") or ""
            if not native_id:
                continue

            try:
                lat = float(
                    str(entry.get("metadata_station_latitude", ""))
                )
                lon = float(
                    str(entry.get("metadata_station_longitude", ""))
                )
            except (ValueError, TypeError):
                continue

            stations.append(Station(
                id=self._station_id(native_id),
                provider=self.slug,
                native_id=native_id,
                name=name or native_id,
                latitude=lat,
                longitude=lon,
                country_code="IE",
                is_active=bool(entry.get("L1_DATA_AVAILABLE")),
            ))
        return stations

    def _parse_dailymean_csv(
        self,
        raw: bytes,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Decompress a gzipped daily-mean CSV and parse observations."""
        try:
            decompressed = gzip.decompress(raw)
        except (gzip.BadGzipFile, OSError) as exc:
            raise DataFormatError(
                self.slug,
                f"Failed to decompress dailymean CSV: {exc}",
            ) from exc

        text = decompressed.decode("utf-8-sig")
        reader = csv.DictReader(io.StringIO(text))

        observations: list[Observation] = []
        for row in reader:
            date_str = row.get("Date") or row.get("date") or ""
            if not date_str:
                continue

            try:
                ts = datetime.fromisoformat(date_str)
            except ValueError:
                try:
                    ts = datetime.strptime(date_str.strip(), "%Y/%m/%d %H:%M:%S")
                except ValueError:
                    try:
                        ts = datetime.strptime(date_str.strip(), "%Y/%m/%d")
                    except ValueError:
                        continue

            # Filter to requested window
            ts_naive = ts.replace(tzinfo=None)
            start_naive = start.replace(tzinfo=None)
            end_naive = end.replace(tzinfo=None)
            if ts_naive < start_naive or ts_naive > end_naive:
                continue

            raw_val = row.get("Value") or row.get("value")
            if raw_val is None or str(raw_val).strip() == "":
                discharge = None
            else:
                try:
                    discharge = float(str(raw_val))
                except ValueError:
                    discharge = None

            quality_str = (
                row.get("Quality") or row.get("quality") or ""
            )
            quality = _QUALITY_MAP.get(
                quality_str.strip(), QualityFlag.RAW,
            )
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
