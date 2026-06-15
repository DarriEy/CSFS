"""Germany NRW connector -- OpenGeodata.NRW discharge (Abfluss) CSV archive.

North Rhine-Westphalia publishes open discharge (Abfluss, m³/s) as bulk CSV
archives on OpenGeodata.NRW, organised by river-basin catchment and decade::

    /produkte/umwelt_klima/wasser/oberflaechengewaesser/hydro/q/index.json
        -> {"datasets": [{"name", "title", "files": [{"name": "<zip>", ...}]}]}

Each dataset is one catchment (e.g. "Diemeleinzugsgebiet_Abfluesse_CSV"); each
file is one decade, e.g. ``Diemeleinzugsgebiet-NRW-Q_2020-2029_EPSG25832_CSV.zip``.
A zip holds one CSV per gauge, named ``<station_no>_<Name>_<range>_Abfluss_m3s.csv``,
semicolon-delimited::

    station_name;station_no;dateTime;value[m³/s]
    Westheim;4433000000100;2020-01-01T00:00:00+01:00;3.78

``dateTime`` is ISO-8601 with a CET/CEST offset; ``value`` is m³/s (decimal
point) with ``NA`` for missing. The archive is bulk/periodic (not realtime) and
high-resolution (15-min), so this is a weekly-tier connector that caches the
downloaded zips. Coordinates are not provided by these files.

References
----------
- Portal: https://www.opengeodata.nrw.de/
"""

from __future__ import annotations

import csv
import io
import re
import zipfile
from datetime import UTC, datetime

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

_BASE_URL = "https://www.opengeodata.nrw.de"
_Q_PATH = "/produkte/umwelt_klima/wasser/oberflaechengewaesser/hydro/q"
# A decade span embedded in a zip file name, e.g. "2020-2029".
_DECADE_RE = re.compile(r"_(\d{4})-(\d{4})_")


@register("germany_nrw")
class GermanyNRWConnector(BaseConnector):
    """NRW (Germany) discharge via the OpenGeodata.NRW CSV archive."""

    slug = "germany_nrw"
    display_name = "OpenGeodata.NRW (Germany)"
    base_url = _BASE_URL
    country_codes = ["DE"]
    # Files are large; keep the shared host from being hammered.
    max_concurrent_requests = 2

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        self._zip_cache: dict[str, bytes] = {}
        # station_no -> {"name": str, "files": [zip file names in its catchment]}
        self._station_index: dict[str, dict] = {}

    async def _fetch_index(self) -> list[dict]:
        try:
            resp = await self._get(f"{_Q_PATH}/index.json")
            data = resp.json()
        except Exception as exc:  # noqa: BLE001
            raise ConnectorError(self.slug, f"Failed to fetch NRW index: {exc}") from exc
        datasets = data.get("datasets")
        if not isinstance(datasets, list):
            raise DataFormatError(self.slug, "NRW index has no 'datasets' list")
        return datasets

    async def _get_zip(self, filename: str) -> bytes:
        if filename not in self._zip_cache:
            resp = await self._get(f"{_Q_PATH}/{filename}")
            self._zip_cache[filename] = resp.content
        return self._zip_cache[filename]

    @staticmethod
    def _latest_file(file_names: list[str]) -> str | None:
        """Pick the most recent decade zip among a catchment's file names."""
        best: str | None = None
        best_year = -1
        for name in file_names:
            m = _DECADE_RE.search(name)
            if m and int(m.group(2)) > best_year:
                best, best_year = name, int(m.group(2))
        return best

    async def fetch_stations(self) -> list[Station]:
        """Enumerate gauges from the latest-decade zip of each catchment."""
        datasets = await self._fetch_index()
        stations: list[Station] = []
        for ds in datasets:
            files = ds.get("files") or []
            all_names = [
                f.get("name", "") for f in files if _DECADE_RE.search(f.get("name", ""))
            ]
            latest = self._latest_file(all_names)
            if not latest:
                continue
            try:
                blob = await self._get_zip(latest)
                members = zipfile.ZipFile(io.BytesIO(blob)).namelist()
            except Exception as exc:  # noqa: BLE001
                logger.warning("nrw_dataset_failed", provider=self.slug,
                               dataset=ds.get("name"), error=str(exc)[:120])
                continue
            for member in members:
                if not member.lower().endswith(".csv"):
                    continue
                native_id, name = self._parse_member_name(member)
                if not native_id or native_id in self._station_index:
                    continue
                self._station_index[native_id] = {"name": name, "files": all_names}
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=name or native_id,
                    latitude=0.0,  # OpenGeodata.NRW CSVs carry no coordinates
                    longitude=0.0,
                    country_code="DE",
                    is_active=True,
                ))
        logger.info("stations_fetched", provider=self.slug, count=len(stations))
        return stations

    @staticmethod
    def _parse_member_name(member: str) -> tuple[str, str]:
        """Extract (station_no, name) from `<no>_<Name>_<range>_Abfluss_m3s.csv`."""
        base = member.rsplit("/", 1)[-1]
        parts = base.split("_")
        if len(parts) < 2 or not parts[0].isdigit():
            return "", ""
        native_id = parts[0]
        # Name is everything between the id and the trailing date/marker fields.
        name_parts = parts[1:]
        while name_parts and (
            name_parts[-1] in ("Abfluss", "m3s.csv", "m3s")
            or re.fullmatch(r"\d{4}-\d{4}", name_parts[-1])
        ):
            name_parts.pop()
        return native_id, " ".join(name_parts)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge for a station over [start, end] from the decade zips."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        if not self._station_index:
            await self.fetch_stations()
        info = self._station_index.get(native_id)
        if info is None:
            return self._empty_chunk(station_id)

        # Decade files whose span overlaps the requested window.
        wanted = [
            f for f in info["files"]
            if (m := _DECADE_RE.search(f))
            and int(m.group(1)) <= end.year
            and int(m.group(2)) >= start.year
        ]
        observations: list[Observation] = []
        for filename in wanted:
            try:
                blob = await self._get_zip(filename)
                zf = zipfile.ZipFile(io.BytesIO(blob))
            except Exception as exc:  # noqa: BLE001
                logger.warning("nrw_zip_failed", provider=self.slug,
                               file=filename, error=str(exc)[:120])
                continue
            member = next(
                (n for n in zf.namelist()
                 if n.rsplit("/", 1)[-1].startswith(f"{native_id}_")),
                None,
            )
            if member is None:
                continue
            text = zf.open(member).read().decode("utf-8-sig")
            observations.extend(self._parse_csv(text, station_id, start, end))

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    def _parse_csv(
        self, text: str, station_id: str, start: datetime, end: datetime,
    ) -> list[Observation]:
        observations: list[Observation] = []
        reader = csv.DictReader(text.splitlines(), delimiter=";")
        value_key = next(
            (f for f in (reader.fieldnames or []) if f.startswith("value")),
            "value[m³/s]",
        )
        for row in reader:
            raw_ts = row.get("dateTime")
            if not raw_ts:
                continue
            try:
                ts = datetime.fromisoformat(raw_ts.strip())
            except ValueError:
                continue
            if ts.tzinfo is None:
                ts = ts.replace(tzinfo=UTC)
            ts = ts.astimezone(UTC)
            if ts < start or ts > end:
                continue
            raw_val = (row.get(value_key) or "").strip()
            if not raw_val or raw_val == "NA":
                discharge, quality = None, QualityFlag.MISSING
            else:
                try:
                    discharge, quality = float(raw_val), QualityFlag.RAW
                except ValueError:
                    discharge, quality = None, QualityFlag.MISSING
            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=discharge,
                quality=quality,
            ))
        return observations

    def _empty_chunk(self, station_id: str) -> TimeSeriesChunk:
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )
