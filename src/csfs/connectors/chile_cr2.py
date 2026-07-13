# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Chile CR2 explorador connector — DGA gauge discharge via CR2.

The official DGA/SNIA portal is browser-only and its legacy ArcGIS endpoint
is dead, so Chilean discharge is acquired through the CR2 explorador
(Center for Climate and Resilience Research, Universidad de Chile), which
re-serves DGA daily-mean discharge through a scriptable endpoint at
``https://explorador.cr2.cl/request.php?options=<url-encoded JSON>``.

Two request modes are used (both live-verified 2026-07):

* ``action=["map"]`` returns the full station listing for the daily
  discharge layer (``qflxDaily``): 548 DGA gauges with coordinates, names,
  basins, elevations, and record spans. ``fetch_stations`` is fully live.
* ``action=["export_series"]`` with ``series.sites=["<gauge_id>"]`` returns
  JSON (historically HTML) containing a link to a server-generated CSV
  under ``/tmp/<token>/EC_series.csv`` with columns ``agno`` (year),
  ``mes`` (month), ``dia`` (day), ``valor`` (daily mean discharge, m³/s).
  The host in the advertised link is unreliable (currently
  ``http://localhost:8080``), so only the ``/tmp/...`` path is kept and
  fetched from the public host.

The server ignores the requested time window for CSV exports and always
returns the full record, so ``fetch_observations`` slices client-side.
Note that CR2's discharge snapshot currently ends 2020-06-05: this is a
historical/backfill source, not a real-time feed.
"""

from __future__ import annotations

import asyncio
import json
import re
from datetime import UTC, datetime
from typing import Any

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import (
    Observation,
    QualityFlag,
    Resolution,
    Station,
    TimeSeriesChunk,
    Variable,
)
from csfs.core.registry import register

logger = structlog.get_logger()

_REQUEST_PATH = "/request.php"

#: Path of the server-generated CSV inside the request.php response. Only the
#: path is used — the advertised scheme/host is broken (localhost:8080).
_TMP_CSV_RE = re.compile(r"/tmp/[^\"'\s>]+?\.csv")

#: Epoch bounds mirroring the explorador UI's full-range query (1940-01-01).
_EPOCH_START = -946771200

#: DGA missing-value sentinel occasionally present in exports.
_MISSING_SENTINEL = -999.0

#: Seconds to wait between the request.php call and the tmp-CSV download
#: (the explorador is a small academic server; RivRetrieve paces similarly).
_DEFAULT_PACING_S = 0.3

#: Google-map view block required by the export_series options payload.
_EXPORT_VIEW = {
    "frame": "Vista Actual",
    "map": "roadmap",
    "clat": -18.0036,
    "clon": -69.6331,
    "zoom": 5,
    "width": 461,
    "height": 2207,
}


def _first(fields: dict[str, Any], key: str) -> str | None:
    """Return the first entry of a CR2 point-metadata field (lists of str)."""
    value = fields.get(key)
    if isinstance(value, list) and value:
        return str(value[0]).strip() or None
    return None


def _parse_date(raw: str | None) -> datetime | None:
    """Parse a 'YYYY-MM-DD' record-span date to a tz-aware UTC datetime."""
    if not raw:
        return None
    try:
        return datetime.strptime(raw, "%Y-%m-%d").replace(tzinfo=UTC)
    except ValueError:
        return None


@register("chile_cr2")
class ChileCr2Connector(BaseConnector):
    """Connector for Chilean DGA discharge re-served by the CR2 explorador."""

    slug = "chile_cr2"
    display_name = "CR2 Explorador (Chile, DGA)"
    base_url = "https://explorador.cr2.cl"
    country_codes = ["CL"]
    supported_variables = ("discharge",)
    # One station = two sequential requests against a small academic server.
    max_concurrent_requests = 1

    async def __aenter__(self) -> ChileCr2Connector:
        await super().__aenter__()
        # request.php serves the explorador web app; present a browser-
        # compatible User-Agent while still identifying CSFS.
        self.client.headers["User-Agent"] = (
            "Mozilla/5.0 (compatible; CSFS/0.1; +https://github.com/DarriEy/CSFS)"
        )
        return self

    @property
    def _pacing_s(self) -> float:
        return float(self.config.get("pacing_s", _DEFAULT_PACING_S))

    def _options_json(self, *, sites: list[str], action: str) -> str:
        """Serialize the request.php ``options`` payload for one action."""
        export: dict[str, Any]
        if action == "export_series":
            export = {"map": "Shapefile", "series": "CSV", "view": _EXPORT_VIEW}
        else:
            export = {"map": None, "series": None}
        options = {
            "variable": {
                "id": "qflxDaily",
                "var": "caudal",
                "intv": "daily",
                "season": "year",
                "stat": "mean",
                "minFrac": 80,
            },
            # Full-range window: the server ignores it for CSV exports and
            # always returns the complete record; slicing happens client-side.
            "time": {
                "start": _EPOCH_START,
                "end": int(datetime.now(UTC).timestamp()),
                "months": "Año completo",
            },
            "anomaly": {
                "enabled": False,
                "type": "dif",
                "rank": "no",
                "start_year": 1980,
                "end_year": 2010,
                "minFrac": 70,
            },
            "map": {
                "stat": "mean",
                "minFrac": 10,
                "borderColor": "7F7F7F",
                "colorRamp": "Jet",
                "showNaN": False,
                "limits": {"range": [5, 95], "size": [4, 12], "type": "prc"},
            },
            "series": {"sites": sites, "start": None, "end": None},
            "export": export,
            "action": [action],
        }
        return json.dumps(options, separators=(",", ":"), ensure_ascii=False)

    # ------------------------------------------------------------------
    # Stations
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Fetch all daily-discharge gauges live from the map layer."""
        try:
            resp = await self._get(
                _REQUEST_PATH,
                params={"options": self._options_json(sites=[], action="map")},
            )
            payload = resp.json()
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch/parse station map listing: {exc}",
            ) from exc

        points = (payload.get("map") or {}).get("points") or []
        stations: list[Station] = []
        for point in points:
            try:
                fields: dict[str, Any] = point.get("fields") or {}
                native_id = str(
                    point.get("id") or _first(fields, "codigo_estacion") or ""
                ).strip()
                if not native_id:
                    continue
                elevation = point.get("alt")
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=_first(fields, "nombre") or native_id,
                    latitude=float(point["lat"]),
                    longitude=float(point["lon"]),
                    country_code="CL",
                    river=_first(fields, "nombre_cuenca"),
                    catchment_area_km2=None,  # not exposed by the explorador
                    elevation_m=float(elevation) if elevation is not None else None,
                    record_start=_parse_date(_first(fields, "inicio_observaciones")),
                    record_end=_parse_date(_first(fields, "fin_observaciones")),
                    is_active=True,
                ))
            except (KeyError, TypeError, ValueError):
                continue

        logger.info("stations_fetched", provider=self.slug, count=len(stations))
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
        """Fetch daily-mean discharge for one gauge, sliced to [start, end]."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        try:
            resp = await self._get(
                _REQUEST_PATH,
                params={
                    "options": self._options_json(
                        sites=[native_id], action="export_series",
                    ),
                },
            )
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"export_series request failed for {native_id}: {exc}",
            ) from exc

        csv_path = self._extract_csv_path(resp.text)
        if csv_path is None:
            raise ConnectorError(
                self.slug,
                f"No CSV export link in response for {native_id}: {resp.text[:200]!r}",
            )

        # Gentle pacing between the export request and the CSV download.
        if self._pacing_s > 0:
            await asyncio.sleep(self._pacing_s)

        try:
            csv_resp = await self._get(csv_path)
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to download generated CSV {csv_path} for {native_id}: {exc}",
            ) from exc

        observations = self._parse_csv(station_id, csv_resp.text, start, end)
        logger.info(
            "observations_fetched",
            provider=self.slug,
            station=station_id,
            count=len(observations),
        )
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    def _extract_csv_path(self, body: str) -> str | None:
        """Extract the /tmp/... CSV path from a request.php response.

        The current API returns JSON with ``export.series.url``; older
        deployments returned HTML with an embedded absolute link. Either way
        only the path is trusted — the advertised host is rewritten to
        ``base_url`` (the server currently emits ``http://localhost:8080``).
        """
        haystack = body
        try:
            payload = json.loads(body)
        except ValueError:
            payload = None
        if isinstance(payload, dict):
            url = ((payload.get("export") or {}).get("series") or {}).get("url")
            if isinstance(url, str):
                haystack = url
        match = _TMP_CSV_RE.search(haystack)
        return match.group(0) if match else None

    def _parse_csv(
        self,
        station_id: str,
        text: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse the agno/mes/dia/valor CSV and slice to [start, end]."""
        lines = text.strip().splitlines()
        if not lines:
            return []
        header = [col.strip().lower() for col in lines[0].split(",")]
        try:
            idx = [header.index(col) for col in ("agno", "mes", "dia", "valor")]
        except ValueError as exc:
            raise ConnectorError(
                self.slug,
                f"Unexpected CSV header for {station_id}: {lines[0]!r}",
            ) from exc
        i_year, i_month, i_day, i_value = idx

        observations: list[Observation] = []
        for line in lines[1:]:
            parts = [part.strip() for part in line.split(",")]
            if len(parts) <= max(idx):
                continue
            try:
                ts = datetime(
                    int(parts[i_year]), int(parts[i_month]), int(parts[i_day]),
                    tzinfo=UTC,
                )
                value = float(parts[i_value])
            except (TypeError, ValueError):
                continue
            if value <= _MISSING_SENTINEL or not (start <= ts <= end):
                continue
            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                variable=Variable.DISCHARGE,
                resolution=Resolution.DAILY_MEAN,
                value=value,
                quality=QualityFlag.RAW,
            ))
        return observations
