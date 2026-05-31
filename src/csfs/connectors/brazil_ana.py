# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Brazil ANA connector — Agência Nacional de Águas (HidroWeb SOAP service).

ANA's public ``telemetriaws1.ana.gov.br/ServiceANA.asmx`` service (the one the
``hydrobr`` ecosystem uses) accepts plain HTTP GET and returns XML DiffGrams.
Two operations are used:

* ``HidroInventario`` — station metadata. The unbounded query returns an empty
  result (server-side size cap), but filtering by ``codBacia`` works, and Brazil
  has only 8 major hydrographic basins (codes 1–8), so the full fluviometric
  inventory is enumerable in 8 calls. ``tpEst=1`` selects fluviometric stations.
* ``HidroSerieHistorica`` — the daily-mean discharge archive. Each record is one
  month carrying ``Vazao01``…``Vazao31`` (daily mean m³/s) plus a
  ``NivelConsistencia`` (1 = raw, 2 = consolidated). We expand those into daily
  observations and prefer consolidated over raw for the same day.

Because ``HidroSerieHistorica`` is the *archive*, data lags the present by months
— this is a historical/backfill connector (like ``adhi``). Query past windows;
the short recent-window tiers will usually return nothing.
"""

from __future__ import annotations

import calendar
from datetime import UTC, datetime
from xml.etree import ElementTree as ET

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

_SERVICE = "/ServiceANA.asmx"
# Brazil's eight major hydrographic basins (first digit of the station code).
_BASINS = range(1, 9)
# tipoDados=3 selects discharge (vazões) in HidroSerieHistorica.
_TIPO_VAZAO = "3"


@register("brazil_ana")
class BrazilAnaConnector(BaseConnector):
    """Connector for Brazil's ANA daily-discharge archive."""

    slug = "brazil_ana"
    display_name = "ANA (Brazil)"
    base_url = "https://telemetriaws1.ana.gov.br"
    country_codes = ["BR"]

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        self._stations: list[Station] | None = None

    # -----------------------------------------------------------------
    # Public API
    # -----------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all fluviometric stations across the 8 major basins."""
        if self._stations is not None:
            return self._stations

        stations: list[Station] = []
        seen: set[str] = set()
        for basin in _BASINS:
            try:
                resp = await self._get(
                    f"{_SERVICE}/HidroInventario",
                    params=self._inventory_params(basin),
                )
            except (httpx.HTTPStatusError, ConnectorError) as exc:
                logger.warning(
                    "basin_inventory_failed",
                    provider=self.slug, basin=basin, error=str(exc)[:120],
                )
                continue
            for st in self._parse_inventory(resp.content):
                if st.native_id not in seen:
                    seen.add(st.native_id)
                    stations.append(st)

        logger.info("stations_fetched", provider=self.slug, count=len(stations))
        self._stations = stations
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch daily-mean discharge for a station over a time range."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        try:
            resp = await self._get(
                f"{_SERVICE}/HidroSerieHistorica",
                params={
                    "codEstacao": native_id,
                    "dataInicio": _fmt_date(start),
                    "dataFim": _fmt_date(end),
                    "tipoDados": _TIPO_VAZAO,
                    "nivelConsistencia": "",
                },
            )
        except (httpx.HTTPStatusError, ConnectorError) as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch observations for {native_id}: {exc}",
            ) from exc

        observations = self._parse_series(resp.content, station_id, start, end)
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # -----------------------------------------------------------------
    # Internal helpers
    # -----------------------------------------------------------------

    @staticmethod
    def _inventory_params(basin: int) -> dict[str, str]:
        return {
            "codEstDE": "", "codEstATE": "", "tpEst": "1", "nmEst": "",
            "nmRio": "", "codSubBacia": "", "codBacia": str(basin),
            "nmMunicipio": "", "nmEstado": "", "sgResp": "", "sgOper": "",
            "telemetrica": "",
        }

    def _parse_inventory(self, content: bytes) -> list[Station]:
        """Parse a HidroInventario DiffGram into Station models."""
        root = _parse_xml(self.slug, content)
        stations: list[Station] = []
        for rec in _records_with(root, "Codigo"):
            code = (rec.get("Codigo") or "").strip()
            if not code:
                continue
            lat = _parse_float(rec.get("Latitude"))
            lon = _parse_float(rec.get("Longitude"))
            if lat is None or lon is None:
                continue
            river = (rec.get("RioNome") or "").strip() or None
            stations.append(Station(
                id=self._station_id(code),
                provider=self.slug,
                native_id=code,
                name=(rec.get("Nome") or code).strip(),
                latitude=lat,
                longitude=lon,
                country_code="BR",
                river=river,
                catchment_area_km2=_parse_float(rec.get("AreaDrenagem")),
                elevation_m=_parse_float(rec.get("Altitude")),
            ))
        return stations

    def _parse_series(
        self,
        content: bytes,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Expand monthly HidroSerieHistorica records into daily observations.

        Prefers consolidated (NivelConsistencia=2) over raw (1) for a given day.
        """
        root = _parse_xml(self.slug, content)
        # date -> (consistencia, discharge); higher consistencia wins.
        best: dict[datetime, tuple[int, float]] = {}

        for rec in _records_with(root, "DataHora"):
            month_start = _parse_datahora(rec.get("DataHora"))
            if month_start is None:
                continue
            consist = int(_parse_float(rec.get("NivelConsistencia")) or 1)
            days = calendar.monthrange(month_start.year, month_start.month)[1]
            for day in range(1, days + 1):
                value = _parse_float(rec.get(f"Vazao{day:02d}"))
                if value is None:
                    continue
                ts = datetime(month_start.year, month_start.month, day, tzinfo=UTC)
                if not (start <= ts <= end):
                    continue
                prev = best.get(ts)
                if prev is None or consist > prev[0]:
                    best[ts] = (consist, value)

        observations = [
            Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=value,
                quality=QualityFlag.GOOD if consist >= 2 else QualityFlag.RAW,
            )
            for ts, (consist, value) in sorted(best.items())
        ]
        return observations


# ---------------------------------------------------------------------------
# Module-level parsing helpers
# ---------------------------------------------------------------------------


def _local(tag: str) -> str:
    """Strip any XML namespace from a tag, returning the local name."""
    return tag.rsplit("}", 1)[-1]


def _parse_xml(slug: str, content: bytes) -> ET.Element:
    try:
        return ET.fromstring(content)
    except ET.ParseError as exc:
        raise DataFormatError(slug, f"Invalid ANA XML: {exc}") from exc


def _records_with(root: ET.Element, child_name: str) -> list[dict[str, str | None]]:
    """Find DiffGram record elements that have a child named ``child_name``.

    Returns each record flattened to a {local_child_tag: text} dict. Namespaces
    are stripped so the schema section (which has no such child elements) is
    naturally excluded.
    """
    records: list[dict[str, str | None]] = []
    for el in root.iter():
        fields = {_local(c.tag): c.text for c in el}
        if child_name in fields:
            records.append(fields)
    return records


def _parse_float(value: str | None) -> float | None:
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    try:
        return float(text)
    except ValueError:
        return None


def _fmt_date(dt: datetime) -> str:
    """ANA expects dd/mm/yyyy."""
    return dt.strftime("%d/%m/%Y")


def _parse_datahora(value: str | None) -> datetime | None:
    """Parse a HidroSerieHistorica DataHora (``YYYY-MM-DD HH:MM:SS``)."""
    if value is None:
        return None
    text = value.strip()
    if not text:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(text, fmt)
        except ValueError:
            continue
    return None
