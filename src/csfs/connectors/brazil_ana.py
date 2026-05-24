"""Brazil ANA (Agência Nacional de Águas) telemetry connector."""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import UTC, datetime

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()


@register("brazil_ana")
class BrazilANAConnector(BaseConnector):
    slug = "brazil_ana"
    display_name = "Brazil ANA Telemetry"
    base_url = "https://telemetriaws1.ana.gov.br/ServiceANA.asmx"
    country_codes = ["BR"]

    async def __aenter__(self) -> BrazilANAConnector:
        self._client = httpx.AsyncClient(
            base_url=self.base_url,
            timeout=httpx.Timeout(60.0, connect=10.0),
            headers={
                "User-Agent": "CSFS/0.1 (https://github.com/csfs)",
                "Accept": "text/xml",
            },
            follow_redirects=True,
        )
        return self

    async def fetch_stations(self) -> list[Station]:
        """Fetch all active fluviometric telemetry stations from ANA."""
        resp = await self._get("/HidroInventario", params={
            "codEstDE": "",
            "codEstATE": "",
            "tpEst": "1",
            "nmEst": "",
            "nmRio": "",
            "codSubBacia": "",
            "codBacia": "",
            "nmMunicipio": "",
            "nmEstado": "",
            "sgResp": "",
            "sgOper": "",
            "telession": "1",
        })
        return self._parse_stations_xml(resp.text)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for a station over a time range."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        resp = await self._get("/DadosHidrometeorologicos", params={
            "codEstacao": native_id,
            "dataInicio": start.strftime("%d/%m/%Y"),
            "dataFim": end.strftime("%d/%m/%Y"),
        })
        return self._parse_observations_xml(resp.text, station_id)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent 24h of observations."""
        from datetime import timedelta

        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    def _parse_stations_xml(self, text: str) -> list[Station]:
        """Parse the HidroInventario XML response into Station models."""
        try:
            root = ET.fromstring(text)
        except ET.ParseError as exc:
            raise DataFormatError(self.slug, f"Invalid XML in station response: {exc}") from exc

        stations: list[Station] = []
        # ANA nests <Table> elements inside a <DataSet> or directly under diffgram
        for table in root.iter("Table"):
            try:
                codigo = self._xml_text(table, "Codigo")
                if not codigo:
                    continue

                lat = self._xml_float(table, "Latitude")
                lon = self._xml_float(table, "Longitude")
                if lat is None or lon is None:
                    continue

                operando = self._xml_text(table, "Operando")
                is_active = operando == "1" if operando else True

                stations.append(Station(
                    id=self._station_id(codigo),
                    provider=self.slug,
                    native_id=codigo,
                    name=self._xml_text(table, "Nome") or codigo,
                    latitude=lat,
                    longitude=lon,
                    country_code="BR",
                    river=self._xml_text(table, "RioNome"),
                    catchment_area_km2=self._xml_float(table, "AreaDrenagem"),
                    is_active=is_active,
                ))
            except (ValueError, TypeError) as exc:
                logger.debug("skipping_station", error=str(exc))
                continue

        return stations

    def _parse_observations_xml(self, text: str, station_id: str) -> TimeSeriesChunk:
        """Parse DadosHidrometeorologicos XML response into a TimeSeriesChunk."""
        if not text or not text.strip():
            return TimeSeriesChunk(
                station_id=station_id,
                provider=self.slug,
                observations=[],
                fetched_at=datetime.now(UTC),
            )

        try:
            root = ET.fromstring(text)
        except ET.ParseError as exc:
            raise DataFormatError(
                self.slug, f"Invalid XML in observations response: {exc}"
            ) from exc

        observations: list[Observation] = []
        for item in root.iter("DadosHidrometworolgicos"):
            try:
                data_hora = self._xml_text(item, "DataHora")
                if not data_hora:
                    continue

                timestamp = self._parse_ana_datetime(data_hora)
                discharge = self._extract_discharge(item)

                observations.append(Observation(
                    station_id=station_id,
                    timestamp=timestamp,
                    discharge_m3s=discharge,
                    quality=QualityFlag.RAW if discharge is not None else QualityFlag.MISSING,
                ))
            except (ValueError, TypeError) as exc:
                logger.debug("skipping_observation", error=str(exc))
                continue

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    def _extract_discharge(self, item: ET.Element) -> float | None:
        """Extract discharge from Vazao element, preferring Media then first non-null."""
        # Try Media (mean) first
        media = self._xml_float(item, "Media")
        if media is not None:
            return media

        # Fall back to Maxima or Minima
        for field in ("Maxima", "Minima"):
            val = self._xml_float(item, field)
            if val is not None:
                return val

        # Try the Vazao text directly if present
        vazao = self._xml_float(item, "Vazao")
        return vazao

    @staticmethod
    def _parse_ana_datetime(text: str) -> datetime:
        """Parse ANA datetime strings (various formats)."""
        for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%d/%m/%Y %H:%M:%S"):
            try:
                return datetime.strptime(text, fmt).replace(tzinfo=UTC)
            except ValueError:
                continue
        raise ValueError(f"Unrecognized datetime format: {text!r}")

    @staticmethod
    def _xml_text(element: ET.Element, tag: str) -> str | None:
        """Get text content of a child element, or None if absent/empty."""
        child = element.find(tag)
        if child is not None and child.text:
            return child.text.strip()
        return None

    @staticmethod
    def _xml_float(element: ET.Element, tag: str) -> float | None:
        """Get float value of a child element, or None if absent/empty/non-numeric."""
        child = element.find(tag)
        if child is not None and child.text:
            text = child.text.strip()
            if text:
                try:
                    return float(text)
                except ValueError:
                    return None
        return None
