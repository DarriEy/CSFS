"""New Zealand regional council Hilltop API connector for hydrology data.

The Hilltop server software is used by many NZ regional councils to serve
real-time and historical hydrological data.  Each council runs its own
instance; this connector targets Environment Canterbury (ECan) as the
primary source but exposes configuration to point at other councils.

Known endpoints:
- Horizons (Manawatu-Whanganui): https://hilltopserver.horizons.govt.nz/boo.hts
- Environment Canterbury: https://data.ecan.govt.nz/data/hilltop.hts
- Waikato: https://monitoring.waikatoregion.govt.nz/data.hts
- Hawke's Bay: https://data.hbrc.govt.nz/Hilltop/Data.hts
"""

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

# Default measurement names to look for (in priority order)
_DISCHARGE_MEASUREMENTS = ("Flow", "Discharge", "Mean Flow", "Streamflow")


@register("newzealand_hilltop")
class NewZealandHilltopConnector(BaseConnector):
    slug = "newzealand_hilltop"
    display_name = "New Zealand Hilltop (Environment Canterbury)"
    base_url = "https://data.ecan.govt.nz/data/hilltop.hts"
    country_codes = ["NZ"]

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        # Allow overriding the base URL via config for other regional councils
        if config and "base_url" in config:
            self.base_url = config["base_url"]
        # Cache: site name -> preferred discharge measurement name
        self._site_measurement: dict[str, str] = {}

    async def __aenter__(self) -> NewZealandHilltopConnector:
        # Hilltop APIs use a single endpoint URL with query parameters,
        # so we set base_url to the parent path and store the script name.
        url_parts = self.base_url.rsplit("/", 1)
        base = url_parts[0] if len(url_parts) > 1 else self.base_url
        self._script = url_parts[1] if len(url_parts) > 1 else ""

        self._client = httpx.AsyncClient(
            base_url=base,
            timeout=httpx.Timeout(60.0, connect=10.0),
            headers={
                "User-Agent": "CSFS/0.1 (https://github.com/csfs)",
                "Accept": "text/xml",
            },
            follow_redirects=True,
        )
        return self

    async def fetch_stations(self) -> list[Station]:
        """Return all stations with lat/long from the Hilltop SiteList."""
        resp = await self._get(f"/{self._script}", params={
            "Service": "Hilltop",
            "Request": "SiteList",
            "Location": "LatLong",
        })
        return self._parse_site_list_xml(resp.text)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for a station over a time range."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        measurement = await self._resolve_measurement(native_id)

        time_interval = (
            f"{start.strftime('%Y-%m-%dT%H:%M:%S')}/"
            f"{end.strftime('%Y-%m-%dT%H:%M:%S')}"
        )

        resp = await self._get(f"/{self._script}", params={
            "Service": "Hilltop",
            "Request": "GetData",
            "Site": native_id,
            "Measurement": measurement,
            "TimeInterval": time_interval,
        })
        return self._parse_get_data_xml(resp.text, station_id)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent 24h of observations."""
        from datetime import timedelta

        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_site_list_xml(self, text: str) -> list[Station]:
        """Parse Hilltop SiteList XML into Station models.

        Expected XML structure::

            <HilltopServer>
              <Site Name="...">
                <Latitude>...</Latitude>
                <Longitude>...</Longitude>
              </Site>
              ...
            </HilltopServer>
        """
        try:
            root = ET.fromstring(text)
        except ET.ParseError as exc:
            raise DataFormatError(
                self.slug, f"Invalid XML in SiteList response: {exc}"
            ) from exc

        stations: list[Station] = []
        for site in root.iter("Site"):
            try:
                name = site.get("Name")
                if not name:
                    continue

                lat = self._xml_float(site, "Latitude")
                lon = self._xml_float(site, "Longitude")
                if lat is None or lon is None:
                    continue

                stations.append(Station(
                    id=self._station_id(name),
                    provider=self.slug,
                    native_id=name,
                    name=name,
                    latitude=lat,
                    longitude=lon,
                    country_code="NZ",
                ))
            except (ValueError, TypeError) as exc:
                logger.debug(
                    "skipping_station",
                    provider=self.slug,
                    site=name if name else "unknown",
                    error=str(exc),
                )
                continue

        return stations

    def _parse_measurement_list_xml(self, text: str) -> list[str]:
        """Parse Hilltop MeasurementList XML and return measurement names.

        Expected XML structure::

            <HilltopServer>
              <DataSource>
                <MeasurementName>Flow</MeasurementName>
                ...
              </DataSource>
              ...
            </HilltopServer>
        """
        try:
            root = ET.fromstring(text)
        except ET.ParseError as exc:
            raise DataFormatError(
                self.slug, f"Invalid XML in MeasurementList response: {exc}"
            ) from exc

        measurements: list[str] = []
        for ds in root.iter("DataSource"):
            mname = self._xml_text(ds, "MeasurementName")
            if mname:
                measurements.append(mname)
        return measurements

    def _parse_get_data_xml(self, text: str, station_id: str) -> TimeSeriesChunk:
        """Parse Hilltop GetData XML into a TimeSeriesChunk.

        Expected XML structure::

            <Hilltop>
              <Measurement SiteName="..." DataSourceName="...">
                <Data DateFormat="..." NumItems="...">
                  <E>
                    <T>2024-06-01T12:00:00</T>
                    <I1>123.4</I1>
                  </E>
                  ...
                </Data>
              </Measurement>
            </Hilltop>
        """
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
                self.slug, f"Invalid XML in GetData response: {exc}"
            ) from exc

        observations: list[Observation] = []
        for entry in root.iter("E"):
            try:
                time_text = self._xml_text(entry, "T")
                if not time_text:
                    continue

                timestamp = self._parse_hilltop_datetime(time_text)
                discharge = self._xml_float(entry, "I1")

                observations.append(Observation(
                    station_id=station_id,
                    timestamp=timestamp,
                    discharge_m3s=discharge,
                    quality=(
                        QualityFlag.RAW if discharge is not None
                        else QualityFlag.MISSING
                    ),
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

    async def _resolve_measurement(self, site_name: str) -> str:
        """Determine the discharge measurement name for a site.

        Checks the cache first, then queries the MeasurementList endpoint
        to find a discharge-related measurement name.
        """
        if site_name in self._site_measurement:
            return self._site_measurement[site_name]

        resp = await self._get(f"/{self._script}", params={
            "Service": "Hilltop",
            "Request": "MeasurementList",
            "Site": site_name,
        })
        measurements = self._parse_measurement_list_xml(resp.text)

        # Find the first discharge-related measurement
        for preferred in _DISCHARGE_MEASUREMENTS:
            for meas in measurements:
                if meas.lower() == preferred.lower():
                    self._site_measurement[site_name] = meas
                    return meas

        # Fall back to "Flow" if nothing matched
        default = "Flow"
        logger.warning(
            "no_discharge_measurement_found",
            provider=self.slug,
            site=site_name,
            available=measurements,
            fallback=default,
        )
        self._site_measurement[site_name] = default
        return default

    @staticmethod
    def _parse_hilltop_datetime(text: str) -> datetime:
        """Parse Hilltop datetime strings into timezone-aware datetimes.

        Hilltop typically uses ISO-like formats without timezone info
        (data is in NZST).  We store as UTC-aware for consistency.
        """
        for fmt in (
            "%Y-%m-%dT%H:%M:%S",
            "%Y-%m-%d %H:%M:%S",
            "%Y-%m-%dT%H:%M",
            "%Y-%m-%d %H:%M",
        ):
            try:
                return datetime.strptime(text, fmt).replace(tzinfo=UTC)
            except ValueError:
                continue
        raise ValueError(f"Unrecognized Hilltop datetime format: {text!r}")

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
