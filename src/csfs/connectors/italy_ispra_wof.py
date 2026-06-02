# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Italy connector — ISPRA HIS-Central (WaterOneFlow / WaterML 1.1 SOAP).

Status: RESEARCH / token-gated (not openly fixable)
--------------------------------------------------
ISPRA HIS-Central used to expose a national CUAHSI WaterOneFlow SOAP
service at::

    http://hiscentral.isprambiente.gov.it/hiscentral/webservices/cuahsi_1_1.asmx

That host is now **dead** (the ``hiscentral`` sub-domain no longer has a
DNS record — NXDOMAIN). The portal was migrated onto the WMO WHOS /
GeoDAB broker infrastructure, where a live WaterOneFlow 1.1 endpoint
does exist::

    https://www.hiscentral.isprambiente.gov.it/gs-service/services/essi/view/whos/cuahsi_1_1.asmx

The WSDL is publicly readable (GetSites / GetSiteInfo / GetValues /
GetVariables, CUAHSI namespace ``http://www.cuahsi.org/his/1.1/ws/``),
**but every data-returning POST is rejected with HTTP 403** unless a
valid WHOS/GeoDAB ``authToken`` is supplied. That token is granted by
the WMO WHOS team out-of-band (registration / approval) and cannot be
acquired programmatically, so out of the box this connector returns
gracefully empty.

This connector is implemented as a *real* WaterOneFlow client. If a
caller supplies a token via ``config["whos_token"]`` it will issue real
GetSites / GetValues SOAP calls and parse WaterML 1.1 discharge
(``m3/s``). Without a token it logs a hint and returns empty — it never
fabricates a seed station.

References
----------
- Portal: https://www.hiscentral.isprambiente.gov.it/gs-service/hisc/search.html
- WaterOneFlow WSDL (token-gated POST):
  https://www.hiscentral.isprambiente.gov.it/gs-service/services/essi/view/whos/cuahsi_1_1.asmx?WSDL
"""

from __future__ import annotations

import xml.etree.ElementTree as ET
from datetime import UTC, datetime, timedelta

import httpx
import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import (
    Observation,
    QualityFlag,
    Station,
    TimeSeriesChunk,
)
from csfs.core.registry import register

logger = structlog.get_logger()

# Live WaterOneFlow 1.1 endpoint on the GeoDAB/WHOS broker. POST is
# token-gated (403) — only usable when config["whos_token"] is set.
_WOF_URL = (
    "https://www.hiscentral.isprambiente.gov.it"
    "/gs-service/services/essi/view/whos/cuahsi_1_1.asmx"
)
_HIS_NS = "http://www.cuahsi.org/his/1.1/ws/"
_WML_NS = "http://www.cuahsi.org/waterML/1.1/"

# Variable codes / names that denote river discharge in m3/s.
_DISCHARGE_HINTS = ("discharge", "portata", "streamflow", "river discharge")


@register("italy_ispra_wof")
class ISPRAWOFConnector(BaseConnector):
    """Connector for ISPRA HIS-Central (WaterOneFlow / WaterML 1.1 SOAP).

    Configuration options (via ``config`` dict):
        whos_token : str
            WHOS / GeoDAB ``authToken`` granted by the WMO WHOS team.
            Required for any data — without it the broker returns 403
            and this connector yields empty results.
        discharge_variable : str
            Override the WaterOneFlow variable code passed to GetValues
            (default tries common discharge codes).
    """

    slug = "italy_ispra_wof"
    display_name = "ISPRA HIS-Central (Italy)"
    base_url = "https://www.hiscentral.isprambiente.gov.it"
    country_codes = ["IT"]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return discharge stations via WaterOneFlow GetSites.

        Returns an empty list when no WHOS token is configured or the
        broker rejects the request (the default state — HIS-Central's
        WaterOneFlow is token-gated).
        """
        token = self._token()
        if not token:
            logger.info(
                "ispra_wof_no_token",
                provider=self.slug,
                hint=(
                    "ISPRA HIS-Central WaterOneFlow is gated behind a "
                    "WMO WHOS authToken (HTTP 403 without it). Set "
                    "config['whos_token'] to enable real fetches."
                ),
            )
            return []

        xml_text = await self._soap_call(
            "GetSites",
            f"<site/><authToken>{token}</authToken>",
        )
        if xml_text is None:
            return []
        return self._parse_sites(xml_text)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations via WaterOneFlow GetValues.

        Returns an empty chunk when no token is configured or the broker
        rejects the request.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")
        token = self._token()
        if not token:
            return self._empty_chunk(station_id)

        variable = self.config.get("discharge_variable", "")
        body = (
            f"<location>{native_id}</location>"
            f"<variable>{variable}</variable>"
            f"<startDate>{start.strftime('%Y-%m-%d')}</startDate>"
            f"<endDate>{end.strftime('%Y-%m-%d')}</endDate>"
            f"<authToken>{token}</authToken>"
        )
        xml_text = await self._soap_call("GetValues", body)
        if xml_text is None:
            return self._empty_chunk(station_id)
        return self._parse_values(xml_text, station_id)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent discharge observations (last 7 days)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(days=7),
            end=now,
        )

    # ------------------------------------------------------------------
    # SOAP transport
    # ------------------------------------------------------------------

    def _token(self) -> str:
        return str(self.config.get("whos_token", "") or "")

    async def _soap_call(self, operation: str, body_inner: str) -> str | None:
        """Issue a WaterOneFlow SOAP POST; return the response body text.

        Returns ``None`` (rather than raising) when the broker is
        unreachable or rejects the request (e.g. 403 without a token),
        so the connector degrades to empty instead of failing the cycle.
        """
        envelope = (
            '<?xml version="1.0" encoding="utf-8"?>'
            '<soap:Envelope '
            'xmlns:soap="http://schemas.xmlsoap.org/soap/envelope/">'
            "<soap:Body>"
            f'<{operation} xmlns="{_HIS_NS}">{body_inner}</{operation}>'
            "</soap:Body></soap:Envelope>"
        )
        headers = {
            "Content-Type": "text/xml; charset=utf-8",
            "SOAPAction": f"{_HIS_NS}{operation}",
        }
        try:
            resp = await self.client.post(
                _WOF_URL, content=envelope, headers=headers,
            )
        except (
            httpx.ConnectError,
            httpx.ReadTimeout,
            httpx.RemoteProtocolError,
        ) as exc:
            logger.warning(
                "ispra_wof_unreachable",
                provider=self.slug,
                operation=operation,
                error=str(exc)[:120],
            )
            return None

        if resp.status_code == 403:
            logger.info(
                "ispra_wof_forbidden",
                provider=self.slug,
                operation=operation,
                hint="WHOS authToken rejected or missing (HTTP 403).",
            )
            return None
        if resp.status_code != 200:
            logger.warning(
                "ispra_wof_http_error",
                provider=self.slug,
                operation=operation,
                status=resp.status_code,
            )
            return None
        return resp.text

    # ------------------------------------------------------------------
    # WaterML 1.1 parsing
    # ------------------------------------------------------------------

    def _parse_sites(self, xml_text: str) -> list[Station]:
        """Parse a WaterML 1.1 GetSites response into Station objects.

        Keeps only sites that advertise a discharge variable in m3/s.
        The GetSites response is wrapped in a SOAP envelope and the
        WaterML payload is XML-escaped inside ``GetSitesResult``.
        """
        payload = self._unwrap_result(xml_text, "GetSitesResponse")
        if payload is None:
            return []
        try:
            root = ET.fromstring(payload)
        except ET.ParseError as exc:
            raise ConnectorError(
                self.slug, f"Malformed WaterML sites: {exc}",
            ) from exc

        stations: list[Station] = []
        for site in root.iter(f"{{{_WML_NS}}}site"):
            info = site.find(f"{{{_WML_NS}}}siteInfo")
            if info is None:
                continue

            name = self._text(info, "siteName") or ""
            native_id = self._site_code(info)
            if not native_id:
                continue

            lat, lon = self._site_latlon(info)
            if lat is None or lon is None:
                continue

            # Only keep sites exposing a discharge series, when the
            # response lists series (GetSites may or may not include them).
            if not self._site_has_discharge(site):
                continue

            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=name or native_id,
                    latitude=lat,
                    longitude=lon,
                    country_code="IT",
                    river=None,
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

    def _parse_values(self, xml_text: str, station_id: str) -> TimeSeriesChunk:
        """Parse a WaterML 1.1 GetValues response into a TimeSeriesChunk.

        Only values whose variable unit is a discharge rate (m3/s) are
        kept and emitted as ``discharge_m3s``.
        """
        payload = self._unwrap_result(xml_text, "GetValuesResponse")
        observations: list[Observation] = []
        if payload is None:
            return TimeSeriesChunk(
                station_id=station_id,
                provider=self.slug,
                observations=observations,
                fetched_at=datetime.now(UTC),
            )
        try:
            root = ET.fromstring(payload)
        except ET.ParseError as exc:
            raise ConnectorError(
                self.slug, f"Malformed WaterML values: {exc}",
            ) from exc

        for ts_el in root.iter(f"{{{_WML_NS}}}timeSeries"):
            if not self._series_is_discharge(ts_el):
                continue
            for val in ts_el.iter(f"{{{_WML_NS}}}value"):
                ts = self._parse_dt(val.get("dateTime"))
                if ts is None:
                    continue
                discharge = self._to_float(val.text)
                observations.append(Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=discharge,
                    quality=(
                        QualityFlag.RAW
                        if discharge is not None
                        else QualityFlag.MISSING
                    ),
                ))

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # ------------------------------------------------------------------
    # WaterML helpers
    # ------------------------------------------------------------------

    def _unwrap_result(self, xml_text: str, response_tag: str) -> str | None:
        """Return the inner WaterML payload from a WaterOneFlow response.

        WaterOneFlow 1.1 ``*Object`` methods return raw WaterML, but the
        plain string methods wrap an XML-escaped WaterML document inside
        ``<...Result>``. ElementTree un-escapes the text content
        automatically, so we just return the element's text. Falls back
        to treating the whole document as WaterML if no wrapper is found.
        """
        try:
            root = ET.fromstring(xml_text)
        except ET.ParseError:
            return None

        # Look for any *Result element (namespace-agnostic local name).
        for el in root.iter():
            local = el.tag.rsplit("}", 1)[-1]
            if local.endswith("Result") and el.text and el.text.strip():
                return el.text
        # Already-unwrapped WaterML (e.g. *Object variants or mock).
        if root.tag.endswith("}timeSeriesResponse") or root.tag.endswith(
            "}sitesResponse"
        ) or _WML_NS in xml_text:
            return xml_text
        return None

    def _site_code(self, info: ET.Element) -> str:
        code = info.find(f"{{{_WML_NS}}}siteCode")
        if code is not None and code.text:
            return code.text.strip()
        return ""

    def _site_latlon(
        self, info: ET.Element,
    ) -> tuple[float | None, float | None]:
        geo = info.find(f"{{{_WML_NS}}}geoLocation")
        if geo is None:
            return None, None
        loc = geo.find(f"{{{_WML_NS}}}geogLocation")
        if loc is None:
            return None, None
        lat = self._to_float(self._text(loc, "latitude"))
        lon = self._to_float(self._text(loc, "longitude"))
        return lat, lon

    def _site_has_discharge(self, site: ET.Element) -> bool:
        """True if the site lists no series (unknown) or a discharge one."""
        series = list(site.iter(f"{{{_WML_NS}}}series"))
        variables = list(site.iter(f"{{{_WML_NS}}}variable"))
        if not series and not variables:
            # GetSites often omits series; don't exclude — GetValues will
            # filter on units anyway.
            return True
        return any(self._variable_is_discharge(v) for v in variables)

    def _series_is_discharge(self, ts_el: ET.Element) -> bool:
        for var in ts_el.iter(f"{{{_WML_NS}}}variable"):
            if self._variable_is_discharge(var):
                return True
        # If no variable metadata at all, accept (best effort).
        return ts_el.find(f"{{{_WML_NS}}}variable") is None

    def _variable_is_discharge(self, var: ET.Element) -> bool:
        name = (self._text(var, "variableName") or "").lower()
        code = (self._text(var, "variableCode") or "").lower()
        unit = self._unit_name(var).lower()
        if any(h in name or h in code for h in _DISCHARGE_HINTS):
            return True
        # Volumetric flow units: m3/s, m^3/s, cms, cfs->no (not m3/s).
        compact = unit.replace(" ", "").replace("³", "3").replace("^", "")
        return compact in ("m3/s", "m3s-1", "cumecs", "cms")

    def _unit_name(self, var: ET.Element) -> str:
        unit = var.find(f"{{{_WML_NS}}}unit")
        if unit is not None:
            txt = self._text(unit, "unitName") or self._text(
                unit, "unitAbbreviation",
            )
            if txt:
                return txt
        return self._text(var, "units") or ""

    @staticmethod
    def _text(parent: ET.Element, local: str) -> str | None:
        el = parent.find(f"{{{_WML_NS}}}{local}")
        if el is not None and el.text is not None:
            return el.text.strip()
        return None

    @staticmethod
    def _to_float(value: str | None) -> float | None:
        if value is None:
            return None
        value = value.strip()
        if not value:
            return None
        try:
            f = float(value)
        except ValueError:
            return None
        # WaterML no-data sentinels.
        if f in (-9999.0, -999999.0, -9999999.0):
            return None
        return f

    @staticmethod
    def _parse_dt(value: str | None) -> datetime | None:
        if not value:
            return None
        text = value.strip().replace("Z", "+00:00")
        try:
            dt = datetime.fromisoformat(text)
        except ValueError:
            for fmt in ("%Y-%m-%dT%H:%M:%S", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
                try:
                    dt = datetime.strptime(value.strip(), fmt)
                    break
                except ValueError:
                    continue
            else:
                return None
        if dt.tzinfo is None:
            dt = dt.replace(tzinfo=UTC)
        return dt

    def _empty_chunk(self, station_id: str) -> TimeSeriesChunk:
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )
