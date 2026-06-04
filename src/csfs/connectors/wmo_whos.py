"""WMO WHOS connector -- WMO Hydrological Observing System (GEO DAB).

WHOS is a federated broker (run by ESSI-Lab / CNR-IIA on the GEO DAB
infrastructure) that harmonises hydrological data published by national
agencies worldwide. It is queried through two REST APIs hosted under
``whos.geodab.eu/gs-service/services/essi/token/<token>/view/<view>/``:

* ``om-api/features``       -- monitoring-point (station) discovery. Returns
  GeoJSON-ish records with ``id`` (an opaque feature hash), ``name`` and
  ``shape.coordinates``. Supports semantic filtering by observed property
  (``observedProperty=Discharge&ontology=whos``) and ``country`` (ISO3).
* ``timeseries-api/timeseries`` -- time series + data. Queried by
  ``monitoringPoint=<feature id>`` it returns OM-JSON ``member`` series, each
  with ``observedProperty``, ``result.defaultPointMetadata.uom`` and
  ``result.points[].{time.instant,value}``.

Notes
-----
* The ``timeseries-api/monitoring-points`` endpoint exists but its JSON
  serialiser is broken server-side (HTTP 500 "Exception writing response"
  whenever it would return a record), so we use ``om-api/features`` instead.
* WHOS requires a token. The public WHOS portal ships an anonymous token in
  its ``config.json``; we use it by default. It can be overridden via config.
* Discharge is reported in m3/s. We keep only series whose ``uom`` is cubic
  metre per second and de-duplicate the provider-native vs. canonical copies
  of the same series.

References
----------
- Timeseries API: https://whos.geodab.eu/gs-service/timeseries-api/
- OM API doc:     https://whos.geodab.eu/gs-service/om-api/whos.html
- Portal:         https://whos.geodab.eu/gs-service/whos/search.html
"""

from __future__ import annotations

from datetime import UTC, datetime
from typing import Any

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# Anonymous token shipped in the public WHOS portal config.json. Read-only,
# no registration required. Override via config["token"] / config["api_key"].
_DEFAULT_TOKEN = "whos-d40a452b-b865-4fbe-8165-43a96ebf1b3d"
_DEFAULT_VIEW = "whos"

# WHOS reports discharge in m3/s under this unit-of-measure label.
_DISCHARGE_UOM = {"cubic metre per second", "m3/s", "m^3/s", "m3 s-1"}

# ISO3 countries whose providers expose discharge via the default ``whos`` view.
# Bounds the (otherwise ~20k-station) global view to a tractable subset.
# Override with config["countries"] (list[str] of ISO3 codes).
_DEFAULT_COUNTRIES = ["BRA", "ARG"]

# Hard cap on stations fetched per country (paging guard).
_DEFAULT_LIMIT_PER_COUNTRY = 500
_PAGE_SIZE = 250


@register("wmo_whos")
class WHOSConnector(BaseConnector):
    """Connector for WMO WHOS (brokered by ESSI-Lab / GEO DAB).

    Configuration options (all optional):
        token / api_key : str
            Access token. Defaults to the public portal token.
        view : str
            WHOS view to query. Defaults to ``"whos"`` (global).
        countries : list[str]
            ISO3 country codes to enumerate stations for. Defaults to a small
            set of countries known to publish discharge through the broker.
        limit_per_country : int
            Maximum stations to keep per country.
    """

    slug = "wmo_whos"
    display_name = "WMO WHOS (Hydrological Observing System)"
    base_url = "https://whos.geodab.eu/gs-service/services/essi"
    country_codes: list[str] = ["global"]
    # WHOS proxies to remote providers; keep request pressure modest.
    max_concurrent_requests = 4

    @property
    def _token(self) -> str:
        return self.config.get("token") or self.config.get("api_key") or _DEFAULT_TOKEN

    @property
    def _view(self) -> str:
        return self.config.get("view") or _DEFAULT_VIEW

    def _api_root(self, api: str) -> str:
        """Build the per-view/token API root, e.g. ``.../om-api`` ."""
        return f"{self.base_url}/token/{self._token}/view/{self._view}/{api}"

    @property
    def _countries(self) -> list[str]:
        c = self.config.get("countries")
        if isinstance(c, str):
            return [c]
        if c:
            return list(c)
        return list(_DEFAULT_COUNTRIES)

    @property
    def _limit_per_country(self) -> int:
        return int(self.config.get("limit_per_country", _DEFAULT_LIMIT_PER_COUNTRY))

    # ------------------------------------------------------------------
    # Stations
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """List discharge monitoring points for the configured countries.

        Uses ``om-api/features`` with the semantic discharge filter so only
        stations that publish a discharge series are returned. Paged and
        bounded per country to avoid pulling the whole global view.
        """
        stations: list[Station] = []
        seen: set[str] = set()
        for country in self._countries:
            stations.extend(await self._fetch_country_stations(country, seen))
        logger.info("stations_fetched", provider=self.slug, count=len(stations))
        return stations

    async def _fetch_country_stations(
        self, country: str, seen: set[str]
    ) -> list[Station]:
        out: list[Station] = []
        resumption_token: str | None = None
        while len(out) < self._limit_per_country:
            page = min(_PAGE_SIZE, self._limit_per_country - len(out))
            params: dict[str, Any] = {
                "observedProperty": "Discharge",
                "ontology": "whos",
                "country": country,
                "limit": page,
            }
            if resumption_token:
                params["resumptionToken"] = resumption_token
            try:
                resp = await self._get(f"{self._api_root('om-api')}/features", params=params)
                data = resp.json()
            except Exception as exc:
                raise ConnectorError(
                    self.slug, f"Failed to fetch WHOS features ({country}): {exc}"
                ) from exc

            results = data.get("results") or []
            for feat in results:
                station = self._parse_feature(feat, country)
                if station is None or station.native_id in seen:
                    continue
                seen.add(station.native_id)
                out.append(station)

            # ``completed`` true, a short page, or a missing/repeated token
            # means we have exhausted matches.
            next_token = data.get("resumptionToken")
            if (
                data.get("completed")
                or len(results) < page
                or not next_token
                or next_token == resumption_token
            ):
                break
            resumption_token = next_token
        return out

    def _parse_feature(self, feat: dict[str, Any], country: str) -> Station | None:
        native_id = feat.get("id")
        if not native_id:
            return None

        shape = feat.get("shape") or {}
        coords = shape.get("coordinates") or []
        if len(coords) < 2:
            return None
        lon, lat = float(coords[0]), float(coords[1])

        params = {
            p.get("name"): p.get("value")
            for p in feat.get("parameter", [])
            if isinstance(p, dict)
        }

        return Station(
            id=self._station_id(native_id),
            provider=self.slug,
            native_id=native_id,
            name=feat.get("name") or params.get("identifier") or native_id,
            latitude=lat,
            longitude=lon,
            country_code=self._iso2(params.get("country")) or country,
        )

    @staticmethod
    def _iso2(country_name: str | None) -> str | None:
        """Best-effort country label normalisation (kept short, non-fatal)."""
        if not country_name:
            return None
        # WHOS feature parameters carry a human country name (e.g. "Brazil"),
        # not an ISO code; truncate to a stable short token.
        return country_name.strip()[:64] or None

    # ------------------------------------------------------------------
    # Observations
    # ------------------------------------------------------------------

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch the discharge (m3/s) time series for one monitoring point."""
        native_id = station_id.removeprefix(f"{self.slug}:")

        params = {
            "monitoringPoint": native_id,
            "beginPosition": _fmt(start),
            "endPosition": _fmt(end),
        }
        try:
            resp = await self._get(
                f"{self._api_root('timeseries-api')}/timeseries", params=params
            )
            data = resp.json()
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch WHOS observations for {native_id}: {exc}",
            ) from exc

        observations = self._parse_timeseries(data, station_id)
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    def _parse_timeseries(
        self, data: dict[str, Any], station_id: str
    ) -> list[Observation]:
        """Parse OM-JSON members, keeping only discharge (m3/s) series.

        WHOS returns both the provider-native series and a canonical copy of
        the same data, so we de-duplicate on (timestamp) and keep the first
        non-null value seen.
        """
        members = data.get("member") if isinstance(data, dict) else None
        if not isinstance(members, list):
            return []

        by_ts: dict[datetime, float | None] = {}
        for member in members:
            if not isinstance(member, dict):
                continue
            result = member.get("result") or {}
            uom = (result.get("defaultPointMetadata") or {}).get("uom") or ""
            if uom.strip().lower() not in _DISCHARGE_UOM:
                continue

            for point in result.get("points") or []:
                ts = _parse_instant(point.get("time"))
                if ts is None:
                    continue
                value = _coerce_value(point.get("value"))
                # Prefer a real value over a previously stored missing one.
                if ts not in by_ts or (by_ts[ts] is None and value is not None):
                    by_ts[ts] = value

        observations: list[Observation] = []
        for ts in sorted(by_ts):
            value = by_ts[ts]
            observations.append(
                Observation(
                    station_id=station_id,
                    timestamp=ts,
                    discharge_m3s=value,
                    quality=QualityFlag.RAW if value is not None else QualityFlag.MISSING,
                )
            )
        return observations


@register("wmo_whos_plata")
class WHOSPlataConnector(WHOSConnector):
    """WHOS view scoped to the La Plata River basin (``whos-plata``).

    Verified to serve discharge in m3/s via the public portal token.
    """

    slug = "wmo_whos_plata"
    display_name = "WMO WHOS-Plata (South America)"
    # ISO 3166-1 alpha-2 for the La Plata basin countries.
    country_codes = ["AR", "BO", "BR", "PY", "UY"]

    @property
    def _view(self) -> str:
        return self.config.get("view") or "whos-plata"

    @property
    def _countries(self) -> list[str]:
        # The whos-plata view is already basin-scoped; list its discharge
        # stations directly. ISO3 codes are still accepted as a filter.
        c = self.config.get("countries")
        if isinstance(c, str):
            return [c]
        if c:
            return list(c)
        return ["ARG", "BOL", "BRA", "PRY", "URY"]


@register("wmo_whos_africa")
class WHOSAfricaConnector(WHOSConnector):
    """WHOS view scoped to WMO RA1 / HydroSOS Africa (``whos-ra1``).

    Note: the ``whos-ra1`` view may require a dedicated token; the public
    portal token does not authorise it, in which case this connector degrades
    (raises ConnectorError) rather than returning data.
    """

    slug = "wmo_whos_africa"
    display_name = "WMO WHOS-Africa (HydroSOS)"
    country_codes = ["global"]

    @property
    def _view(self) -> str:
        return self.config.get("view") or "whos-ra1"


def _fmt(dt: datetime) -> str:
    """Format a datetime as the ISO-8601 instant WHOS expects (UTC, Z)."""
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC).strftime("%Y-%m-%dT%H:%M:%SZ")


def _parse_instant(time_obj: Any) -> datetime | None:
    """Extract a UTC datetime from a WHOS point ``time`` object.

    Accepts ``{"instant": "...Z"}`` and bare ISO strings.
    """
    time_str = time_obj.get("instant") or time_obj.get("value") if isinstance(time_obj, dict) else time_obj
    if not isinstance(time_str, str) or not time_str:
        return None
    try:
        ts = datetime.fromisoformat(time_str.replace("Z", "+00:00"))
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=UTC)
    return ts.astimezone(UTC)


def _coerce_value(value: Any) -> float | None:
    """Coerce a point value to float, mapping WHOS no-data sentinels to None."""
    if value is None:
        return None
    try:
        f = float(value)
    except (TypeError, ValueError):
        return None
    # -9999 is the conventional WHOS / provider no-data marker.
    if f <= -9990.0:
        return None
    return f
