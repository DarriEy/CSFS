# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Germany Berlin connector -- Wasserportal Berlin (Land Berlin, SenMVKU).

Wasserportal Berlin publishes surface-water time series for ~228 stations in
Berlin (plus a few upstream Spree gauges in Brandenburg/Saxony). This is a
multi-variable connector: it emits discharge, stage and water temperature.

Endpoints (verified live 2026-07-13):

* Station catalogue (HTML table, one row per station)::

      /start.php?anzeige=tabelle_ow&messanzeige=ms_all

  Columns: Messstellennummer, Messstellenname, Betreiber, Ausprägung,
  Gewässer, Fluss-km, Status, Rechtswert, Hochwert, Projektion. The
  ``Projektion`` column takes three values:

  - ``UTM 33N``  -- ETRS89 / UTM zone 33N (the vast majority of rows);
  - ``GK4``      -- DHDN / Gauss-Krüger zone 4 (8 upstream Spree gauges,
    recognisable by eastings > 4,000,000). Other scrapers have historically
    mistaken these for "10x too large" UTM eastings and divided by 10, which
    puts those stations ~15 km off; this connector converts them properly.
  - ``WGS 84``   -- already lon/lat (bathing-water sampling sites only).

* Time-series CSV download::

      /station.php?anzeige=d&station=<id>&thema=<t>&sreihe=<s>&smode=c
                  &sdatum=<DD.MM.YYYY>

  ``thema`` selects the variable, ``sreihe`` the series type:

  ========================  =======  ========  =========================
  variable                  thema    sreihe    CSV value column
  ========================  =======  ========  =========================
  stage (cm)                ows      tw / ew   Tagesmittelwert/Einzelwert
  discharge (m³/s)          odf      tw / ew   Tagesmittelwert/Einzelwert
  water temperature (°C)    owt      tw / ew   Tagesmittelwert/Einzelwert
  ========================  =======  ========  =========================

  ``tw`` = Tageswerte (daily aggregates; the ``owt`` daily CSV also carries
  Tagesminimum/-maximum columns), ``ew`` = Einzelwerte (15-minute
  instantaneous values). ``sdatum`` is the start date; the server returns
  from there to the present (an ``edatum`` parameter is accepted but ignored),
  so the requested window is sliced client-side.

Format quirks (all verified live):

* CSV is semicolon-delimited with German decimal commas; missing values use
  the sentinel ``-777`` (declared in the header as ``"Fehlwerte: -777"``).
* Responses are Latin-1/-9 bytes. The CSV Content-Type even *claims*
  ``charset=utf-8`` while shipping Latin-1 umlauts, so bytes are decoded
  manually (strict UTF-8 first, Latin-1 fallback) and the header is ignored.
* Timestamps are Europe/Berlin local time (CET/CEST); converted to UTC.
* Stage is served in cm and converted to the canonical metre.
* Unknown stations / themas answer HTTP 200 with an HTML or PHP-notice page
  instead of CSV; those are detected and treated as "no data".

References
----------
- Portal: https://wasserportal.berlin.de/
- Download docs: https://wasserportal.berlin.de/download/
- Terms: https://daten.berlin.de/impressum
"""

from __future__ import annotations

import csv
import html as _html
import io
import math
import re
from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

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

# Wasserportal timestamps are German local time (CET/CEST); convert to UTC.
_BERLIN_TZ = ZoneInfo("Europe/Berlin")

_CATALOGUE_PATH = "/start.php"
_CATALOGUE_PARAMS = {"anzeige": "tabelle_ow", "messanzeige": "ms_all"}
_DATA_PATH = "/station.php"

#: thema codes selecting the variable served by /station.php.
_THEMA: dict[Variable, str] = {
    Variable.STAGE: "ows",
    Variable.DISCHARGE: "odf",
    Variable.WATER_TEMPERATURE: "owt",
}
_SREIHE_DAILY = "tw"  # Tageswerte: daily aggregates (Tagesmittelwert column)
_SREIHE_INSTANT = "ew"  # Einzelwerte: 15-minute instantaneous values

#: Missing-value sentinel, declared in every CSV header as "Fehlwerte: -777".
_MISSING_SENTINEL = -777.0

#: Catalogue "Ausprägung" categories with no continuous time series behind
#: them (grab-sample sites); they are skipped when building the catalogue.
_EXCLUDED_KINDS = {"Badegewässerqualität", "Probenahme"}

# Prefer 15-min instantaneous series only for short, recent windows; anything
# longer/older is served as daily means to keep payloads and row counts sane.
_INSTANT_MAX_SPAN = timedelta(days=3)
_INSTANT_MAX_AGE = timedelta(days=7)

_TR_RE = re.compile(r"<tr[^>]*>(.*?)</tr>", re.S)
_TD_RE = re.compile(r"<td[^>]*>(.*?)</td>", re.S)
_TAG_RE = re.compile(r"<[^>]+>")


@register("germany_berlin")
class GermanyBerlinConnector(BaseConnector):
    """Connector for Wasserportal Berlin (multi-variable CSV downloads)."""

    slug = "germany_berlin"
    display_name = "Wasserportal Berlin (Germany)"
    base_url = "https://wasserportal.berlin.de"
    country_codes = ["DE"]
    supported_variables = ("discharge", "stage", "water_temperature")
    # Single shared state-run host; keep concurrency modest.
    max_concurrent_requests = 4

    async def fetch_stations(self) -> list[Station]:
        """Return the surface-water station catalogue (time-series sites)."""
        resp = await self._get(_CATALOGUE_PATH, params=_CATALOGUE_PARAMS)
        stations = self._parse_stations(_decode(resp.content))
        logger.info("stations_fetched", provider=self.slug, count=len(stations))
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch stage, discharge and water temperature for ``[start, end]``.

        Request plan (bounded at 3 requests per fetch): stage and discharge
        are requested as 15-minute Einzelwerte when the window is short
        (<= 3 days) and recent (ends within 7 days of now), otherwise as
        daily means; water temperature is always requested as a daily mean.
        The portal returns data from ``sdatum`` to the present, so rows are
        sliced to the requested window client-side. Stations lacking a thema
        answer with an HTML notice page, which parses to zero observations.
        """
        native_id = station_id.removeprefix(f"{self.slug}:")

        now = datetime.now(UTC)
        use_instant = (end - start) <= _INSTANT_MAX_SPAN and (now - end) <= _INSTANT_MAX_AGE
        level_flow_sreihe = _SREIHE_INSTANT if use_instant else _SREIHE_DAILY
        plan = [
            (Variable.STAGE, level_flow_sreihe),
            (Variable.DISCHARGE, level_flow_sreihe),
            (Variable.WATER_TEMPERATURE, _SREIHE_DAILY),
        ]

        # sdatum is a Berlin-local calendar date; use start's local day so the
        # first (partial) day of the window is included.
        sdatum = start.astimezone(_BERLIN_TZ).strftime("%d.%m.%Y")

        observations: list[Observation] = []
        errors: list[str] = []
        for variable, sreihe in plan:
            params = {
                "anzeige": "d",
                "station": native_id,
                "thema": _THEMA[variable],
                "sreihe": sreihe,
                "smode": "c",
                "sdatum": sdatum,
            }
            try:
                resp = await self._get(_DATA_PATH, params=params)
            except Exception as exc:  # noqa: BLE001 - collected, raised below if total
                logger.warning(
                    "thema_fetch_failed",
                    provider=self.slug,
                    station=native_id,
                    thema=_THEMA[variable],
                    sreihe=sreihe,
                    error=str(exc)[:120],
                )
                errors.append(f"{_THEMA[variable]}/{sreihe}: {exc}")
                continue
            observations.extend(
                self._parse_csv(resp.content, station_id, variable, start, end)
            )

        if errors and len(errors) == len(plan):
            raise ConnectorError(
                self.slug,
                f"all data requests failed for {native_id}: " + "; ".join(errors),
            )

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_stations(self, page: str) -> list[Station]:
        """Parse the catalogue HTML table into Station objects."""
        stations: list[Station] = []
        seen: set[str] = set()
        for row_html in _TR_RE.findall(page):
            cells = [_clean_cell(td) for td in _TD_RE.findall(row_html)]
            if len(cells) < 10:
                continue  # header row (th cells) or malformed row
            (native_id, name, _operator, kind, river, _km, status,
             easting, northing, projection) = cells[:10]
            if not native_id or native_id in seen or kind in _EXCLUDED_KINDS:
                continue

            coords = _to_wgs84(easting, northing, projection)
            if coords is None:
                logger.warning(
                    "station_coords_unparsed",
                    provider=self.slug,
                    station=native_id,
                    projection=projection,
                )
                continue
            lat, lon = coords

            seen.add(native_id)
            stations.append(
                Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=name or native_id,
                    latitude=lat,
                    longitude=lon,
                    country_code="DE",
                    river=river or None,
                    is_active=status.lower() == "aktiv",
                )
            )
        return stations

    def _parse_csv(
        self,
        content: bytes,
        station_id: str,
        variable: Variable,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse a Wasserportal CSV download into Observations.

        The value column is located by header name (``Tagesmittelwert`` for
        daily series -- the owt daily CSV also carries min/max columns which
        are ignored -- or ``Einzelwert`` for 15-minute series), which also
        determines the emitted resolution.
        """
        text = _decode(content)
        stripped = text.lstrip()
        if not stripped.startswith("Datum"):
            # Unknown station / thema not served here: the portal answers
            # HTTP 200 with an HTML or PHP-notice page instead of CSV.
            return []

        reader = csv.reader(io.StringIO(text), delimiter=";")
        header = next(reader, [])
        value_idx: int | None = None
        resolution = Resolution.UNKNOWN
        for idx, col in enumerate(header):
            name = col.strip()
            if name == "Tagesmittelwert":
                value_idx, resolution = idx, Resolution.DAILY_MEAN
                break
            if name == "Einzelwert":
                value_idx, resolution = idx, Resolution.INSTANTANEOUS
                break
        if value_idx is None:
            logger.warning(
                "unrecognised_csv_header",
                provider=self.slug,
                station=station_id,
                header=header[:6],
            )
            return []

        observations: list[Observation] = []
        for cells in reader:
            if len(cells) <= value_idx:
                continue
            ts = _parse_timestamp(cells[0].strip())
            if ts is None or not (start <= ts <= end):
                continue

            try:
                value = float(cells[value_idx].strip().replace(",", "."))
            except ValueError:
                continue

            if value == _MISSING_SENTINEL:
                observations.append(
                    Observation(
                        station_id=station_id,
                        timestamp=ts,
                        variable=variable,
                        resolution=resolution,
                        value=None,
                        quality=QualityFlag.MISSING,
                    )
                )
                continue

            if variable is Variable.STAGE:
                value /= 100.0  # portal serves Wasserstand in cm; store metres

            observations.append(
                Observation(
                    station_id=station_id,
                    timestamp=ts,
                    variable=variable,
                    resolution=resolution,
                    value=value,
                    quality=QualityFlag.RAW,
                )
            )
        return observations


# ----------------------------------------------------------------------
# Text helpers
# ----------------------------------------------------------------------


def _decode(content: bytes) -> str:
    """Decode a Wasserportal response body.

    The portal ships Latin-1/-9 bytes; CSV downloads even declare
    ``charset=utf-8`` while containing Latin-1 umlauts, so the Content-Type
    header cannot be trusted. Strict UTF-8 is tried first (pure-ASCII and
    genuinely-UTF-8 bodies pass), falling back to Latin-1.
    """
    try:
        return content.decode("utf-8")
    except UnicodeDecodeError:
        return content.decode("latin-1")


def _clean_cell(td_html: str) -> str:
    """Strip tags/entities from a <td> body and collapse whitespace."""
    text = _html.unescape(_TAG_RE.sub(" ", td_html))
    return re.sub(r"\s+", " ", text).strip()


def _parse_timestamp(raw: str) -> datetime | None:
    """Parse 'DD.MM.YYYY' / 'DD.MM.YYYY HH:MM' Berlin local time to UTC.

    Daily values carry a bare date and are stamped at local midnight of
    that day (converted to UTC).
    """
    fmt = "%d.%m.%Y %H:%M" if " " in raw else "%d.%m.%Y"
    try:
        naive = datetime.strptime(raw, fmt)
    except ValueError:
        return None
    return naive.replace(tzinfo=_BERLIN_TZ).astimezone(UTC)


# ----------------------------------------------------------------------
# Coordinate conversion (self-contained; no pyproj)
# ----------------------------------------------------------------------
# The catalogue mixes three CRS (see module docstring). The inverse
# transverse-Mercator below uses the classic USGS series (Snyder 1987,
# eqs. 8-17..8-25), accurate to well under 1 m within a UTM/GK zone --
# ample for gauge coordinates. GK4 additionally needs the DHDN->WGS84
# datum shift (EPSG:1777 Helmert parameters, ~1 m over Germany).

_WGS84_A = 6378137.0
_WGS84_F = 1.0 / 298.257223563
_BESSEL_A = 6377397.155
_BESSEL_F = 1.0 / 299.1528128


def _inverse_tm(
    easting: float,
    northing: float,
    a: float,
    f: float,
    lon0_deg: float,
    k0: float,
    false_easting: float,
) -> tuple[float, float]:
    """Inverse transverse Mercator (USGS/Snyder series) -> (lat, lon) degrees."""
    e2 = f * (2.0 - f)
    ep2 = e2 / (1.0 - e2)
    e1 = (1.0 - math.sqrt(1.0 - e2)) / (1.0 + math.sqrt(1.0 - e2))

    m = northing / k0
    mu = m / (a * (1 - e2 / 4 - 3 * e2**2 / 64 - 5 * e2**3 / 256))
    phi1 = (
        mu
        + (3 * e1 / 2 - 27 * e1**3 / 32) * math.sin(2 * mu)
        + (21 * e1**2 / 16 - 55 * e1**4 / 32) * math.sin(4 * mu)
        + (151 * e1**3 / 96) * math.sin(6 * mu)
        + (1097 * e1**4 / 512) * math.sin(8 * mu)
    )
    sin1, cos1, tan1 = math.sin(phi1), math.cos(phi1), math.tan(phi1)
    c1 = ep2 * cos1**2
    t1 = tan1**2
    n1 = a / math.sqrt(1 - e2 * sin1**2)
    r1 = a * (1 - e2) / (1 - e2 * sin1**2) ** 1.5
    d = (easting - false_easting) / (n1 * k0)

    lat = phi1 - (n1 * tan1 / r1) * (
        d**2 / 2
        - (5 + 3 * t1 + 10 * c1 - 4 * c1**2 - 9 * ep2) * d**4 / 24
        + (61 + 90 * t1 + 298 * c1 + 45 * t1**2 - 252 * ep2 - 3 * c1**2) * d**6 / 720
    )
    lon = math.radians(lon0_deg) + (
        d
        - (1 + 2 * t1 + c1) * d**3 / 6
        + (5 - 2 * c1 + 28 * t1 - 3 * c1**2 + 8 * ep2 + 24 * t1**2) * d**5 / 120
    ) / cos1
    return math.degrees(lat), math.degrees(lon)


def _utm33n_to_wgs84(easting: float, northing: float) -> tuple[float, float]:
    """UTM zone 33N (northern hemisphere) -> WGS84 (lat, lon) degrees.

    ETRS89/UTM (EPSG:25833) and WGS84/UTM (EPSG:32633) differ by <1 m in
    Germany, far below gauge-location accuracy, so one function serves both.
    """
    return _inverse_tm(easting, northing, _WGS84_A, _WGS84_F, 15.0, 0.9996, 500_000.0)


def _gk4_to_wgs84(easting: float, northing: float) -> tuple[float, float]:
    """DHDN Gauss-Krüger zone 4 (EPSG:31468) -> WGS84 (lat, lon) degrees.

    Inverse TM on the Bessel 1841 ellipsoid (central meridian 12 degE, k0=1,
    false easting 4,500,000 incl. the zone digit), then the EPSG:1777
    position-vector Helmert shift DHDN -> WGS84 (~1 m over Germany).
    """
    lat_d, lon_d = _inverse_tm(easting, northing, _BESSEL_A, _BESSEL_F, 12.0, 1.0, 4_500_000.0)

    # Geodetic (Bessel) -> ECEF.
    e2 = _BESSEL_F * (2 - _BESSEL_F)
    la, lo = math.radians(lat_d), math.radians(lon_d)
    n = _BESSEL_A / math.sqrt(1 - e2 * math.sin(la) ** 2)
    x = n * math.cos(la) * math.cos(lo)
    y = n * math.cos(la) * math.sin(lo)
    z = n * (1 - e2) * math.sin(la)

    # Helmert DHDN -> WGS84 (EPSG:1777): tx,ty,tz [m]; rx,ry,rz ["]; ds [ppm].
    tx, ty, tz = 598.1, 73.7, 418.2
    rx, ry, rz = (math.radians(v / 3600.0) for v in (0.202, 0.045, -2.455))
    s = 1.0 + 6.7e-6
    x2 = tx + s * (x - rz * y + ry * z)
    y2 = ty + s * (rz * x + y - rx * z)
    z2 = tz + s * (-ry * x + rx * y + z)

    # ECEF -> geodetic (WGS84), fixed-point iteration on latitude.
    e2w = _WGS84_F * (2 - _WGS84_F)
    lon = math.atan2(y2, x2)
    p = math.hypot(x2, y2)
    lat = math.atan2(z2, p * (1 - e2w))
    for _ in range(5):
        nw = _WGS84_A / math.sqrt(1 - e2w * math.sin(lat) ** 2)
        lat = math.atan2(z2 + e2w * nw * math.sin(lat), p)
    return math.degrees(lat), math.degrees(lon)


def _to_wgs84(easting: str, northing: str, projection: str) -> tuple[float, float] | None:
    """Convert a catalogue coordinate pair to (lat, lon) per its Projektion.

    ``WGS 84`` rows already hold lon (Rechtswert) / lat (Hochwert) degrees.
    A defensive legacy quirk is kept for UTM rows: eastings recorded 10x too
    large (> 1,000,000, an old portal data-entry artefact) are divided by 10.
    """
    try:
        east, north = float(easting), float(northing)
    except ValueError:
        return None

    proj = projection.strip().upper()
    if proj.startswith("WGS"):
        lat, lon = north, east
    elif proj.startswith("UTM"):
        if east > 1_000_000:
            east /= 10.0
        lat, lon = _utm33n_to_wgs84(east, north)
    elif proj.startswith("GK4"):
        lat, lon = _gk4_to_wgs84(east, north)
    else:
        return None

    if not (-90.0 <= lat <= 90.0 and -180.0 <= lon <= 180.0):
        return None
    return lat, lon
