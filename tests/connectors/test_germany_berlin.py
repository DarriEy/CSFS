# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Tests for the Wasserportal Berlin (germany_berlin) connector.

Payloads are trimmed captures from the live portal (2026-07-13). Verified:
  * catalogue rows are parsed per their Projektion column: UTM 33N (with the
    legacy 10x-easting quirk), GK4 (Gauss-Krüger zone 4 on DHDN) and WGS 84;
    grab-sample-only categories are excluded;
  * the self-contained UTM 33N / GK4 -> WGS84 conversions match pyproj
    reference values to ~1 m;
  * CSVs (semicolon-delimited, decimal commas, Latin-1 bytes behind a lying
    charset=utf-8 header) parse into canonical multi-variable Observations;
  * stage cm -> m, the -777 missing sentinel, Tagesmittelwert column selection
    in the min/mean/max temperature CSV, Europe/Berlin -> UTC conversion for
    both CEST and CET, and daily/instantaneous resolution tagging.
"""

from datetime import UTC, datetime, timedelta
from zoneinfo import ZoneInfo

import httpx
import pytest
import respx

from csfs.connectors.germany_berlin import (
    GermanyBerlinConnector,
    _gk4_to_wgs84,
    _to_wgs84,
    _utm33n_to_wgs84,
)
from csfs.core.models import QualityFlag, Resolution, Variable

_BASE = "https://wasserportal.berlin.de"
_BERLIN_TZ = ZoneInfo("Europe/Berlin")

# ----------------------------------------------------------------------
# Captured payload snippets
# ----------------------------------------------------------------------

# Station-table rows modelled on the live catalogue (10 columns: id, name,
# Betreiber, Ausprägung, Gewässer, Fluss-km, Status, Rechtswert, Hochwert,
# Projektion). Row 2 carries the legacy 10x-easting quirk (3855170 instead
# of 385517); row 3 is a genuine GK4 (Gauss-Krüger) upstream gauge; rows 4/5
# are grab-sample categories that must be excluded from the catalogue.
CATALOGUE_HTML = """
<html><head><meta charset="ISO-8859-15"></head><body>
<table style='font-size:92%;' class='tablesorter' id='pegeltab'>
<thead>
<tr><th scope='col'>Messstellen-<br>nummer</th><th>Messstellen-<br>name</th>
<th>Betreiber</th><th>Ausprägung</th><th>Gewässer</th><th>Fluss-<br>kilometer</th>
<th>Mess-<br>stellen-<br>status</th><th>Rechts-<br>wert</th><th>Hoch-<br>wert</th>
<th>Projektion</th></tr>
</thead>
<tbody>
<tr>
 <td><a href='station.php?anzeige=i&thema=ows&station=5827101'>5827101</a></td>
 <td><a href='station.php?anzeige=i&thema=ows&station=5827101'>Fähre Rahnsdorf</a></td>
 <td>Land Berlin</td>
 <td><span class='btntext circle_blue'></span>Wasserstand | Durchfluss</td>
 <td>Müggelspree</td>
 <td style='text-align:right;'>11.35</td>
 <td>Aktiv</td>
 <td style='text-align:right;'>410862</td>
 <td style='text-align:right;'>5809624</td>
 <td>UTM 33N</td>
</tr>
<tr>
 <td><a href='station.php?anzeige=i&thema=owq&station=151'>151</a></td>
 <td><a href='station.php?anzeige=i&thema=owq&station=151'>MPS Caprivibrücke</a></td>
 <td>Land Berlin</td>
 <td><span class='btntext circle_blue'></span>Online-Messstelle</td>
 <td>Spree</td>
 <td style='text-align:right;'>8.75</td>
 <td>Aktiv</td>
 <td style='text-align:right;'>3855170</td>
 <td style='text-align:right;'>5820267</td>
 <td>UTM 33N</td>
</tr>
<tr>
 <td><a href='station.php?anzeige=i&thema=ows&station=5825500'>5825500</a></td>
 <td><a href='station.php?anzeige=i&thema=ows&station=5825500'>Beeskow, Spreeschleuse UP</a></td>
 <td>LfU Brandenburg</td>
 <td><span class='btntext circle_blue'></span>Wasserstand</td>
 <td>Spree</td>
 <td style='text-align:right;'>158.10</td>
 <td>Inaktiv</td>
 <td style='text-align:right;'>4654348</td>
 <td style='text-align:right;'>5784437</td>
 <td>GK4</td>
</tr>
<tr>
 <td><a href='station.php?anzeige=i&thema=obl&station=B334'>B334</a></td>
 <td><a href='station.php?anzeige=i&thema=obl&station=B334'>Alter Hof</a></td>
 <td>Land Berlin</td>
 <td><span class='btntext circle_green'></span>Badegewässerqualität</td>
 <td>Untere Havel</td>
 <td></td>
 <td></td>
 <td style='text-align:right;'>13.1423</td>
 <td style='text-align:right;'>52.4327</td>
 <td>WGS 84</td>
</tr>
<tr>
 <td><a href='station.php?anzeige=i&thema=opr&station=110'>110</a></td>
 <td><a href='station.php?anzeige=i&thema=opr&station=110'>Sophienwerder</a></td>
 <td>Land Berlin</td>
 <td><span class='btntext circle_grey'></span>Probenahme</td>
 <td>Havel</td>
 <td style='text-align:right;'>4.50</td>
 <td>Aktiv</td>
 <td style='text-align:right;'>383000</td>
 <td style='text-align:right;'>5822000</td>
 <td>UTM 33N</td>
</tr>
</tbody>
</table>
</body></html>
"""

# Live-captured CSV shapes. Note the decimal commas, the metadata "columns"
# embedded in the header, the "Fehlwerte: -777" sentinel declaration and the
# genuine portal typo "Wasssertemperatur".
STAGE_DAILY_CSV = (
    'Datum;Tagesmittelwert;"Stationsnummer: 5827101";"Stationsname: Fähre Rahnsdorf";'
    '"Gewässer: Müggelspree";"Wasserstand in cm";"Fehlwerte: -777"\n'
    "01.07.2026;55\n"
    "02.07.2026;57\n"
    "03.07.2026;-777\n"
)
DISCHARGE_DAILY_CSV = (
    'Datum;Tagesmittelwert;"Stationsnummer: 5827101";"Stationsname: Fähre Rahnsdorf";'
    '"Gewässer: Müggelspree";"Durchfluss in m³/s";"Fehlwerte: -777"\n'
    "01.07.2026;2,49\n"
    "02.07.2026;1,91\n"
)
TEMPERATURE_DAILY_CSV = (
    'Datum;Tagesminimum;"Zeitpunkt Minimum";Tagesmittelwert;Tagesmaximum;"Zeitpunkt Maximum";'
    '"Stationsnummer: 5827101";"Stationsname: Fähre Rahnsdorf";"Gewässer: Müggelspree";'
    '"Wasssertemperatur in °C";"Fehlwerte: -777"\n'
    '01.07.2026;26,10;"01.07.2026 22:00";26,44;27,00;"01.07.2026 00:00"\n'
    '02.07.2026;25,50;"02.07.2026 23:45";25,95;26,60;"02.07.2026 14:30"\n'
)
DISCHARGE_WINTER_CSV = (
    'Datum;Tagesmittelwert;"Stationsnummer: 5827101";"Stationsname: Fähre Rahnsdorf";'
    '"Gewässer: Müggelspree";"Durchfluss in m³/s";"Fehlwerte: -777"\n'
    "15.01.2026;3,10\n"
)
# Unknown station / unavailable thema: HTTP 200 with PHP notices, not CSV.
PHP_NOTICE_HTML = (
    "<br />\n<b>Notice</b>:  Undefined variable: strStationNr in "
    "<b>C:\\web\\bwpweb\\inc\\bwp_util.inc.php</b> on line <b>1765</b><br />\n"
)


def _csv_response(body: str) -> httpx.Response:
    """A CSV response as the portal really sends it: Latin-1 bytes behind a
    Content-Type header that (wrongly) claims utf-8."""
    return httpx.Response(
        200,
        content=body.encode("latin-1"),
        headers={"Content-Type": "text/csv; charset=utf-8"},
    )


def _mock_catalogue() -> None:
    respx.get(
        f"{_BASE}/start.php",
        params={"anzeige": "tabelle_ow", "messanzeige": "ms_all"},
    ).mock(
        return_value=httpx.Response(
            200,
            content=CATALOGUE_HTML.encode("iso-8859-15"),
            headers={"Content-Type": "text/html; charset=ISO-8859-15"},
        )
    )


def _mock_data(thema: str, sreihe: str, body: str) -> respx.Route:
    return respx.get(
        f"{_BASE}/station.php", params={"thema": thema, "sreihe": sreihe}
    ).mock(return_value=_csv_response(body))


# ----------------------------------------------------------------------
# Coordinate conversion (validated against pyproj EPSG:25833 / EPSG:31468)
# ----------------------------------------------------------------------


def test_utm33n_to_wgs84_known_points():
    """UTM 33N inverse matches pyproj to well under 1 m for Berlin gauges."""
    cases = [
        # (easting, northing, lat, lon) — pyproj EPSG:25833 -> EPSG:4326
        (410862.0, 5809624.0, 52.429549, 13.688954),  # Fähre Rahnsdorf
        (385517.0, 5820267.0, 52.520471, 13.312680),  # MPS Caprivibrücke
        (387758.0, 5822226.0, 52.538542, 13.345032),  # MPS Schifffahrtskanal
    ]
    for easting, northing, exp_lat, exp_lon in cases:
        lat, lon = _utm33n_to_wgs84(easting, northing)
        assert lat == pytest.approx(exp_lat, abs=1e-5)  # ~1 m
        assert lon == pytest.approx(exp_lon, abs=1e-5)


def test_gk4_to_wgs84_known_points():
    """GK4 (DHDN) inverse + Helmert shift matches pyproj to ~1 m."""
    cases = [
        # (easting, northing, lat, lon) — pyproj EPSG:31468 -> EPSG:4326
        (4654348.0, 5784437.0, 52.171977, 14.254683),  # Beeskow, Spreeschleuse
        (4661766.0, 5739074.0, 51.762505, 14.341674),  # Cottbus, Sandower Brücke
    ]
    for easting, northing, exp_lat, exp_lon in cases:
        lat, lon = _gk4_to_wgs84(easting, northing)
        assert lat == pytest.approx(exp_lat, abs=5e-5)  # ~5 m
        assert lon == pytest.approx(exp_lon, abs=5e-5)


def test_to_wgs84_dispatch():
    """_to_wgs84 dispatches on the Projektion column."""
    # WGS 84 rows already hold lon (Rechtswert) / lat (Hochwert).
    assert _to_wgs84("13.1423", "52.4327", "WGS 84") == (52.4327, 13.1423)
    # 10x-easting quirk: divided by 10 before the UTM inverse.
    quirk = _to_wgs84("3855170", "5820267", "UTM 33N")
    assert quirk is not None
    lat, lon = quirk
    assert lat == pytest.approx(52.520471, abs=1e-4)
    assert lon == pytest.approx(13.312680, abs=1e-4)
    # Unknown projections and non-numeric coordinates are rejected.
    assert _to_wgs84("410862", "5809624", "Soldner") is None
    assert _to_wgs84("", "5809624", "UTM 33N") is None


# ----------------------------------------------------------------------
# Station catalogue
# ----------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_catalogue():
    """Catalogue rows parse with per-Projektion coordinate handling."""
    _mock_catalogue()

    async with GermanyBerlinConnector() as conn:
        stations = await conn.fetch_stations()

    by_id = {s.native_id: s for s in stations}
    # Grab-sample categories (Badegewässerqualität, Probenahme) are excluded.
    assert set(by_id) == {"5827101", "151", "5825500"}

    a = by_id["5827101"]
    assert a.id == "germany_berlin:5827101"
    assert a.provider == "germany_berlin"
    assert a.country_code == "DE"
    assert a.name == "Fähre Rahnsdorf"  # Latin-9 bytes decoded correctly
    assert a.river == "Müggelspree"
    assert a.is_active is True
    assert a.latitude == pytest.approx(52.429549, abs=1e-4)
    assert a.longitude == pytest.approx(13.688954, abs=1e-4)

    # 10x-easting quirk row lands on the true Caprivibrücke location.
    b = by_id["151"]
    assert b.latitude == pytest.approx(52.520471, abs=1e-4)
    assert b.longitude == pytest.approx(13.312680, abs=1e-4)

    # GK4 row is converted via Gauss-Krüger, NOT the /10 hack (which would
    # put it ~15 km off near lon 14.49).
    c = by_id["5825500"]
    assert c.latitude == pytest.approx(52.171977, abs=1e-3)
    assert c.longitude == pytest.approx(14.254683, abs=1e-3)
    assert c.is_active is False


# ----------------------------------------------------------------------
# Observations: daily means (multi-variable)
# ----------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_daily_multivariable():
    """A >3-day window fetches daily means for all three variables."""
    stage_route = _mock_data("ows", "tw", STAGE_DAILY_CSV)
    flow_route = _mock_data("odf", "tw", DISCHARGE_DAILY_CSV)
    temp_route = _mock_data("owt", "tw", TEMPERATURE_DAILY_CSV)

    async with GermanyBerlinConnector() as conn:
        chunk = await conn.fetch_observations(
            "germany_berlin:5827101",
            start=datetime(2026, 6, 30, tzinfo=UTC),
            end=datetime(2026, 7, 5, tzinfo=UTC),
        )

    assert chunk.provider == "germany_berlin"
    assert chunk.station_id == "germany_berlin:5827101"
    # Exactly one bounded request per variable.
    assert stage_route.call_count == 1
    assert flow_route.call_count == 1
    assert temp_route.call_count == 1
    assert respx.calls.call_count == 3
    # sdatum is the Berlin-local calendar date of the window start.
    assert stage_route.calls[0].request.url.params["sdatum"] == "30.06.2026"
    assert stage_route.calls[0].request.url.params["smode"] == "c"

    stage = [o for o in chunk.observations if o.variable is Variable.STAGE]
    flow = [o for o in chunk.observations if o.variable is Variable.DISCHARGE]
    temp = [o for o in chunk.observations if o.variable is Variable.WATER_TEMPERATURE]
    assert len(stage) == 3 and len(flow) == 2 and len(temp) == 2

    # Stage: 55 cm -> 0.55 m; daily value stamped at Berlin local midnight
    # (CEST, UTC+2) -> 22:00 UTC of the previous day.
    assert stage[0].value == pytest.approx(0.55)
    assert stage[0].resolution is Resolution.DAILY_MEAN
    assert stage[0].quality is QualityFlag.RAW
    assert stage[0].timestamp == datetime(2026, 6, 30, 22, 0, tzinfo=UTC)

    # -777 sentinel -> missing observation (before the cm->m conversion).
    assert stage[2].value is None
    assert stage[2].quality is QualityFlag.MISSING

    # Discharge: decimal comma, already m³/s (no scaling).
    assert flow[0].value == pytest.approx(2.49)
    assert flow[0].discharge_m3s == pytest.approx(2.49)
    assert flow[1].value == pytest.approx(1.91)
    assert flow[0].resolution is Resolution.DAILY_MEAN

    # Temperature: the Tagesmittelwert column is selected, not min/max.
    assert temp[0].value == pytest.approx(26.44)
    assert temp[1].value == pytest.approx(25.95)
    assert temp[0].resolution is Resolution.DAILY_MEAN


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_winter_time_cet():
    """In winter (CET, UTC+1) local midnight maps to 23:00 UTC."""
    _mock_data("ows", "tw", PHP_NOTICE_HTML)
    _mock_data("odf", "tw", DISCHARGE_WINTER_CSV)
    _mock_data("owt", "tw", PHP_NOTICE_HTML)

    async with GermanyBerlinConnector() as conn:
        chunk = await conn.fetch_observations(
            "germany_berlin:5827101",
            start=datetime(2026, 1, 10, tzinfo=UTC),
            end=datetime(2026, 1, 20, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    obs = chunk.observations[0]
    assert obs.variable is Variable.DISCHARGE
    assert obs.value == pytest.approx(3.10)
    assert obs.timestamp == datetime(2026, 1, 14, 23, 0, tzinfo=UTC)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_slices_to_window():
    """Rows outside [start, end] are dropped (server ignores end dates)."""
    _mock_data("ows", "tw", STAGE_DAILY_CSV)
    _mock_data("odf", "tw", DISCHARGE_DAILY_CSV)
    _mock_data("owt", "tw", TEMPERATURE_DAILY_CSV)

    async with GermanyBerlinConnector() as conn:
        chunk = await conn.fetch_observations(
            "germany_berlin:5827101",
            # Covers only the local day 01.07 (stamped 30.06 22:00 UTC).
            start=datetime(2026, 6, 30, 12, 0, tzinfo=UTC),
            end=datetime(2026, 7, 1, 12, 0, tzinfo=UTC),
        )

    stamps = {o.timestamp for o in chunk.observations}
    assert stamps == {datetime(2026, 6, 30, 22, 0, tzinfo=UTC)}
    assert len(chunk.observations) == 3  # one per variable


# ----------------------------------------------------------------------
# Observations: instantaneous (Einzelwerte)
# ----------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_recent_short_window_uses_einzelwerte():
    """A short, recent window requests 15-min Einzelwerte for stage/discharge
    (sreihe=ew) but keeps water temperature on daily means."""
    now = datetime.now(UTC)
    t_local = (now - timedelta(hours=6)).astimezone(_BERLIN_TZ).replace(
        minute=0, second=0, microsecond=0
    )
    stage_csv = (
        'Datum;Einzelwert;"Stationsnummer: 5865200";"Stationsname: Hoppendorfer Straße";'
        '"Gewässer: Wuhle";"Wasserstand in cm";"Fehlwerte: -777"\n'
        f'"{t_local.strftime("%d.%m.%Y %H:%M")}";48\n'
    )
    flow_csv = (
        'Datum;Einzelwert;"Stationsnummer: 5865200";"Stationsname: Hoppendorfer Straße";'
        '"Gewässer: Wuhle";"Durchfluss in m³/s";"Fehlwerte: -777"\n'
        f'"{t_local.strftime("%d.%m.%Y %H:%M")}";0,546\n'
    )
    stage_route = _mock_data("ows", "ew", stage_csv)
    flow_route = _mock_data("odf", "ew", flow_csv)
    temp_route = _mock_data("owt", "tw", PHP_NOTICE_HTML)

    async with GermanyBerlinConnector() as conn:
        chunk = await conn.fetch_observations(
            "germany_berlin:5865200", start=now - timedelta(days=1), end=now
        )

    assert stage_route.call_count == 1
    assert flow_route.call_count == 1
    assert temp_route.call_count == 1

    stage = next(o for o in chunk.observations if o.variable is Variable.STAGE)
    flow = next(o for o in chunk.observations if o.variable is Variable.DISCHARGE)
    assert stage.resolution is Resolution.INSTANTANEOUS
    assert stage.value == pytest.approx(0.48)  # 48 cm -> 0.48 m
    assert stage.timestamp == t_local.astimezone(UTC)
    assert flow.resolution is Resolution.INSTANTANEOUS
    assert flow.value == pytest.approx(0.546)


# ----------------------------------------------------------------------
# Failure modes
# ----------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_unknown_station_yields_no_observations():
    """The portal answers unknown stations with HTTP 200 PHP-notice pages;
    these parse to zero observations rather than raising."""
    for thema in ("ows", "odf", "owt"):
        _mock_data(thema, "tw", PHP_NOTICE_HTML)

    async with GermanyBerlinConnector() as conn:
        chunk = await conn.fetch_observations(
            "germany_berlin:999999",
            start=datetime(2026, 6, 1, tzinfo=UTC),
            end=datetime(2026, 6, 20, tzinfo=UTC),
        )

    assert chunk.observations == []


@pytest.mark.asyncio
@respx.mock
async def test_all_requests_failing_raises_connector_error():
    """If every thema request fails outright, a ConnectorError is raised."""
    from csfs.core.exceptions import ConnectorError

    respx.get(f"{_BASE}/station.php").mock(return_value=httpx.Response(500))

    async with GermanyBerlinConnector() as conn:
        with pytest.raises(ConnectorError, match="all data requests failed"):
            await conn.fetch_observations(
                "germany_berlin:5827101",
                start=datetime(2026, 6, 1, tzinfo=UTC),
                end=datetime(2026, 6, 20, tzinfo=UTC),
            )


@pytest.mark.asyncio
@respx.mock
async def test_partial_failure_keeps_successful_variables():
    """One failing thema does not discard the other variables' data."""
    _mock_data("ows", "tw", STAGE_DAILY_CSV)
    respx.get(f"{_BASE}/station.php", params={"thema": "odf"}).mock(
        return_value=httpx.Response(500)
    )
    _mock_data("owt", "tw", TEMPERATURE_DAILY_CSV)

    async with GermanyBerlinConnector() as conn:
        chunk = await conn.fetch_observations(
            "germany_berlin:5827101",
            start=datetime(2026, 6, 30, tzinfo=UTC),
            end=datetime(2026, 7, 5, tzinfo=UTC),
        )

    variables = {o.variable for o in chunk.observations}
    assert variables == {Variable.STAGE, Variable.WATER_TEMPERATURE}


# ----------------------------------------------------------------------
# Registration
# ----------------------------------------------------------------------


def test_registration():
    """The connector is registered under the 'germany_berlin' slug."""
    from csfs.core.registry import discover, get_connector

    discover()
    cls = get_connector("germany_berlin")
    assert cls is GermanyBerlinConnector
    assert cls.slug == "germany_berlin"
    assert cls.country_codes == ["DE"]
    assert cls.supported_variables == ("discharge", "stage", "water_temperature")
