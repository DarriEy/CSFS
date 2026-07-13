# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Tests for the Portugal SNIRH connector with mocked HTML.

SNIRH serves one HTML table per (site, parameter) request via
``janela_verdados.php`` and — critically — ignores ``tmin``/``tmax``, always
returning the full period of record. The mock pages below are trimmed copies
of real archived responses (station 19B/01H, 2025-10 capture; stage values are
the real record). These tests verify multi-variable parsing (discharge +
stage), canonical resolutions, tz-aware UTC timestamps, client-side
windowing, "Sem dados." handling, graceful degradation, and that a failed
discharge request short-circuits the stage request. The conftest network
guard keeps the suite hermetic.
"""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.portugal_snirh import _DATA_PATH, PortugalSnirhConnector
from csfs.core.models import QualityFlag, Resolution, Variable
from csfs.core.registry import get_connector

_DATA_URL = f"https://snirh.apambiente.pt{_DATA_PATH}"


def _table_page(header: str, rows: list[tuple[str, str]]) -> str:
    """Render a trimmed janela_verdados page with the real cell markup.

    The real pages indent with tabs and wrap value-cell content in
    tabs/newlines; that whitespace is reproduced here via ``\\t`` escapes.
    """
    body = "".join(
        "\n\t\t\t  <tr>\n"
        f'\t\t\t  \t<td nowrap="nowrap" align="center" class="tbl_val">{ts}</td>\n'
        '\t\t\t\t\t\t\t\t\t\t<td align="right" class="tbl_val">\n'
        f"\t\t\t\t\t{val}\t\t\t\t</td>\n"
        "\t\t\t  </tr>\n"
        for ts, val in rows
    )
    return (
        '<script type="text/javascript">\n'
        "\tvar texto1 = 'A sua sessão terminou devido ao excessivo tempo de inactividade.';\n"
        "\talert(texto1)\n"
        "</script>\n"
        "<html>\n"
        "<head><title>SNIRH > Dados de Base</title></head>\n"
        "<body>\n"
        '<table width="100%" border="0"><tr><td class="banner_top_txt">Dados de Base</td></tr></table>\n'
        '<table border="0" cellspacing="0" cellpadding="0" align="center">\n'
        '  <tr><td style="padding:3px">\n'
        '\t\t<table width="100%" border="0" cellspacing="0px" cellpadding="2px">\n'
        "\t\t\t  <tr>\n"
        '\t\t\t  <td rowspan="2" align="center" class="tbl_tit">Data</td>\n'
        "\t\t\t  </tr>\n"
        "\t\t\t  <tr>\n"
        f'\t\t\t\t<td align="center" class="tbl_tit">{header}</td>\n'
        "\t\t\t  </tr>\n"
        f"{body}\n"
        "\t\t</table>\n"
        "  </td></tr>\n"
        "</table>\n"
        "</body>\n"
        "</html>"
    )


# Real stage record of station 19B/01H (SNIRH site id 1627743378).
STAGE_HTML = _table_page(
    "Nível médio diário (m)",
    [
        ("16/02/2002 00:00", "0.36"),
        ("17/02/2002 00:00", "0.35"),
        ("18/02/2002 00:00", "0.37"),
        ("19/02/2002 00:00", "0.38"),
    ],
)

DISCHARGE_HTML = _table_page(
    "Caudal médio diário (m3/s)",
    [
        ("16/02/2002 00:00", "4.20"),
        ("17/02/2002 00:00", "3.85"),
        ("18/02/2002 00:00", "5.10"),
    ],
)

# Trimmed copy of the real "no record for this parameter" page.
NO_DATA_HTML = """<html>
<body>
<table border="0" cellspacing="0" cellpadding="0" align="center">
  <tr><td style="padding:3px">
   <h4>Sem dados.<br><br>
   <a href="janela.php?obj_janela=INFO_PARAMETROS&tp_lista=I" target="_self">Click aqui
   para verificar os períodos com dados.</a></h4>
  </td></tr>
</table>
</body>
</html>"""


def _mock_snirh(discharge: str = DISCHARGE_HTML, stage: str = STAGE_HTML) -> respx.Route:
    """Route janela_verdados by the ``pars`` query parameter."""

    def handler(request: httpx.Request) -> httpx.Response:
        pars = request.url.params.get("pars")
        assert pars in ("1850", "1845"), f"unexpected pars={pars}"
        return httpx.Response(200, text=discharge if pars == "1850" else stage)

    return respx.get(_DATA_URL).mock(side_effect=handler)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_emits_discharge_and_stage():
    """Both daily-mean variables are parsed, in SI units, tz-aware UTC."""
    route = _mock_snirh()
    start = datetime(2002, 2, 1, tzinfo=UTC)
    end = datetime(2002, 3, 1, tzinfo=UTC)

    async with PortugalSnirhConnector() as conn:
        chunk = await conn.fetch_observations("portugal_snirh:19B/01H", start, end)

    assert chunk.station_id == "portugal_snirh:19B/01H"
    assert chunk.provider == "portugal_snirh"
    assert route.call_count == 2  # one request per parameter
    # The data request must target the SNIRH numeric site id, not the code.
    assert route.calls[0].request.url.params["sites"] == "1627743378"
    assert route.calls[0].request.url.params["pars"] == "1850"

    discharge = [o for o in chunk.observations if o.variable is Variable.DISCHARGE]
    stage = [o for o in chunk.observations if o.variable is Variable.STAGE]
    assert [o.value for o in discharge] == [4.20, 3.85, 5.10]
    assert [o.value for o in stage] == [0.36, 0.35, 0.37, 0.38]
    for obs in chunk.observations:
        assert obs.resolution is Resolution.DAILY_MEAN
        assert obs.quality is QualityFlag.RAW
        assert obs.timestamp.tzinfo is UTC
    assert discharge[0].timestamp == datetime(2002, 2, 16, tzinfo=UTC)
    assert stage[-1].timestamp == datetime(2002, 2, 19, tzinfo=UTC)


@pytest.mark.asyncio
@respx.mock
async def test_client_side_windowing_filters_full_record():
    """The server ignores tmin/tmax; rows outside [start, end] are dropped."""
    _mock_snirh()
    start = datetime(2002, 2, 17, tzinfo=UTC)
    end = datetime(2002, 2, 18, 23, 59, tzinfo=UTC)

    async with PortugalSnirhConnector() as conn:
        chunk = await conn.fetch_observations("portugal_snirh:19B/01H", start, end)

    stamps = {(o.variable, o.timestamp.day) for o in chunk.observations}
    assert stamps == {
        (Variable.DISCHARGE, 17),
        (Variable.DISCHARGE, 18),
        (Variable.STAGE, 17),
        (Variable.STAGE, 18),
    }


@pytest.mark.asyncio
@respx.mock
async def test_stage_without_record_yields_discharge_only():
    """A 'Sem dados.' stage page is valid-but-empty; discharge still returned."""
    _mock_snirh(stage=NO_DATA_HTML)
    start = datetime(2002, 2, 1, tzinfo=UTC)
    end = datetime(2002, 3, 1, tzinfo=UTC)

    async with PortugalSnirhConnector() as conn:
        chunk = await conn.fetch_observations("portugal_snirh:19B/01H", start, end)

    assert {o.variable for o in chunk.observations} == {Variable.DISCHARGE}
    assert len(chunk.observations) == 3


@pytest.mark.asyncio
@respx.mock
async def test_http_403_degrades_gracefully_and_skips_stage():
    """The SNIRH edge 403s blocked IP ranges: empty chunk, and no second
    (stage) request after the discharge request failed structurally."""
    route = respx.get(_DATA_URL).mock(return_value=httpx.Response(403, text="403 Forbidden"))
    start = datetime(2002, 2, 1, tzinfo=UTC)
    end = datetime(2002, 3, 1, tzinfo=UTC)

    async with PortugalSnirhConnector() as conn:
        chunk = await conn.fetch_observations("portugal_snirh:19B/01H", start, end)

    assert chunk.observations == []
    assert chunk.station_id == "portugal_snirh:19B/01H"
    assert route.call_count == 1  # stage request was skipped


@pytest.mark.asyncio
@respx.mock
async def test_unrecognizable_page_skips_stage():
    """A 200 that is not a SNIRH data page (WAF interstitial) short-circuits."""
    route = respx.get(_DATA_URL).mock(
        return_value=httpx.Response(200, text="<html><body>maintenance</body></html>")
    )
    start = datetime(2002, 2, 1, tzinfo=UTC)
    end = datetime(2002, 3, 1, tzinfo=UTC)

    async with PortugalSnirhConnector() as conn:
        chunk = await conn.fetch_observations("portugal_snirh:19B/01H", start, end)

    assert chunk.observations == []
    assert route.call_count == 1


@pytest.mark.asyncio
async def test_unknown_station_returns_empty_without_requests():
    """A station code missing from the catalogue makes no HTTP request."""
    start = datetime(2002, 2, 1, tzinfo=UTC)
    end = datetime(2002, 3, 1, tzinfo=UTC)

    async with PortugalSnirhConnector() as conn:
        chunk = await conn.fetch_observations("portugal_snirh:ZZ/99X", start, end)

    assert chunk.observations == []


@pytest.mark.asyncio
@respx.mock
async def test_raw_numeric_site_id_is_accepted():
    """A raw SNIRH numeric id (not in the catalogue) is passed through."""
    route = _mock_snirh()
    start = datetime(2002, 2, 1, tzinfo=UTC)
    end = datetime(2002, 3, 1, tzinfo=UTC)

    async with PortugalSnirhConnector() as conn:
        chunk = await conn.fetch_observations("portugal_snirh:920686150", start, end)

    assert route.calls[0].request.url.params["sites"] == "920686150"
    assert len(chunk.observations) == 7


@pytest.mark.asyncio
async def test_fetch_stations_returns_seed_catalog():
    """The bundled catalogue is served without any network access."""
    async with PortugalSnirhConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 715
    byid = {s.native_id: s for s in stations}
    s = byid["19B/01H"]
    assert s.id == "portugal_snirh:19B/01H"
    assert s.provider == "portugal_snirh"
    assert s.country_code == "PT"
    assert s.latitude == pytest.approx(39.152)
    assert s.longitude == pytest.approx(-9.302)
    # Gauges with corrupt upstream coordinates are excluded from the seed.
    for excluded in ("04J/09A", "12G/05H", "21O/03H"):
        assert excluded not in byid
    # Everything must sit inside mainland Portugal's bounding box.
    assert all(36.5 <= st.latitude <= 42.5 for st in stations)
    assert all(-10.0 <= st.longitude <= -6.0 for st in stations)


def test_registered():
    """Connector is importable and registered under its slug."""
    cls = get_connector("portugal_snirh")
    assert cls is PortugalSnirhConnector
    assert cls.slug == "portugal_snirh"
    assert cls.country_codes == ["PT"]
    assert cls.supported_variables == ("discharge", "stage")
