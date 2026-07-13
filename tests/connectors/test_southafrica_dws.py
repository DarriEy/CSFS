# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Tests for the South Africa DWS connector with mocked HyData responses.

Payloads are trimmed copies of real ``HyData.aspx`` responses (captured live
via the legacy ``www.dwa.gov.za`` mirror and the Wayback Machine): fixed-width
``<pre>`` text where either value column may be blank while its quality code is
present, the "No data for requested period." page, and the Hydstra/ODBC error
page currently returned by the mirror's Daily mode. The conftest network guard
keeps the suite hermetic.
"""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.southafrica_dws import SouthAfricaDwsConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import QualityFlag, Resolution, Variable
from csfs.core.registry import get_connector

_DATA_URL = "https://www.dws.gov.za/Hydrology/Verified/HyData.aspx"

_HTML_TAIL = """
<!DOCTYPE html PUBLIC "-//W3C//DTD XHTML 1.0 Transitional//EN" "http://www.w3.org/TR/xhtml1/DTD/xhtml1-transitional.dtd">

<html xmlns="http://www.w3.org/1999/xhtml" >
<head><title>
\tX3H001100.00
</title></head>
<body>
    <form method="post" action="./HyData.aspx?Station=X3H001100.00" id="form1">
    </form>
</body>
</html>
"""

# Daily mode: date [1-8], daily avg flow [10-18], quality [20-24]. The
# 2023-01-03 row is a gap (blank value, quality 170 "permanent gap") and the
# 2023-01-04 row is a program estimate (quality 66). 2022-12-30 lies outside
# the requested window and must be filtered out.
DAILY_HTML = (
    "<p><pre>Data are continuously updated and reviewed.\n"
    "The format of this file is as follows:\n"
    "POS.  1-8   = Date of daily flow  CCYYMMDD\n"
    "POS. 10-18  = Daily avg flow rate in cubic metres/sec 99999.999\n"
    "POS. 20-24  = Quality code\n"
    "\n"
    "X3H001\n"
    "Variable 100.00 Surface Water Level\n"
    "\n"
    "DATE     D AVG F/R  QUAL\n"
    "20221230     9.999     1\n"
    "20230101     5.142     1\n"
    "20230102     4.890     2\n"
    "20230103             170\n"
    "20230104     4.500    66\n"
    "</pre>" + _HTML_TAIL
)

# Point mode: date [1-8], time [10-15], corrected level [27-35] + qual
# [37-40], corrected flow [52-60] + qual [62-65]. Times are SAST (UTC+2).
# The 060000 row has no rating (quality 161): level present, flow blank.
POINT_HTML = (
    "<p><pre>Data are continuously updated and reviewed.\n"
    "The format of this file is as follows:\n"
    "POS.  1-8   = Date of measurement CCYYMMDD\n"
    "POS. 10-15  = Time of measurement HHMMSS\n"
    "POS. 27-35  = Corrected level in m\n"
    "POS. 37-40  = Quality code\n"
    "POS. 52-60  = Corrected flow in cubic metres/sec\n"
    "POS. 62-65  = Quality code\n"
    "X3H001\n"
    "Variable 100.00 Surface Water Level\n"
    "DATE     TIME             COR.LEVEL QUA           COR.FLOW  QUA\n"
    "20230101 042400               0.289   1               5.142   1\n"
    "20230101 060000               0.485   1                     161\n"
    "20230101 150000               0.288   1               5.102   1\n"
    "</pre>" + _HTML_TAIL
)

NO_DATA_HTML = "No data for requested period.\n" + _HTML_TAIL

# Hydstra/ODBC failure page: error text before the HTML document, no <pre>.
# Observed live for Daily mode on the legacy mirror (2026-07).
ERROR_HTML = (
    "ERROR [HY000] [Kisters][ScriptServerODBC Driver]General error. "
    "Expected date/time value 'YYYY-MM-DD[_HH:MM[:SS]]'. Current token "
    "[tkValueQuote] ,current value ['2023-01-31_24:00'] at /hydstra/odbc[line:222]\n"
    " at function HydstraSelect\n" + _HTML_TAIL
)

_START = datetime(2023, 1, 1, tzinfo=UTC)
_END = datetime(2023, 1, 5, tzinfo=UTC)


def _mock_modes(daily: httpx.Response, point: httpx.Response) -> tuple:
    daily_route = respx.get(_DATA_URL, params={"DataType": "Daily"}).mock(
        return_value=daily
    )
    point_route = respx.get(_DATA_URL, params={"DataType": "Point"}).mock(
        return_value=point
    )
    return daily_route, point_route


def test_registered():
    """Connector is importable and registered under its slug."""
    cls = get_connector("southafrica_dws")
    assert cls is SouthAfricaDwsConnector
    assert cls.slug == "southafrica_dws"
    assert cls.country_codes == ["ZA"]
    assert cls.supported_variables == ("discharge", "stage")


async def test_fetch_stations_seed_catalog():
    """The bundled seed catalog is returned without any network access."""
    async with SouthAfricaDwsConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1294
    a2h001 = next(s for s in stations if s.native_id == "A2H001")
    assert a2h001.id == "southafrica_dws:A2H001"
    assert a2h001.provider == "southafrica_dws"
    assert a2h001.country_code == "ZA"
    assert a2h001.latitude == pytest.approx(-25.73386)
    assert a2h001.longitude == pytest.approx(27.85969)
    # All gauges lie within South Africa's bounding box.
    assert all(-35.0 < s.latitude < -22.0 for s in stations)
    assert all(16.0 < s.longitude < 33.0 for s in stations)


@respx.mock
async def test_fetch_observations_daily_and_point():
    """Daily means + instantaneous discharge/stage are emitted in UTC."""
    daily_route, point_route = _mock_modes(
        httpx.Response(200, text=DAILY_HTML),
        httpx.Response(200, text=POINT_HTML),
    )

    async with SouthAfricaDwsConnector() as conn:
        chunk = await conn.fetch_observations("southafrica_dws:X3H001", _START, _END)

    assert chunk.provider == "southafrica_dws"
    assert daily_route.called and point_route.called
    params = daily_route.calls[0].request.url.params
    assert params["Station"] == "X3H001100.00"  # gauge id + variable suffix
    assert params["SiteType"] == "RIV"
    assert params["StartDT"] == "2023-01-01"

    daily = [o for o in chunk.observations if o.resolution is Resolution.DAILY_MEAN]
    assert [(o.timestamp, o.value) for o in daily] == [
        (datetime(2023, 1, 1, tzinfo=UTC), 5.142),
        (datetime(2023, 1, 2, tzinfo=UTC), 4.890),
        (datetime(2023, 1, 4, tzinfo=UTC), 4.500),  # gap day 2023-01-03 skipped
    ]
    assert all(o.variable is Variable.DISCHARGE for o in daily)
    assert daily[0].quality is QualityFlag.GOOD  # code 1
    assert daily[1].quality is QualityFlag.GOOD  # code 2 (good edited)
    assert daily[2].quality is QualityFlag.ESTIMATED  # code 66

    inst = [o for o in chunk.observations if o.resolution is Resolution.INSTANTANEOUS]
    flows = [o for o in inst if o.variable is Variable.DISCHARGE]
    stages = [o for o in inst if o.variable is Variable.STAGE]
    # SAST (UTC+2) 04:24 -> 02:24 UTC.
    assert flows[0].timestamp == datetime(2023, 1, 1, 2, 24, tzinfo=UTC)
    assert flows[0].value == 5.142  # m3/s
    assert flows[0].quality is QualityFlag.GOOD
    assert stages[0].timestamp == datetime(2023, 1, 1, 2, 24, tzinfo=UTC)
    assert stages[0].value == 0.289  # m
    # The 06:00 SAST row has no rating (qual 161): stage only, no flow.
    assert [o.timestamp for o in stages] == [
        datetime(2023, 1, 1, 2, 24, tzinfo=UTC),
        datetime(2023, 1, 1, 4, 0, tzinfo=UTC),
        datetime(2023, 1, 1, 13, 0, tzinfo=UTC),
    ]
    assert [o.timestamp for o in flows] == [
        datetime(2023, 1, 1, 2, 24, tzinfo=UTC),
        datetime(2023, 1, 1, 13, 0, tzinfo=UTC),
    ]


@respx.mock
async def test_no_data_period_returns_empty():
    """The 'No data for requested period.' page yields an empty chunk."""
    _mock_modes(
        httpx.Response(200, text=NO_DATA_HTML),
        httpx.Response(200, text=NO_DATA_HTML),
    )

    async with SouthAfricaDwsConnector() as conn:
        chunk = await conn.fetch_observations("southafrica_dws:X3H001", _START, _END)

    assert chunk.observations == []
    assert chunk.station_id == "southafrica_dws:X3H001"


@respx.mock
async def test_daily_error_page_still_yields_point_data():
    """A Hydstra/ODBC error in Daily mode must not lose the Point data."""
    _mock_modes(
        httpx.Response(200, text=ERROR_HTML),
        httpx.Response(200, text=POINT_HTML),
    )

    async with SouthAfricaDwsConnector() as conn:
        chunk = await conn.fetch_observations("southafrica_dws:X3H001", _START, _END)

    assert len(chunk.observations) == 5  # 3 stage + 2 flow, no daily means
    assert all(o.resolution is Resolution.INSTANTANEOUS for o in chunk.observations)


@respx.mock
async def test_long_window_requests_daily_only():
    """Windows beyond the server's 1-year Point cap issue a single Daily call."""
    daily_route, point_route = _mock_modes(
        httpx.Response(200, text=DAILY_HTML),
        httpx.Response(200, text=POINT_HTML),
    )

    async with SouthAfricaDwsConnector() as conn:
        chunk = await conn.fetch_observations(
            "southafrica_dws:X3H001",
            datetime(2020, 1, 1, tzinfo=UTC),
            _END,
        )

    assert daily_route.call_count == 1
    assert not point_route.called
    assert all(o.resolution is Resolution.DAILY_MEAN for o in chunk.observations)


@respx.mock
async def test_all_requests_failing_raises_connector_error():
    """If every issued request fails (e.g. WAF 403), raise ConnectorError."""
    _mock_modes(httpx.Response(403), httpx.Response(403))

    async with SouthAfricaDwsConnector() as conn:
        with pytest.raises(ConnectorError):
            await conn.fetch_observations("southafrica_dws:X3H001", _START, _END)


@respx.mock
async def test_base_url_override_for_legacy_mirror():
    """config['base_url'] retargets requests (legacy www.dwa.gov.za mirror)."""
    route = respx.get("https://www.dwa.gov.za/Hydrology/Verified/HyData.aspx").mock(
        return_value=httpx.Response(200, text=NO_DATA_HTML)
    )

    conn = SouthAfricaDwsConnector(
        config={"base_url": "https://www.dwa.gov.za", "verify_ssl": False}
    )
    async with conn:
        chunk = await conn.fetch_observations("southafrica_dws:X3H001", _START, _END)

    assert route.called
    assert chunk.observations == []
