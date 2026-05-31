"""Tests for the Japan MLIT Water Information System connector."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.japan_mlit import (
    _SEED_STATIONS,
    JapanMlitConnector,
    _flag_to_quality,
)
from csfs.core.models import QualityFlag

BASE = "http://www1.river.go.jp"
DATA_URL = f"{BASE}/cgi-bin/DspWaterData.exe"
DAT_PATH = "/dat/dload/download/test123.dat"
DAT_URL = f"{BASE}{DAT_PATH}"

# Display page embeds the .dat download link.
MOCK_PAGE = (
    f'<HTML><BODY><a href="{DAT_PATH}">'
    '<img src="/img/download.gif"></a></BODY></HTML>'
).encode("euc-jp")

# .dat: one row per day, 24 hourly value,flag pairs. Hour h -> h:00 JST.
# Row below has values for hours 1..24; hour 3 missing (-9999.99),
# hour 4 flagged provisional (*), hour 5 flagged missing ($).
_HOURS = []
for h in range(1, 25):
    if h == 3:
        _HOURS += ["-9999.99", "-"]
    elif h == 4:
        _HOURS += ["   1.50", "*"]
    elif h == 5:
        _HOURS += ["-9999.99", "$"]
    else:
        _HOURS += [f"{h:7.2f}", " "]
MOCK_DAT = ("2000/07/01," + ",".join(_HOURS) + "\n").encode("euc-jp")


_NO_LINK_PAGE = "<HTML>no data for this month</HTML>".encode("euc-jp")


def _route():
    """Wire the 2-step scrape: only July 2000 has data, other months empty."""
    # July 2000 -> page with the .dat link.
    respx.get(DATA_URL, params={"BGNDATE": "20000701"}).mock(
        return_value=httpx.Response(200, content=MOCK_PAGE),
    )
    respx.get(DAT_URL).mock(return_value=httpx.Response(200, content=MOCK_DAT))
    # Any other month -> page without a download link.
    respx.get(DATA_URL).mock(
        return_value=httpx.Response(200, content=_NO_LINK_PAGE),
    )


# ======================================================================
# Station tests
# ======================================================================


@pytest.mark.asyncio
async def test_fetch_stations_returns_seed():
    """fetch_stations returns the curated seed stations."""
    async with JapanMlitConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    for s in stations:
        assert s.provider == "japan_mlit"
        assert s.country_code == "JP"
        assert s.id == f"japan_mlit:{s.native_id}"
        assert s.latitude != 0.0 and s.longitude != 0.0


# ======================================================================
# Observation tests
# ======================================================================


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_dat():
    """The 2-step scrape parses hourly discharge with JST->UTC conversion."""
    _route()

    async with JapanMlitConnector() as conn:
        # JST day 2000-07-01 spans UTC 2000-06-30 15:00 .. 2000-07-01 15:00.
        chunk = await conn.fetch_observations(
            "japan_mlit:309151289916040",
            start=datetime(2000, 6, 30, tzinfo=UTC),
            end=datetime(2000, 7, 2, tzinfo=UTC),
        )

    obs = chunk.observations
    assert chunk.provider == "japan_mlit"
    # 24 hours minus the 2 missing-value hours = 22 numeric points.
    numeric = [o for o in obs if o.discharge_m3s is not None]
    assert len(numeric) == 22

    # Hour 1 JST (01:00 JST) == 2000-06-30 16:00 UTC, value 1.00.
    first = obs[0]
    assert first.timestamp == datetime(2000, 6, 30, 16, 0, tzinfo=UTC)
    assert first.discharge_m3s == pytest.approx(1.0)
    assert first.quality == QualityFlag.GOOD

    # Hour 4 flagged provisional (*), value present.
    hour4 = obs[3]
    assert hour4.discharge_m3s == pytest.approx(1.5)
    assert hour4.quality == QualityFlag.RAW

    # Hour 3 missing value -> None / MISSING.
    hour3 = obs[2]
    assert hour3.discharge_m3s is None
    assert hour3.quality == QualityFlag.MISSING


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_filters_window():
    """Observations outside [start, end] are dropped."""
    _route()

    async with JapanMlitConnector() as conn:
        # Narrow UTC window: only hours that map into it survive.
        chunk = await conn.fetch_observations(
            "japan_mlit:309151289916040",
            start=datetime(2000, 7, 1, 0, 0, tzinfo=UTC),   # 09:00 JST
            end=datetime(2000, 7, 1, 2, 0, tzinfo=UTC),     # 11:00 JST
        )

    # JST hours 9, 10, 11 fall in the window (values present for all).
    assert len(chunk.observations) == 3
    assert chunk.observations[0].timestamp == datetime(2000, 7, 1, 0, 0, tzinfo=UTC)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_no_download_link_returns_empty():
    """A page without a .dat link yields no observations (not an error)."""
    respx.get(DATA_URL).mock(
        return_value=httpx.Response(200, content="<HTML>no data</HTML>".encode("euc-jp")),
    )

    async with JapanMlitConnector() as conn:
        chunk = await conn.fetch_observations(
            "japan_mlit:309151289916040",
            start=datetime(2000, 7, 1, tzinfo=UTC),
            end=datetime(2000, 7, 2, tzinfo=UTC),
        )

    assert chunk.observations == []


# ======================================================================
# Unit helpers
# ======================================================================


def test_months_enumeration():
    months = JapanMlitConnector._months(
        datetime(2019, 11, 1, tzinfo=UTC),
        datetime(2020, 2, 15, tzinfo=UTC),
    )
    assert months == [(2019, 11), (2019, 12), (2020, 1), (2020, 2)]


def test_months_single():
    months = JapanMlitConnector._months(
        datetime(2020, 5, 3, tzinfo=UTC),
        datetime(2020, 5, 28, tzinfo=UTC),
    )
    assert months == [(2020, 5)]


def test_parse_value():
    assert JapanMlitConnector._parse_value("   1.50") == pytest.approx(1.5)
    assert JapanMlitConnector._parse_value("-9999.99") is None
    assert JapanMlitConnector._parse_value("") is None
    assert JapanMlitConnector._parse_value("abc") is None


def test_flag_to_quality():
    assert _flag_to_quality(" ") == QualityFlag.GOOD
    assert _flag_to_quality("*") == QualityFlag.RAW
    assert _flag_to_quality("#") == QualityFlag.ESTIMATED
    assert _flag_to_quality("$") == QualityFlag.MISSING
    assert _flag_to_quality("-") == QualityFlag.MISSING


def test_connector_is_registered():
    from csfs.core.registry import get_connector

    assert get_connector("japan_mlit") is JapanMlitConnector
