# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Tests for the Bulgaria EAEMDR (Danube) connector with mocked HTML.

EAEMDR publishes a live daily hydrology bulletin (HTML table) for the Bulgarian
section of the Danube. A subset of gauges report a current discharge (m3/s);
the rest report level only. These tests mock that bulletin and verify discharge
parsing, level-only exclusion, date handling, graceful degradation when the host
is unreachable, and connector registration. The conftest network guard keeps the
suite hermetic.
"""

from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx

from csfs.connectors.bulgaria_eaemdr import _HIDROLOGY_PATH, EAEMDRConnector
from csfs.core.models import QualityFlag
from csfs.core.registry import get_connector

_BULLETIN_URL = f"https://appd-bg.org{_HIDROLOGY_PATH}"

# Trimmed-down copy of the real bulletin: the date heading plus the first
# "Water levels" summary table. Ruse/Silistra report discharge (m3/s); Vidin is
# a level-only gauge (empty discharge cell) and must be excluded.
MOCK_HTML = """<!DOCTYPE html><html><body>
<h3>Water levels on the bulgarian section of the Danube river 02.06.2026 г.<br/></h3>
<table class="dbg-table">
    <tr>
        <td>station</td><td>kilometre</td><td>water level (cm)</td>
        <td>discharge (m3/s)</td><td>24 hours difference (cm)</td><td>t water</td>
    </tr>
    <tr>
        <td>Vidin</td>
        <td>790.30</td>
        <td><span style="color: blue;">90</span></td>
        <td></td>
        <td>-1</td>
        <td><span style="color: blue;">20.5</span></td>
    </tr>
    <tr>
        <td>Ruse</td>
        <td>495.60</td>
        <td><span style="color: blue;">72</span></td>
        <td>3289</td>
        <td>-19</td>
        <td><span style="color: blue;">21.6</span></td>
    </tr>
    <tr>
        <td>Silistra</td>
        <td>375.50</td>
        <td><span style="color: blue;">110</span></td>
        <td>3593</td>
        <td>-24</td>
        <td><span style="color: blue;">22.3</span></td>
    </tr>
</table>
</body></html>
"""


def _mock_bulletin(text: str = MOCK_HTML, status: int = 200) -> None:
    respx.get(_BULLETIN_URL).mock(return_value=httpx.Response(status, text=text))


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_only_discharge_gauges():
    """Only gauges reporting discharge (m3/s) are surfaced; level-only excluded."""
    _mock_bulletin()
    async with EAEMDRConnector() as conn:
        stations = await conn.fetch_stations()

    names = {s.native_id for s in stations}
    assert names == {"Ruse", "Silistra"}  # Vidin (level-only) excluded
    s = next(s for s in stations if s.native_id == "Ruse")
    assert s.id == "bulgaria_eaemdr:Ruse"
    assert s.provider == "bulgaria_eaemdr"
    assert s.country_code == "BG"
    assert s.river == "Danube"
    assert s.latitude == pytest.approx(43.85, abs=0.01)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_returns_discharge_m3s():
    """A station's current discharge is returned as a single m3/s observation."""
    _mock_bulletin()
    end = datetime(2026, 6, 2, 12, 0, tzinfo=UTC)
    start = end - timedelta(days=30)

    async with EAEMDRConnector() as conn:
        chunk = await conn.fetch_observations("bulgaria_eaemdr:Ruse", start, end)

    assert chunk.station_id == "bulgaria_eaemdr:Ruse"
    assert chunk.provider == "bulgaria_eaemdr"
    assert len(chunk.observations) == 1
    obs = chunk.observations[0]
    assert obs.discharge_m3s == 3289.0  # m3/s
    assert obs.timestamp == datetime(2026, 6, 2, tzinfo=UTC)
    assert obs.quality == QualityFlag.RAW


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_out_of_range_returns_empty():
    """Snapshot outside the requested window yields no observations."""
    _mock_bulletin()
    # Window entirely before the bulletin date (2026-06-02).
    start = datetime(2025, 1, 1, tzinfo=UTC)
    end = datetime(2025, 2, 1, tzinfo=UTC)

    async with EAEMDRConnector() as conn:
        chunk = await conn.fetch_observations("bulgaria_eaemdr:Ruse", start, end)

    assert chunk.observations == []


@pytest.mark.asyncio
@respx.mock
async def test_level_only_station_has_no_observation():
    """A level-only gauge (empty discharge cell) yields no discharge obs."""
    _mock_bulletin()
    end = datetime(2026, 6, 2, 12, 0, tzinfo=UTC)
    start = end - timedelta(days=30)

    async with EAEMDRConnector() as conn:
        chunk = await conn.fetch_observations("bulgaria_eaemdr:Vidin", start, end)

    assert chunk.observations == []


@pytest.mark.asyncio
@respx.mock
async def test_unreachable_host_degrades_gracefully():
    """If the host errors, fetch_* return empty rather than raising."""
    respx.get(_BULLETIN_URL).mock(side_effect=httpx.ConnectError("host down"))
    end = datetime(2026, 6, 2, 12, 0, tzinfo=UTC)
    start = end - timedelta(days=30)

    async with EAEMDRConnector() as conn:
        stations = await conn.fetch_stations()
        chunk = await conn.fetch_observations("bulgaria_eaemdr:Ruse", start, end)

    assert stations == []
    assert chunk.observations == []
    assert chunk.station_id == "bulgaria_eaemdr:Ruse"


def test_registered():
    """Connector is importable and registered under its slug."""
    cls = get_connector("bulgaria_eaemdr")
    assert cls is EAEMDRConnector
    assert cls.slug == "bulgaria_eaemdr"
    assert cls.country_codes == ["BG"]
