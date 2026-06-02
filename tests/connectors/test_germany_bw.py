"""Tests for the HVZ Baden-Württemberg (germany_bw) connector.

HVZ ships its station catalogue as a JavaScript file containing a
``HVZ_Site.PEG_DB = [ ... ];`` array. These tests mock that file and verify:
  * only discharge-capable stations are returned;
  * the current discharge value is parsed in m³/s (Abfluss), NOT water level;
  * timestamps are converted from MESZ/MEZ local time to UTC;
  * the connector is registered.
"""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.germany_bw import GermanyBwConnector

_CATALOGUE_URL = "https://www.hvz.baden-wuerttemberg.de/js/hvz_peg_stmn.js"

# A trimmed PEG_DB. Columns: 0=DASA 1=NAME 2=GEW 3=FG 4=W 5=WD 6=WZ
# 7=Q 8=QD 9=QZ 10=FLAG 11=FILE 12..19=misc 20=LON 21=LAT ...
# Cols 12-19 are filled with eight zeros so LON/LAT land at indices 20/21.
_MID = ",0" * 8  # columns 12..19
_PAD = ",0" * 5  # extra trailing columns so len(row) > 21
MOCK_CATALOGUE = (
    "// Stammdaten Stand: 02.06.2026\n"
    "HVZ_Site.PEG_LHP = 0;\n"
    "HVZ_Site.PEG_DB =\n"
    "[\n"
    # Station A: has discharge (12.6 m³/s), water level 52 cm.
    " ['00435','Maxau','Rhein',1,'52','cm','02.06.2026 07:00 MESZ',"
    "'12.6','m³/s','02.06.2026 07:00 MESZ',0,'file'" + _MID + ","
    "8.30,49.03" + _PAD + "],\n"
    # Station B: water level only (Q == '--'), must be excluded from stations.
    " ['00500','LevelOnly','Donau',1,'315','cm','02.06.2026 07:00 MESZ',"
    "'--','','--',0,'file'" + _MID + ",9.0,48.0" + _PAD + "],\n"
    # Station C: discharge in winter time (MEZ = UTC+1).
    " ['00076','Rengers','Untere Argen',1,'34','cm','02.01.2026 06:00 MEZ',"
    "'0.42','m³/s','02.01.2026 06:00 MEZ',0,'file'" + _MID + ","
    "9.95,47.65" + _PAD + "],\n"
    "];\n"
)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_filters_discharge():
    """Only stations that publish a discharge (m³/s) value are returned."""
    respx.get(_CATALOGUE_URL).mock(
        return_value=httpx.Response(200, text=MOCK_CATALOGUE)
    )

    async with GermanyBwConnector() as conn:
        stations = await conn.fetch_stations()

    # Station B (water level only) is excluded.
    native_ids = {s.native_id for s in stations}
    assert native_ids == {"00435", "00076"}

    a = next(s for s in stations if s.native_id == "00435")
    assert a.id == "germany_bw:00435"
    assert a.provider == "germany_bw"
    assert a.country_code == "DE"
    assert a.name == "Maxau"
    assert a.river == "Rhein"
    assert a.latitude == pytest.approx(49.03)
    assert a.longitude == pytest.approx(8.30)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_discharge_in_m3s():
    """The current discharge value is parsed in m³/s (Abfluss, not water level)."""
    respx.get(_CATALOGUE_URL).mock(
        return_value=httpx.Response(200, text=MOCK_CATALOGUE)
    )

    async with GermanyBwConnector() as conn:
        chunk = await conn.fetch_observations(
            "germany_bw:00435",
            start=datetime(2026, 6, 1, tzinfo=UTC),
            end=datetime(2026, 6, 3, tzinfo=UTC),
        )

    assert chunk.provider == "germany_bw"
    assert chunk.station_id == "germany_bw:00435"
    assert len(chunk.observations) == 1

    obs = chunk.observations[0]
    # 12.6 m³/s — the discharge column, NOT 52 (water level in cm) or 0.52 m.
    assert obs.discharge_m3s == pytest.approx(12.6)
    assert obs.quality.value == "raw"
    # 07:00 MESZ (UTC+2) -> 05:00 UTC.
    assert obs.timestamp == datetime(2026, 6, 2, 5, 0, tzinfo=UTC)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_mez_winter_time():
    """Winter-time MEZ timestamps are converted with a UTC+1 offset."""
    respx.get(_CATALOGUE_URL).mock(
        return_value=httpx.Response(200, text=MOCK_CATALOGUE)
    )

    async with GermanyBwConnector() as conn:
        chunk = await conn.fetch_observations(
            "germany_bw:00076",
            start=datetime(2026, 1, 1, tzinfo=UTC),
            end=datetime(2026, 1, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    obs = chunk.observations[0]
    assert obs.discharge_m3s == pytest.approx(0.42)
    # 06:00 MEZ (UTC+1) -> 05:00 UTC.
    assert obs.timestamp == datetime(2026, 1, 2, 5, 0, tzinfo=UTC)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_outside_window_empty():
    """Observations whose timestamp is outside [start, end] are dropped."""
    respx.get(_CATALOGUE_URL).mock(
        return_value=httpx.Response(200, text=MOCK_CATALOGUE)
    )

    async with GermanyBwConnector() as conn:
        chunk = await conn.fetch_observations(
            "germany_bw:00435",
            start=datetime(2020, 1, 1, tzinfo=UTC),
            end=datetime(2020, 1, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_resolves_when_not_cached():
    """fetch_observations fetches the catalogue when the row isn't cached."""
    respx.get(_CATALOGUE_URL).mock(
        return_value=httpx.Response(200, text=MOCK_CATALOGUE)
    )

    # Fresh connector, no prior fetch_stations call.
    async with GermanyBwConnector() as conn:
        chunk = await conn.fetch_observations(
            "germany_bw:00435",
            start=datetime(2026, 6, 1, tzinfo=UTC),
            end=datetime(2026, 6, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert "00435" in conn._catalogue


@pytest.mark.asyncio
@respx.mock
async def test_resolve_row_unknown_station_raises():
    """An unknown station id raises DataFormatError after a catalogue fetch."""
    from csfs.core.exceptions import DataFormatError

    respx.get(_CATALOGUE_URL).mock(
        return_value=httpx.Response(200, text=MOCK_CATALOGUE)
    )

    async with GermanyBwConnector() as conn:
        with pytest.raises(DataFormatError, match="No HVZ discharge station"):
            await conn.fetch_observations(
                "germany_bw:99999",
                start=datetime(2026, 6, 1, tzinfo=UTC),
                end=datetime(2026, 6, 3, tzinfo=UTC),
            )


@pytest.mark.asyncio
@respx.mock
async def test_missing_peg_db_raises():
    """A catalogue body without PEG_DB raises DataFormatError."""
    from csfs.core.exceptions import DataFormatError

    respx.get(_CATALOGUE_URL).mock(
        return_value=httpx.Response(200, text="// nothing here\n")
    )

    async with GermanyBwConnector() as conn:
        with pytest.raises(DataFormatError, match="PEG_DB array not found"):
            await conn.fetch_stations()


def test_registration():
    """The connector is registered under its slug."""
    from csfs.core.registry import discover, get_connector

    discover()
    cls = get_connector("germany_bw")
    assert cls is GermanyBwConnector
    assert cls.slug == "germany_bw"
