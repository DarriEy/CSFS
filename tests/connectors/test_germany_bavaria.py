"""Tests for the GKD Bayern (Germany) connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.germany_bavaria import GermanyBavariaConnector
from csfs.core.registry import get_connector

_CATALOGUE_URL = "https://www.gkd.bayern.de/de/fluesse/abfluss/tabellen"

# A trimmed catalogue table: two real-shaped rows + the header row (which must
# NOT be matched because it has no station detail link).
MOCK_CATALOGUE_HTML = """
<table id="abfluss">
<tr><th class="left" data-sorter="text">Messstelle</th><th class="left">Gew&auml;sser</th><th class="left">Lkr.</th><th class="center sorter-numberSorter">Abfluss [m³/s]</th></tr>
<tr class="row2" data-messnetze="ap dp"><td class="left" data-text="Achsheim"><ul class="linkliste"><li class="intern"><a href="https://www.gkd.bayern.de/de/fluesse/abfluss/kelheim/achsheim-11944004/messwerte?method=tabellen">Achsheim</a></li></ul></td><td class="left" data-text="Schmutter11944004">Schmutter</td><td class="left" data-text="A11944004">A</td><td class="left" data-text="02.06.2026 07:00">02.06.2026 07:00 Uhr</td><td class="center sorter-numberSorter" data-text="1,63">1,63</td></tr>
<tr class="row" data-messnetze="ap dp"><td class="left" data-text="Adlerh&uuml;tte"><ul class="linkliste"><li class="intern"><a href="https://www.gkd.bayern.de/de/fluesse/abfluss/elbe/adlerhuette-24118000/messwerte?method=tabellen">Adlerh&uuml;tte</a></li></ul></td><td class="left" data-text="Koserbach24118000">Koserbach</td><td class="left" data-text="KU24118000">KU</td><td class="left" data-text="02.06.2026 07:00">02.06.2026 07:00 Uhr</td><td class="center sorter-numberSorter" data-text="0,306">0,306</td></tr>
</table>
"""

# A trimmed per-station measurement table (Datum / Abfluss [m³/s]).
MOCK_OBS_HTML = """
<table id="messwerte">
<tr><th data-sorter="shortDate">Datum</th><th class="center sorter-numberSorter">Abfluss [m³/s]</th></tr>
<tr><td >01.06.2026 00:00 Uhr</td><td  class="center">1,63</td></tr>
<tr><td >01.06.2026 00:15 Uhr</td><td  class="center">1,750</td></tr>
<tr><td >01.06.2026 00:30 Uhr</td><td  class="center">-</td></tr>
</table>
"""


def _obs_url(path: str) -> str:
    return f"https://www.gkd.bayern.de{path}/messwerte"


def test_registration():
    """The connector is registered under its slug."""
    assert get_connector("germany_bavaria") is GermanyBavariaConnector


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_catalogue():
    """The catalogue table is parsed into Station objects (header row skipped)."""
    respx.get(_CATALOGUE_URL).mock(
        return_value=httpx.Response(200, text=MOCK_CATALOGUE_HTML)
    )

    async with GermanyBavariaConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    native_ids = {s.native_id for s in stations}
    assert native_ids == {"11944004", "24118000"}

    achsheim = next(s for s in stations if s.native_id == "11944004")
    assert achsheim.id == "germany_bavaria:11944004"
    assert achsheim.provider == "germany_bavaria"
    assert achsheim.country_code == "DE"
    assert achsheim.name == "Achsheim"
    assert achsheim.river == "Schmutter"

    # HTML entities in the name are decoded.
    adler = next(s for s in stations if s.native_id == "24118000")
    assert adler.name == "Adlerhütte"
    assert adler.river == "Koserbach"

    # The region-qualified detail path is cached for observation fetches.
    assert conn._station_paths["11944004"] == (
        "/de/fluesse/abfluss/kelheim/achsheim-11944004"
    )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_table():
    """Measurements parse into m³/s observations (no unit conversion)."""
    conn = GermanyBavariaConnector()
    # Pre-seed the path cache to avoid a catalogue round-trip.
    conn._station_paths["11944004"] = (
        "/de/fluesse/abfluss/kelheim/achsheim-11944004"
    )

    respx.get(
        _obs_url("/de/fluesse/abfluss/kelheim/achsheim-11944004")
    ).mock(return_value=httpx.Response(200, text=MOCK_OBS_HTML))

    async with conn:
        chunk = await conn.fetch_observations(
            "germany_bavaria:11944004",
            start=datetime(2026, 5, 1, tzinfo=UTC),
            end=datetime(2026, 6, 2, tzinfo=UTC),
        )

    assert chunk.provider == "germany_bavaria"
    assert chunk.station_id == "germany_bavaria:11944004"
    assert len(chunk.observations) == 3

    # German decimal comma -> float, already in m³/s.
    assert chunk.observations[0].discharge_m3s == pytest.approx(1.63)
    assert chunk.observations[0].quality.value == "raw"
    assert chunk.observations[1].discharge_m3s == pytest.approx(1.75)

    # "-" (no value) becomes None / MISSING.
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_filters_out_of_range():
    """Rows outside the requested window are dropped."""
    conn = GermanyBavariaConnector()
    conn._station_paths["11944004"] = (
        "/de/fluesse/abfluss/kelheim/achsheim-11944004"
    )

    respx.get(
        _obs_url("/de/fluesse/abfluss/kelheim/achsheim-11944004")
    ).mock(return_value=httpx.Response(200, text=MOCK_OBS_HTML))

    async with conn:
        # Mock rows are German local time (CEST = UTC+2 in June); 00:15 local
        # is 22:15 UTC the previous day.
        chunk = await conn.fetch_observations(
            "germany_bavaria:11944004",
            start=datetime(2026, 5, 31, 22, 15, tzinfo=UTC),
            end=datetime(2026, 5, 31, 22, 20, tzinfo=UTC),
        )

    # Only the 00:15 (local) row falls inside the narrow window.
    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(1.75)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_resolves_path_via_catalogue():
    """When the path isn't cached, the catalogue is fetched to resolve it."""
    respx.get(_CATALOGUE_URL).mock(
        return_value=httpx.Response(200, text=MOCK_CATALOGUE_HTML)
    )
    respx.get(
        _obs_url("/de/fluesse/abfluss/kelheim/achsheim-11944004")
    ).mock(return_value=httpx.Response(200, text=MOCK_OBS_HTML))

    async with GermanyBavariaConnector() as conn:
        chunk = await conn.fetch_observations(
            "germany_bavaria:11944004",
            start=datetime(2026, 5, 1, tzinfo=UTC),
            end=datetime(2026, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 3
    assert conn._station_paths["11944004"] == (
        "/de/fluesse/abfluss/kelheim/achsheim-11944004"
    )


@pytest.mark.asyncio
@respx.mock
async def test_resolve_path_unknown_station_raises():
    """An unknown station id raises ConnectorError after catalogue lookup."""
    from csfs.core.exceptions import ConnectorError

    respx.get(_CATALOGUE_URL).mock(
        return_value=httpx.Response(200, text=MOCK_CATALOGUE_HTML)
    )

    async with GermanyBavariaConnector() as conn:
        with pytest.raises(ConnectorError, match="Unknown GKD Bayern station"):
            await conn.fetch_observations(
                "germany_bavaria:99999999",
                start=datetime(2026, 5, 1, tzinfo=UTC),
                end=datetime(2026, 6, 2, tzinfo=UTC),
            )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty catalogue page yields no stations."""
    respx.get(_CATALOGUE_URL).mock(
        return_value=httpx.Response(200, text="<table></table>")
    )

    async with GermanyBavariaConnector() as conn:
        stations = await conn.fetch_stations()

    assert stations == []
