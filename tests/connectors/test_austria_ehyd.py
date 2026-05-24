"""Tests for the eHYD (Austria) connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.austria_ehyd import AustriaEhydConnector

MOCK_STATIONS_RESPONSE = [
    {
        "hzbnr": 200287,
        "messstellenname": "Innsbruck / Sill",
        "gewaesser": "Sill",
        "flaeche_km2": 854.3,
        "breite": 47.2622,
        "laenge": 11.3928,
    },
    {
        "hzbnr": 207407,
        "messstellenname": "Wien / Donaukanal",
        "gewaesser": "Donaukanal",
        "flaeche_km2": None,
        "breite": 48.2116,
        "laenge": 16.3808,
    },
    {
        "hzbnr": 210062,
        "messstellenname": "Salzburg / Salzach",
        "gewaesser": "Salzach",
        "flaeche_km2": 4425.0,
        "breite": 47.8,
        "laenge": 13.05,
    },
]

MOCK_OBSERVATIONS_JSON = [
    {"datum": "2024-06-01T00:00:00", "wert": 23.5},
    {"datum": "2024-06-02T00:00:00", "wert": 25.1},
    {"datum": "2024-06-03T00:00:00", "wert": None},
    {"datum": "2024-06-04T00:00:00", "wert": 21.8},
    {"datum": "2024-07-01T00:00:00", "wert": 30.0},
]

MOCK_OBSERVATIONS_CSV = """\
Datum;Wert
01.06.2024;23,5
02.06.2024;25,1
03.06.2024;Lücke
04.06.2024;21,8
01.07.2024;30,0
"""


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_returns_all():
    """All stations from the JSON response are parsed."""
    respx.get("https://ehyd.gv.at/eHYD/api/OGDAbflussMessstellenListe").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE),
    )

    async with AustriaEhydConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 3
    native_ids = {s.native_id for s in stations}
    assert native_ids == {"200287", "207407", "210062"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fields():
    """Station fields are correctly mapped from eHYD JSON."""
    respx.get("https://ehyd.gv.at/eHYD/api/OGDAbflussMessstellenListe").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE),
    )

    async with AustriaEhydConnector() as conn:
        stations = await conn.fetch_stations()

    sill = next(s for s in stations if s.native_id == "200287")
    assert sill.id == "austria_ehyd:200287"
    assert sill.provider == "austria_ehyd"
    assert sill.name == "Innsbruck / Sill"
    assert sill.country_code == "AT"
    assert sill.river == "Sill"
    assert sill.latitude == pytest.approx(47.2622)
    assert sill.longitude == pytest.approx(11.3928)
    assert sill.catchment_area_km2 == pytest.approx(854.3)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_null_catchment():
    """Stations with null catchment area have catchment_area_km2 = None."""
    respx.get("https://ehyd.gv.at/eHYD/api/OGDAbflussMessstellenListe").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE),
    )

    async with AustriaEhydConnector() as conn:
        stations = await conn.fetch_stations()

    wien = next(s for s in stations if s.native_id == "207407")
    assert wien.catchment_area_km2 is None


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty station list returns no stations."""
    respx.get("https://ehyd.gv.at/eHYD/api/OGDAbflussMessstellenListe").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with AustriaEhydConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_primary_endpoint():
    """Observations are parsed from the primary JSON API."""
    respx.get("https://ehyd.gv.at/eHYD/api/OGDAbflussTagesmittel").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_JSON),
    )

    async with AustriaEhydConnector() as conn:
        chunk = await conn.fetch_observations(
            "austria_ehyd:200287",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 30),
        )

    assert chunk.provider == "austria_ehyd"
    assert chunk.station_id == "austria_ehyd:200287"
    # Only June dates should be included (4 of 5 records)
    assert len(chunk.observations) == 4

    # First observation
    assert chunk.observations[0].discharge_m3s == pytest.approx(23.5)
    assert chunk.observations[0].quality.value == "raw"

    # Third observation — None value -> MISSING
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_fallback_csv():
    """When primary endpoint fails, fallback CSV endpoint is used."""
    respx.get("https://ehyd.gv.at/eHYD/api/OGDAbflussTagesmittel").mock(
        return_value=httpx.Response(500),
    )
    respx.get("https://ehyd.gv.at/eHYD/MessstellenExtra662/QDaily/200287/download").mock(
        return_value=httpx.Response(200, text=MOCK_OBSERVATIONS_CSV),
    )

    async with AustriaEhydConnector() as conn:
        chunk = await conn.fetch_observations(
            "austria_ehyd:200287",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 30),
        )

    # Only June dates from CSV (3 data rows + 1 missing)
    assert len(chunk.observations) == 4
    assert chunk.observations[0].discharge_m3s == pytest.approx(23.5)
    # "Lücke" should map to MISSING
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty_json():
    """Empty JSON array returns zero observations."""
    respx.get("https://ehyd.gv.at/eHYD/api/OGDAbflussTagesmittel").mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with AustriaEhydConnector() as conn:
        chunk = await conn.fetch_observations(
            "austria_ehyd:200287",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 30),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_date_filtering():
    """Only observations within the requested date range are returned."""
    respx.get("https://ehyd.gv.at/eHYD/api/OGDAbflussTagesmittel").mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_JSON),
    )

    async with AustriaEhydConnector() as conn:
        chunk = await conn.fetch_observations(
            "austria_ehyd:200287",
            start=datetime(2024, 6, 2),
            end=datetime(2024, 6, 3),
        )

    # Only June 2nd and 3rd should be included
    assert len(chunk.observations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_entries_without_hzbnr():
    """Entries missing hzbnr are silently skipped."""
    data = [
        {"messstellenname": "No ID", "breite": 47.0, "laenge": 13.0},
        {"hzbnr": "", "messstellenname": "Empty ID", "breite": 47.0, "laenge": 13.0},
        MOCK_STATIONS_RESPONSE[0],
    ]
    respx.get("https://ehyd.gv.at/eHYD/api/OGDAbflussMessstellenListe").mock(
        return_value=httpx.Response(200, json=data),
    )

    async with AustriaEhydConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "200287"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_both_endpoints_fail():
    """When both primary and fallback endpoints fail, ConnectorError is raised."""
    from csfs.core.exceptions import ConnectorError

    respx.get("https://ehyd.gv.at/eHYD/api/OGDAbflussTagesmittel").mock(
        return_value=httpx.Response(500),
    )
    respx.get("https://ehyd.gv.at/eHYD/MessstellenExtra662/QDaily/200287/download").mock(
        return_value=httpx.Response(500),
    )

    async with AustriaEhydConnector() as conn:
        with pytest.raises(ConnectorError):
            await conn.fetch_observations(
                "austria_ehyd:200287",
                start=datetime(2024, 6, 1),
                end=datetime(2024, 6, 30),
            )


@pytest.mark.asyncio
@respx.mock
async def test_connector_registration():
    """The connector is registered with the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("austria_ehyd")
    assert cls is AustriaEhydConnector


@pytest.mark.asyncio
@respx.mock
async def test_csv_parsing_comma_decimal_separator():
    """CSV values with comma as decimal separator are handled."""
    csv_text = "Datum;Wert\n01.06.2024;123,45\n02.06.2024;678,90\n"
    respx.get("https://ehyd.gv.at/eHYD/api/OGDAbflussTagesmittel").mock(
        return_value=httpx.Response(500),
    )
    respx.get("https://ehyd.gv.at/eHYD/MessstellenExtra662/QDaily/200287/download").mock(
        return_value=httpx.Response(200, text=csv_text),
    )

    async with AustriaEhydConnector() as conn:
        chunk = await conn.fetch_observations(
            "austria_ehyd:200287",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 30),
        )

    assert chunk.observations[0].discharge_m3s == pytest.approx(123.45)
    assert chunk.observations[1].discharge_m3s == pytest.approx(678.90)
