"""Tests for the Netherlands Rijkswaterstaat WaterWebservices DD-API connector."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.netherlands_rws import (
    NetherlandsRwsConnector,
    _status_to_quality,
)
from csfs.core.models import QualityFlag

BASE = "https://ddapi20-waterwebservices.rijkswaterstaat.nl"
CATALOGUE_URL = f"{BASE}/METADATASERVICES/OphalenCatalogus"
OBS_URL = f"{BASE}/ONLINEWAARNEMINGENSERVICES/OphalenWaarnemingen"

# -- Catalogue fixture -------------------------------------------------
# MessageID 138 = Q/OW/m3/s (discharge); 200 = some other parameter.
MOCK_CATALOGUE = {
    "Succesvol": True,
    "AquoMetadataLijst": [
        {
            "AquoMetadata_MessageID": 138,
            "Compartiment": {"Code": "OW"},
            "Grootheid": {"Code": "Q"},
            "Eenheid": {"Code": "m3/s"},
        },
        {
            "AquoMetadata_MessageID": 137,
            "Compartiment": {"Code": "OW"},
            "Grootheid": {"Code": "Q"},
            "Eenheid": {"Code": "m3/d"},  # discharge but wrong unit
        },
        {
            "AquoMetadata_MessageID": 200,
            "Compartiment": {"Code": "OW"},
            "Grootheid": {"Code": "WATHTE"},
            "Eenheid": {"Code": "cm"},  # water level, not discharge
        },
    ],
    "AquoMetadataLocatieLijst": [
        {"AquoMetaData_MessageID": 138, "Locatie_MessageID": 1001},  # Lobith Q
        {"AquoMetaData_MessageID": 138, "Locatie_MessageID": 1002},  # Olst Q
        {"AquoMetaData_MessageID": 200, "Locatie_MessageID": 1003},  # level only
        {"AquoMetaData_MessageID": 137, "Locatie_MessageID": 1004},  # m3/d only
    ],
    "LocatieLijst": [
        {
            "Locatie_MessageID": 1001,
            "Code": "lobith.bovenrijn.tolkamer",
            "Naam": "Lobith, Bovenrijn, Tolkamer",
            "Lat": 51.8495, "Lon": 6.1024, "Coordinatenstelsel": "ETRS89",
        },
        {
            "Locatie_MessageID": 1002,
            "Code": "olst", "Naam": "Olst",
            "Lat": 52.342, "Lon": 6.1045, "Coordinatenstelsel": "ETRS89",
        },
        {
            "Locatie_MessageID": 1003,
            "Code": "levelonly", "Naam": "Level Only",
            "Lat": 52.0, "Lon": 5.0, "Coordinatenstelsel": "ETRS89",
        },
        {
            "Locatie_MessageID": 1004,
            "Code": "m3donly", "Naam": "Daily Only",
            "Lat": 52.1, "Lon": 5.1, "Coordinatenstelsel": "ETRS89",
        },
    ],
}


def _measurement(ts: str, value, status="Ongecontroleerd", hoogte="0"):
    return {
        "Tijdstip": ts,
        "Meetwaarde": {"Waarde_Numeriek": value},
        "WaarnemingMetadata": {
            "Statuswaarde": status, "Bemonsteringshoogte": hoogte,
        },
    }


# Two groups with identical timestamps (differ by sampling height) -> dedup.
MOCK_OBSERVATIONS = {
    "Succesvol": True,
    "WaarnemingenLijst": [
        {
            "Locatie": {"Code": "lobith.bovenrijn.tolkamer"},
            "MetingenLijst": [
                _measurement("2026-05-01T00:00:00.000+02:00", 1200.5),
                _measurement("2026-05-01T00:10:00.000+02:00", 1199.75),
                _measurement(
                    "2026-05-01T00:20:00.000+02:00", 999999999.0,
                ),  # missing sentinel
            ],
        },
        {
            "Locatie": {"Code": "lobith.bovenrijn.tolkamer"},
            "MetingenLijst": [
                _measurement(
                    "2026-05-01T00:00:00.000+02:00", 1218.0, hoogte="-999999999",
                ),
            ],
        },
    ],
}


# ======================================================================
# Station tests
# ======================================================================


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_resolves_discharge_only():
    """Only Q/m3/s stations are returned; level-only and m3/d are excluded."""
    respx.post(CATALOGUE_URL).mock(
        return_value=httpx.Response(200, json=MOCK_CATALOGUE),
    )

    async with NetherlandsRwsConnector() as conn:
        stations = await conn.fetch_stations()

    assert {s.native_id for s in stations} == {"lobith.bovenrijn.tolkamer", "olst"}

    lobith = next(s for s in stations if s.native_id == "lobith.bovenrijn.tolkamer")
    assert lobith.id == "netherlands_rws:lobith.bovenrijn.tolkamer"
    assert lobith.provider == "netherlands_rws"
    assert lobith.name == "Lobith, Bovenrijn, Tolkamer"
    assert lobith.country_code == "NL"
    assert lobith.latitude == pytest.approx(51.8495)
    assert lobith.longitude == pytest.approx(6.1024)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_empty_catalogue():
    """A catalogue with no Q/m3/s metadata returns no stations."""
    respx.post(CATALOGUE_URL).mock(
        return_value=httpx.Response(200, json={
            "Succesvol": True,
            "AquoMetadataLijst": [],
            "AquoMetadataLocatieLijst": [],
            "LocatieLijst": [],
        }),
    )

    async with NetherlandsRwsConnector() as conn:
        stations = await conn.fetch_stations()

    assert stations == []


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_http_error_raises():
    """An HTTP error on the catalogue raises ConnectorError."""
    from csfs.core.exceptions import ConnectorError

    respx.post(CATALOGUE_URL).mock(return_value=httpx.Response(500))

    async with NetherlandsRwsConnector() as conn:
        with pytest.raises(ConnectorError, match="Failed to fetch catalogue"):
            await conn.fetch_stations()


# ======================================================================
# Observation tests
# ======================================================================


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_and_dedups():
    """Measurements are parsed, deduped by timestamp, and converted to UTC."""
    route = respx.post(OBS_URL).mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS),
    )

    async with NetherlandsRwsConnector() as conn:
        chunk = await conn.fetch_observations(
            "netherlands_rws:lobith.bovenrijn.tolkamer",
            start=datetime(2026, 5, 1, tzinfo=UTC),
            end=datetime(2026, 5, 1, 1, tzinfo=UTC),
        )

    assert route.called
    assert chunk.provider == "netherlands_rws"
    # 3 distinct timestamps despite 4 measurements across 2 groups.
    assert len(chunk.observations) == 3

    # First group wins on duplicate timestamp; +02:00 -> UTC.
    first = chunk.observations[0]
    assert first.timestamp == datetime(2026, 4, 30, 22, 0, tzinfo=UTC)
    assert first.discharge_m3s == pytest.approx(1200.5)
    assert first.quality == QualityFlag.RAW

    # Missing sentinel -> None / MISSING.
    last = chunk.observations[2]
    assert last.discharge_m3s is None
    assert last.quality == QualityFlag.MISSING


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_request_body():
    """The OphalenWaarnemingen body carries the Q/OW/m3/s identity + code."""
    route = respx.post(OBS_URL).mock(
        return_value=httpx.Response(200, json={
            "Succesvol": True, "WaarnemingenLijst": [],
        }),
    )

    async with NetherlandsRwsConnector() as conn:
        await conn.fetch_observations(
            "netherlands_rws:olst",
            start=datetime(2026, 5, 1, tzinfo=UTC),
            end=datetime(2026, 5, 1, 1, tzinfo=UTC),
        )

    import json
    body = json.loads(route.calls.last.request.content)
    aquo = body["AquoPlusWaarnemingMetadata"]["aquoMetadata"]
    assert aquo["Grootheid"]["Code"] == "Q"
    assert aquo["Compartiment"]["Code"] == "OW"
    assert aquo["Eenheid"]["Code"] == "m3/s"
    assert body["Locatie"]["Code"] == "olst"
    assert body["Periode"]["Begindatumtijd"] == "2026-05-01T00:00:00.000+00:00"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_unsuccessful_raises():
    """A Succesvol=false response raises ConnectorError with the Foutmelding."""
    from csfs.core.exceptions import ConnectorError

    respx.post(OBS_URL).mock(
        return_value=httpx.Response(200, json={
            "Succesvol": False, "Foutmelding": "Geen data",
        }),
    )

    async with NetherlandsRwsConnector() as conn:
        with pytest.raises(ConnectorError, match="Geen data"):
            await conn.fetch_observations(
                "netherlands_rws:olst",
                start=datetime(2026, 5, 1, tzinfo=UTC),
                end=datetime(2026, 5, 1, 1, tzinfo=UTC),
            )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_chunks_long_range():
    """Ranges longer than the max window are split into multiple requests."""
    route = respx.post(OBS_URL).mock(
        return_value=httpx.Response(200, json={
            "Succesvol": True, "WaarnemingenLijst": [],
        }),
    )

    async with NetherlandsRwsConnector() as conn:
        await conn.fetch_observations(
            "netherlands_rws:olst",
            start=datetime(2026, 1, 1, tzinfo=UTC),
            end=datetime(2026, 4, 1, tzinfo=UTC),  # ~90 days -> 4 windows of 28d
        )

    assert route.call_count == 4


# ======================================================================
# Unit helpers
# ======================================================================


def test_status_to_quality():
    assert _status_to_quality("Definitief") == QualityFlag.GOOD
    assert _status_to_quality("Gecontroleerd") == QualityFlag.GOOD
    assert _status_to_quality("Ongecontroleerd") == QualityFlag.RAW
    assert _status_to_quality(None) == QualityFlag.RAW
    assert _status_to_quality("iets anders") == QualityFlag.RAW


def test_windows_single_for_short_range():
    start = datetime(2026, 5, 1, tzinfo=UTC)
    end = datetime(2026, 5, 2, tzinfo=UTC)
    assert NetherlandsRwsConnector._windows(start, end) == [(start, end)]


def test_fmt_dt_is_utc_iso():
    from datetime import timedelta, timezone
    dt = datetime(2026, 5, 1, 12, 0, tzinfo=timezone(timedelta(hours=2)))
    # 12:00 +02:00 == 10:00 UTC
    assert NetherlandsRwsConnector._fmt_dt(dt) == "2026-05-01T10:00:00.000+00:00"


def test_connector_is_registered():
    from csfs.core.registry import get_connector

    assert get_connector("netherlands_rws") is NetherlandsRwsConnector
