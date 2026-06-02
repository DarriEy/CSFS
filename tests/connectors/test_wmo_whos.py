"""Tests for the WMO WHOS (GEO DAB) connector with mocked HTTP responses.

WHOS is queried through two REST APIs under
``.../token/<token>/view/<view>/``:
  * ``om-api/features``            -- station discovery (discharge-filtered)
  * ``timeseries-api/timeseries``  -- OM-JSON time series / discharge data
"""

import re
from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.wmo_whos import (
    WHOSConnector,
    WHOSPlataConnector,
    _DEFAULT_TOKEN,
)
from csfs.core.registry import discover, get_connector

# --- om-api/features: a discharge-filtered station page ---------------------
MOCK_FEATURES_RESPONSE = {
    "id": "observation collection",
    "completed": True,
    "resumptionToken": None,
    "results": [
        {
            "shape": {"coordinates": [-65.3478, -10.7931, 147], "type": "Point"},
            "parameter": [
                {"name": "country", "value": "Brazil"},
                {"name": "source", "value": "Brazil, ANA"},
                {"name": "sourceId", "value": "brazil-ana"},
                {"name": "identifier", "value": "urn:ANA:15250001"},
            ],
            "name": "UHE JIRAU GUAJARA-MIRIM",
            "id": "00D31B846FBC44355814E23A6BF4150962412D37",
            "relatedParty": [],
        },
        {
            "shape": {"coordinates": [-50.705, -15.536], "type": "Point"},
            "parameter": [
                {"name": "country", "value": "Brazil"},
                {"name": "sourceId", "value": "brazil-ana"},
            ],
            "name": "TRAVESSAO",
            "id": "04F33DAD465F7BD2F923BF7289D01B6989870531",
            "relatedParty": [],
        },
        {
            # No coordinates -> must be skipped.
            "shape": {"coordinates": [], "type": "Point"},
            "parameter": [],
            "name": "BROKEN",
            "id": "DEADBEEF",
        },
        {
            # No id -> must be skipped.
            "shape": {"coordinates": [1.0, 2.0], "type": "Point"},
            "parameter": [],
            "name": "NO_ID",
        },
    ],
}

# --- timeseries-api/timeseries: OM-JSON members -----------------------------
# Two discharge members (native "Vazao" + canonical "Flux, discharge") carry
# identical points -> must de-duplicate. A rainfall member (mm) and a level
# member (cm) must be ignored. A -9999 point maps to MISSING.
MOCK_TIMESERIES_RESPONSE = {
    "id": "observation collection",
    "member": [
        {
            "observedProperty": {"href": "abc", "title": "Vazao (brazil-ana)"},
            "result": {
                "defaultPointMetadata": {"uom": "Cubic metre per second"},
                "points": [
                    {"time": {"instant": "2024-01-01T03:00:00Z"}, "value": 42.97},
                    {"time": {"instant": "2024-01-01T04:00:00Z"}, "value": 42.82},
                    {"time": {"instant": "2024-01-01T05:00:00Z"}, "value": -9999},
                ],
            },
        },
        {
            "observedProperty": {"href": "def", "title": "Flux, discharge"},
            "result": {
                "defaultPointMetadata": {"uom": "Cubic metre per second"},
                "points": [
                    {"time": {"instant": "2024-01-01T03:00:00Z"}, "value": 42.97},
                    {"time": {"instant": "2024-01-01T04:00:00Z"}, "value": 42.82},
                ],
            },
        },
        {
            "observedProperty": {"href": "ghi", "title": "Chuva (brazil-ana)"},
            "result": {
                "defaultPointMetadata": {"uom": "millimeter"},
                "points": [{"time": {"instant": "2024-01-01T03:00:00Z"}, "value": 0}],
            },
        },
        {
            "observedProperty": {"href": "jkl", "title": "Level"},
            "result": {
                "defaultPointMetadata": {"uom": "Centimetre"},
                "points": [{"time": {"instant": "2024-01-01T03:00:00Z"}, "value": 20}],
            },
        },
    ],
}

_FEATURES_RE = re.compile(r".*/om-api/features.*")
_TIMESERIES_RE = re.compile(r".*/timeseries-api/timeseries.*")


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_features():
    """Discharge features become Stations; malformed records are skipped."""
    respx.get(url__regex=_FEATURES_RE).mock(
        return_value=httpx.Response(200, json=MOCK_FEATURES_RESPONSE)
    )

    async with WHOSConnector(config={"countries": ["BRA"]}) as conn:
        stations = await conn.fetch_stations()

    # Two valid records; the no-coords and no-id records are dropped.
    assert len(stations) == 2
    by_id = {s.native_id: s for s in stations}

    s1 = by_id["00D31B846FBC44355814E23A6BF4150962412D37"]
    assert s1.id == "wmo_whos:00D31B846FBC44355814E23A6BF4150962412D37"
    assert s1.provider == "wmo_whos"
    assert s1.name == "UHE JIRAU GUAJARA-MIRIM"
    assert s1.latitude == pytest.approx(-10.7931)
    assert s1.longitude == pytest.approx(-65.3478)
    assert s1.country_code == "Brazil"

    s2 = by_id["04F33DAD465F7BD2F923BF7289D01B6989870531"]
    assert s2.name == "TRAVESSAO"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_uses_token_and_view_in_url():
    """The request path carries the default public token and the configured view."""
    route = respx.get(url__regex=_FEATURES_RE).mock(
        return_value=httpx.Response(200, json={"results": [], "completed": True})
    )

    async with WHOSConnector(config={"view": "whos", "countries": ["BRA"]}) as conn:
        await conn.fetch_stations()

    url = str(route.calls[0].request.url)
    assert f"/token/{_DEFAULT_TOKEN}/view/whos/" in url
    assert "om-api/features" in url
    assert "observedProperty=Discharge" in url
    assert "ontology=whos" in url
    assert "country=BRA" in url


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_discharge_m3s():
    """Discharge series parsed into m3/s; dups merged, non-discharge ignored."""
    respx.get(url__regex=_TIMESERIES_RE).mock(
        return_value=httpx.Response(200, json=MOCK_TIMESERIES_RESPONSE)
    )

    async with WHOSConnector() as conn:
        chunk = await conn.fetch_observations(
            "wmo_whos:04F33DAD465F7BD2F923BF7289D01B6989870531",
            start=datetime(2024, 1, 1, tzinfo=UTC),
            end=datetime(2024, 1, 2, tzinfo=UTC),
        )

    assert chunk.provider == "wmo_whos"
    # Three distinct timestamps from the two discharge members (deduped),
    # rainfall/level members excluded.
    assert len(chunk.observations) == 3

    obs = {o.timestamp: o for o in chunk.observations}
    t0 = datetime(2024, 1, 1, 3, 0, tzinfo=UTC)
    t1 = datetime(2024, 1, 1, 4, 0, tzinfo=UTC)
    t2 = datetime(2024, 1, 1, 5, 0, tzinfo=UTC)

    assert obs[t0].discharge_m3s == pytest.approx(42.97)  # in m3/s, no conversion
    assert obs[t0].quality.value == "raw"
    assert obs[t1].discharge_m3s == pytest.approx(42.82)

    # -9999 no-data sentinel -> MISSING
    assert obs[t2].discharge_m3s is None
    assert obs[t2].quality.value == "missing"

    # Observations are time-ordered.
    timestamps = [o.timestamp for o in chunk.observations]
    assert timestamps == sorted(timestamps)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_queries_by_monitoring_point():
    """The native id is passed as the monitoringPoint query parameter."""
    route = respx.get(url__regex=_TIMESERIES_RE).mock(
        return_value=httpx.Response(200, json={"id": "x", "member": []})
    )

    async with WHOSConnector() as conn:
        chunk = await conn.fetch_observations(
            "wmo_whos:FEATUREHASH123",
            start=datetime(2024, 1, 1, tzinfo=UTC),
            end=datetime(2024, 1, 2, tzinfo=UTC),
        )

    assert chunk.observations == []
    url = str(route.calls[0].request.url)
    assert "monitoringPoint=FEATUREHASH123" in url
    assert "beginPosition=2024-01-01T00%3A00%3A00Z" in url


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty_and_non_list_member():
    """A missing/non-list member field yields zero observations."""
    respx.get(url__regex=_TIMESERIES_RE).mock(
        return_value=httpx.Response(200, json={"id": "x"})
    )

    async with WHOSConnector() as conn:
        chunk = await conn.fetch_observations(
            "wmo_whos:FOO",
            start=datetime(2024, 1, 1, tzinfo=UTC),
            end=datetime(2024, 1, 2, tzinfo=UTC),
        )

    assert chunk.observations == []


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_raises_connectorerror_on_http_error():
    """A server error surfaces as a ConnectorError."""
    from csfs.core.exceptions import ConnectorError

    respx.get(url__regex=_FEATURES_RE).mock(return_value=httpx.Response(500))

    async with WHOSConnector(config={"countries": ["BRA"]}) as conn:
        with pytest.raises(ConnectorError):
            await conn.fetch_stations()


def test_plata_subclass_uses_plata_view():
    """The plata subclass defaults to the whos-plata view."""
    conn = WHOSPlataConnector()
    assert conn._view == "whos-plata"
    assert conn.slug == "wmo_whos_plata"
    assert conn.country_codes == ["AR", "BO", "BR", "PY", "UY"]


def test_registration():
    """All three WHOS slugs are discoverable via the registry."""
    discover()
    for slug, expected in (
        ("wmo_whos", WHOSConnector),
        ("wmo_whos_plata", WHOSPlataConnector),
    ):
        cls = get_connector(slug)
        assert cls is expected
        assert cls.slug == slug

    # The africa slug must at least be registered (it backs a cron tier).
    africa = get_connector("wmo_whos_africa")
    assert africa.slug == "wmo_whos_africa"
    assert "global" in africa.country_codes
