"""Tests for the Greece OpenHI (Enhydris) connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.greece_openhi import GreeceOpenhiConnector

BASE_URL = "https://system.openhi.net"


# ----------------------------------------------------------------------
# Helpers to mock the Enhydris endpoint tree
# ----------------------------------------------------------------------

def _station(sid: int, name: str, lon: float, lat: float, tz: str = "Etc/GMT-2"):
    return {
        "id": sid,
        "name": name,
        "geom": f"SRID=4326;POINT ({lon} {lat})",
        "display_timezone": tz,
    }


def _groups(*variables_and_ids):
    """Build a timeseriesgroups response from (id, variable) pairs."""
    return {
        "count": len(variables_and_ids),
        "next": None,
        "previous": None,
        "results": [
            {"id": gid, "variable": var} for gid, var in variables_and_ids
        ],
    }


def _timeseries(ts_id: int):
    return {
        "count": 1,
        "next": None,
        "previous": None,
        "results": [{"id": ts_id, "type": "Initial"}],
    }


def _mock_two_station_api():
    """Mock a 2-station instance: 100 has discharge, 200 is stage-only."""
    respx.get(f"{BASE_URL}/api/stations/", params={"page": "1"}).mock(
        return_value=httpx.Response(200, json={
            "count": 2,
            "next": None,
            "previous": None,
            "results": [
                _station(100, "Discharge Station", 22.0, 39.0),
                _station(200, "Stage Only", 23.0, 40.0),
            ],
        }),
    )
    # Station 100: discharge (var 2) group 6 + stage (var 14) group 5.
    respx.get(f"{BASE_URL}/api/stations/100/timeseriesgroups/").mock(
        return_value=httpx.Response(200, json=_groups((6, 2), (5, 14))),
    )
    respx.get(
        f"{BASE_URL}/api/stations/100/timeseriesgroups/6/timeseries/",
    ).mock(return_value=httpx.Response(200, json=_timeseries(9001)))
    # Station 200: stage only (var 14) -> no discharge.
    respx.get(f"{BASE_URL}/api/stations/200/timeseriesgroups/").mock(
        return_value=httpx.Response(200, json=_groups((5, 14))),
    )


# Enhydris CSV data export: "timestamp,value,flags" in display-local time.
MOCK_CSV = (
    "2024-06-01 02:00,45.3,VALIDATED\n"
    "2024-06-01 02:15,44.1,\n"
    "2024-06-01 02:30,,\n"
    "2024-06-01 02:45,30.0,SUSPECT\n"
)


# ======================================================================
# Station tests
# ======================================================================


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_filters_to_discharge_only():
    """Only stations with a discharge (variable 2) group are returned."""
    _mock_two_station_api()

    async with GreeceOpenhiConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    sta = stations[0]
    assert sta.native_id == "100"
    assert sta.id == "greece_openhi:100"
    assert sta.provider == "greece_openhi"
    assert sta.country_code == "GR"
    # geom "POINT (lon lat)" -> (lat, lon)
    assert sta.latitude == pytest.approx(39.0)
    assert sta.longitude == pytest.approx(22.0)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_paginated():
    """Station list is parsed across multiple paginated responses."""
    respx.get(f"{BASE_URL}/api/stations/", params={"page": "1"}).mock(
        return_value=httpx.Response(200, json={
            "count": 2,
            "next": f"{BASE_URL}/api/stations/?page=2",
            "previous": None,
            "results": [_station(100, "One", 22.0, 39.0)],
        }),
    )
    respx.get(f"{BASE_URL}/api/stations/", params={"page": "2"}).mock(
        return_value=httpx.Response(200, json={
            "count": 2,
            "next": None,
            "previous": f"{BASE_URL}/api/stations/?page=1",
            "results": [_station(101, "Two", 23.0, 40.0)],
        }),
    )
    for sid in (100, 101):
        respx.get(
            f"{BASE_URL}/api/stations/{sid}/timeseriesgroups/",
        ).mock(return_value=httpx.Response(200, json=_groups((6, 2))))
        respx.get(
            f"{BASE_URL}/api/stations/{sid}/timeseriesgroups/6/timeseries/",
        ).mock(return_value=httpx.Response(200, json=_timeseries(9000 + sid)))

    async with GreeceOpenhiConnector() as conn:
        stations = await conn.fetch_stations()

    assert {s.native_id for s in stations} == {"100", "101"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty results list returns no stations."""
    respx.get(f"{BASE_URL}/api/stations/", params={"page": "1"}).mock(
        return_value=httpx.Response(200, json={
            "count": 0, "next": None, "previous": None, "results": [],
        }),
    )

    async with GreeceOpenhiConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_http_error_raises_connector_error():
    """HTTPStatusError on station listing raises ConnectorError."""
    from csfs.core.exceptions import ConnectorError

    respx.get(f"{BASE_URL}/api/stations/", params={"page": "1"}).mock(
        return_value=httpx.Response(500),
    )

    async with GreeceOpenhiConnector() as conn:
        with pytest.raises(ConnectorError, match="Failed to fetch station list"):
            await conn.fetch_stations()


# ======================================================================
# Observation tests
# ======================================================================


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_csv_with_tz_conversion():
    """CSV data is parsed; local timestamps convert to UTC; flags mapped."""
    route = respx.get(
        f"{BASE_URL}/api/stations/100/timeseriesgroups/6/timeseries/9001/data/",
    ).mock(return_value=httpx.Response(200, text=MOCK_CSV))

    async with GreeceOpenhiConnector() as conn:
        # Pre-seed the discharge ref so we only exercise the data path.
        conn._discharge_ref["100"] = (6, 9001, "Etc/GMT-2")
        chunk = await conn.fetch_observations(
            "greece_openhi:100",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert route.called
    assert chunk.provider == "greece_openhi"
    assert len(chunk.observations) == 4

    # Etc/GMT-2 == UTC+2, so local 02:00 -> 00:00 UTC.
    assert chunk.observations[0].timestamp == datetime(2024, 6, 1, 0, 0, tzinfo=UTC)
    assert chunk.observations[0].discharge_m3s == pytest.approx(45.3)
    assert chunk.observations[0].quality.value == "good"  # VALIDATED

    # Empty flag -> RAW
    assert chunk.observations[1].discharge_m3s == pytest.approx(44.1)
    assert chunk.observations[1].quality.value == "raw"

    # Empty value -> MISSING
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"

    # SUSPECT flag
    assert chunk.observations[3].quality.value == "suspect"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_sends_local_date_bounds():
    """Query start/end are converted to the station's local timezone."""
    route = respx.get(
        f"{BASE_URL}/api/stations/100/timeseriesgroups/6/timeseries/9001/data/",
    ).mock(return_value=httpx.Response(200, text=""))

    async with GreeceOpenhiConnector() as conn:
        conn._discharge_ref["100"] = (6, 9001, "Etc/GMT-2")
        await conn.fetch_observations(
            "greece_openhi:100",
            start=datetime(2024, 6, 1, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 1, 12, 0, tzinfo=UTC),
        )

    sent = route.calls.last.request.url.params
    # UTC 00:00 -> 02:00 local (UTC+2); UTC 12:00 -> 14:00 local.
    assert sent["start_date"] == "2024-06-01 02:00"
    assert sent["end_date"] == "2024-06-01 14:00"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_no_discharge_returns_empty():
    """A station without a discharge group yields an empty chunk."""
    respx.get(f"{BASE_URL}/api/stations/200/timeseriesgroups/").mock(
        return_value=httpx.Response(200, json=_groups((5, 14))),
    )

    async with GreeceOpenhiConnector() as conn:
        chunk = await conn.fetch_observations(
            "greece_openhi:200",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_http_error_raises_connector_error():
    """A non-success on the data endpoint raises ConnectorError."""
    from csfs.core.exceptions import ConnectorError

    respx.get(
        f"{BASE_URL}/api/stations/100/timeseriesgroups/6/timeseries/9001/data/",
    ).mock(return_value=httpx.Response(500))

    async with GreeceOpenhiConnector() as conn:
        conn._discharge_ref["100"] = (6, 9001, "Etc/GMT-2")
        with pytest.raises(ConnectorError, match="Failed to fetch observations"):
            await conn.fetch_observations(
                "greece_openhi:100",
                start=datetime(2024, 6, 1, tzinfo=UTC),
                end=datetime(2024, 6, 2, tzinfo=UTC),
            )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest_delegates():
    """fetch_latest delegates to fetch_observations (last 24h)."""
    respx.get(
        f"{BASE_URL}/api/stations/100/timeseriesgroups/6/timeseries/9001/data/",
    ).mock(return_value=httpx.Response(
        200, text="2024-06-01 02:00,45.3,VALIDATED\n",
    ))

    async with GreeceOpenhiConnector() as conn:
        conn._discharge_ref["100"] = (6, 9001, "Etc/GMT-2")
        chunk = await conn.fetch_latest("greece_openhi:100")

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(45.3)


# ======================================================================
# Discharge-ref resolution
# ======================================================================


@pytest.mark.asyncio
@respx.mock
async def test_resolve_discharge_ref_caches():
    """The discharge ref is resolved once and cached."""
    groups_route = respx.get(
        f"{BASE_URL}/api/stations/100/timeseriesgroups/",
    ).mock(return_value=httpx.Response(200, json=_groups((6, 2))))
    respx.get(
        f"{BASE_URL}/api/stations/100/timeseriesgroups/6/timeseries/",
    ).mock(return_value=httpx.Response(200, json=_timeseries(9001)))

    async with GreeceOpenhiConnector() as conn:
        ref1 = await conn._resolve_discharge_ref("100", _station(100, "X", 22.0, 39.0))
        ref2 = await conn._resolve_discharge_ref("100")

    assert ref1 == (6, 9001, "Etc/GMT-2")
    assert ref2 == ref1
    assert groups_route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_resolve_discharge_ref_no_timeseries_returns_none():
    """A discharge group with no timeseries resolves to None."""
    respx.get(f"{BASE_URL}/api/stations/300/timeseriesgroups/").mock(
        return_value=httpx.Response(200, json=_groups((7, 2))),
    )
    respx.get(
        f"{BASE_URL}/api/stations/300/timeseriesgroups/7/timeseries/",
    ).mock(return_value=httpx.Response(200, json={"results": []}))

    async with GreeceOpenhiConnector() as conn:
        ref = await conn._resolve_discharge_ref("300")

    assert ref is None


# ======================================================================
# Coordinate extraction
# ======================================================================


def test_extract_coords_wkt_with_srid():
    """SRID-prefixed WKT geom is parsed to (lat, lon)."""
    conn = GreeceOpenhiConnector()
    lat, lon = conn._extract_coords(
        {"geom": "SRID=4326;POINT (20.975265 39.15104)"},
    )
    assert lat == pytest.approx(39.15104)
    assert lon == pytest.approx(20.975265)


def test_extract_coords_geojson_point():
    """GeoJSON point coordinates are parsed to (lat, lon)."""
    conn = GreeceOpenhiConnector()
    lat, lon = conn._extract_coords(
        {"point": {"type": "Point", "coordinates": [21.74, 40.19]}},
    )
    assert lat == pytest.approx(40.19)
    assert lon == pytest.approx(21.74)


def test_extract_coords_flat_keys():
    """Flat latitude/longitude keys are parsed."""
    conn = GreeceOpenhiConnector()
    lat, lon = conn._extract_coords({"latitude": 39.5, "longitude": 22.0})
    assert lat == pytest.approx(39.5)
    assert lon == pytest.approx(22.0)


def test_extract_coords_missing_returns_none():
    """A dict with no coordinates returns (None, None)."""
    conn = GreeceOpenhiConnector()
    assert conn._extract_coords({"name": "no coords"}) == (None, None)


def test_parse_stations_skips_missing_coords():
    """Station dicts without coordinates are skipped."""
    conn = GreeceOpenhiConnector()
    stations = conn._parse_stations([
        {"id": 100, "name": "Good", "geom": "POINT (22.0 39.0)"},
        {"id": 200, "name": "No coords"},
        {"id": "", "name": "No id", "geom": "POINT (23.0 40.0)"},
    ])
    assert len(stations) == 1
    assert stations[0].native_id == "100"


# ======================================================================
# Quality flags & registry
# ======================================================================


def test_flag_to_quality_none_returns_raw():
    from csfs.connectors.greece_openhi import _flag_to_quality
    from csfs.core.models import QualityFlag

    assert _flag_to_quality(None) == QualityFlag.RAW


def test_flag_to_quality_known_flags():
    from csfs.connectors.greece_openhi import _flag_to_quality
    from csfs.core.models import QualityFlag

    assert _flag_to_quality("VALIDATED") == QualityFlag.GOOD
    assert _flag_to_quality("suspect") == QualityFlag.SUSPECT
    assert _flag_to_quality("ESTIMATED") == QualityFlag.ESTIMATED
    assert _flag_to_quality("unknown_flag") == QualityFlag.RAW


def test_connector_is_registered():
    """The connector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("greece_openhi")
    assert cls is GreeceOpenhiConnector
