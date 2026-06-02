"""Tests for the VMM Waterinfo (Belgium) KiWIS connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.belgium_waterinfo import BelgiumWaterinfoConnector, _map_quality
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import QualityFlag

# -- Mock response data ------------------------------------------------

# Station metadata list (no parametertype_name — see connector docstring).
MOCK_STATION_LIST_RESPONSE = [
    [
        "station_no",
        "station_name",
        "station_latitude",
        "station_longitude",
    ],
    [
        "L04_00A",
        "Dender te Denderleeuw",
        50.88,
        4.07,
    ],
    [
        "L06_42A",
        "Schelde te Merelbeke",
        51.0,
        3.74,
    ],
    [
        # A station with no Q series — must be filtered out.
        "L99_NOQ",
        "Rainfall only",
        50.5,
        4.5,
    ],
]

MOCK_STATION_LIST_EMPTY = [
    [
        "station_no",
        "station_name",
        "station_latitude",
        "station_longitude",
    ],
]

# Filtered getTimeseriesList (stationparameter_name=Q): one row per
# (station, cadence) discharge series. L04_00A has both P.15 and DagGem;
# L06_42A only DagGem. L99_NOQ is absent (no discharge).
MOCK_Q_SERIES_RESPONSE = [
    ["station_no", "ts_id", "ts_name"],
    ["L04_00A", "78100", "DagGem"],
    ["L04_00A", "78123", "P.15"],
    ["L06_42A", "79200", "DagGem"],
]

MOCK_Q_SERIES_EMPTY = [
    ["station_no", "ts_id", "ts_name"],
]

MOCK_TS_VALUES_RESPONSE = [
    {
        "data": [
            ["2024-06-01T00:00:00.000+02:00", "12.4", "1"],
            ["2024-06-01T01:00:00.000+02:00", "13.1", "10"],
            ["2024-06-01T02:00:00.000+02:00", None, "130"],
        ],
    },
]

MOCK_TS_VALUES_EMPTY = [
    {
        "data": [],
    },
]

KIWIS_URL = "https://download.waterinfo.be/tsmdownload/KiWIS/KiWIS"


def _route_by_request(q_series, station_list):
    """Return a respx side_effect that dispatches on the KiWIS `request` param.

    fetch_stations issues two calls — getTimeseriesList (Q map) and
    getStationList — so tests must answer both based on the request type.
    """
    def _handler(request):
        req = request.url.params.get("request")
        if req == "getTimeseriesList":
            return httpx.Response(200, json=q_series)
        if req == "getStationList":
            return httpx.Response(200, json=station_list)
        if req == "getTimeseriesvalues":
            return httpx.Response(200, json=MOCK_TS_VALUES_RESPONSE)
        return httpx.Response(404)

    return _handler


# -- Tests: fetch_stations ---------------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_kiwis_response():
    """Station list is parsed and filtered to stations that have a Q series."""
    respx.get(KIWIS_URL).mock(
        side_effect=_route_by_request(
            MOCK_Q_SERIES_RESPONSE, MOCK_STATION_LIST_RESPONSE,
        ),
    )

    async with BelgiumWaterinfoConnector() as conn:
        stations = await conn.fetch_stations()

    # L99_NOQ has no discharge series and must be dropped.
    assert len(stations) == 2
    assert {s.native_id for s in stations} == {"L04_00A", "L06_42A"}

    dender = next(s for s in stations if s.native_id == "L04_00A")
    assert dender.id == "belgium_waterinfo:L04_00A"
    assert dender.provider == "belgium_waterinfo"
    assert dender.name == "Dender te Denderleeuw"
    assert dender.latitude == pytest.approx(50.88)
    assert dender.longitude == pytest.approx(4.07)
    assert dender.country_code == "BE"

    schelde = next(s for s in stations if s.native_id == "L06_42A")
    assert schelde.name == "Schelde te Merelbeke"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty station list (headers only) returns no stations."""
    respx.get(KIWIS_URL).mock(
        side_effect=_route_by_request(
            MOCK_Q_SERIES_RESPONSE, MOCK_STATION_LIST_EMPTY,
        ),
    )

    async with BelgiumWaterinfoConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_completely_empty():
    """A completely empty station response returns no stations."""
    respx.get(KIWIS_URL).mock(
        side_effect=_route_by_request(MOCK_Q_SERIES_RESPONSE, []),
    )

    async with BelgiumWaterinfoConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_bad_columns_raises():
    """Unexpected column layout in the station list raises DataFormatError."""
    bad_station_list = [["wrong_col1", "wrong_col2"], ["val1", "val2"]]
    respx.get(KIWIS_URL).mock(
        side_effect=_route_by_request(MOCK_Q_SERIES_RESPONSE, bad_station_list),
    )

    async with BelgiumWaterinfoConnector() as conn:
        with pytest.raises(DataFormatError, match="Unexpected column layout"):
            await conn.fetch_stations()


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_malformed_rows():
    """Rows with missing or invalid data are skipped, not fatal."""
    station_list = [
        ["station_no", "station_name", "station_latitude", "station_longitude"],
        ["L04_00A", "Dender te Denderleeuw", 50.88, 4.07],
        ["BAD"],
    ]
    respx.get(KIWIS_URL).mock(
        side_effect=_route_by_request(MOCK_Q_SERIES_RESPONSE, station_list),
    )

    async with BelgiumWaterinfoConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "L04_00A"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_bad_q_series_columns_raises():
    """Unexpected column layout in the Q timeseries list raises DataFormatError."""
    bad_q_series = [["wrong_col"], ["val1"]]
    respx.get(KIWIS_URL).mock(
        side_effect=_route_by_request(bad_q_series, MOCK_STATION_LIST_RESPONSE),
    )

    async with BelgiumWaterinfoConnector() as conn:
        with pytest.raises(DataFormatError, match="Unexpected column layout"):
            await conn.fetch_stations()


# -- Tests: discharge series selection ---------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_resolve_ts_id_prefers_realtime_cadence():
    """P.15 (validated real-time) is preferred over DagGem (daily mean)."""
    respx.get(KIWIS_URL).mock(
        side_effect=_route_by_request(
            MOCK_Q_SERIES_RESPONSE, MOCK_STATION_LIST_RESPONSE,
        ),
    )

    async with BelgiumWaterinfoConnector() as conn:
        # L04_00A has both DagGem (78100) and P.15 (78123); prefer P.15.
        assert await conn._resolve_ts_id("L04_00A") == "78123"
        # L06_42A only has DagGem.
        assert await conn._resolve_ts_id("L06_42A") == "79200"


@pytest.mark.asyncio
@respx.mock
async def test_resolve_ts_id_falls_back_to_any_series():
    """When no preferred cadence matches, any discharge series is used."""
    q_series = [
        ["station_no", "ts_id", "ts_name"],
        ["L04_00A", "55555", "SomeOtherCadence"],
    ]
    respx.get(KIWIS_URL).mock(
        side_effect=_route_by_request(q_series, MOCK_STATION_LIST_RESPONSE),
    )

    async with BelgiumWaterinfoConnector() as conn:
        assert await conn._resolve_ts_id("L04_00A") == "55555"


# -- Tests: fetch_observations ----------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_values():
    """Timeseries values are correctly parsed into observations."""
    respx.get(KIWIS_URL).mock(
        side_effect=_route_by_request(
            MOCK_Q_SERIES_RESPONSE, MOCK_STATION_LIST_RESPONSE,
        ),
    )

    async with BelgiumWaterinfoConnector() as conn:
        chunk = await conn.fetch_observations(
            "belgium_waterinfo:L04_00A",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "belgium_waterinfo"
    assert chunk.station_id == "belgium_waterinfo:L04_00A"
    assert len(chunk.observations) == 3

    assert chunk.observations[0].discharge_m3s == pytest.approx(12.4)
    assert chunk.observations[0].quality == QualityFlag.GOOD

    assert chunk.observations[1].discharge_m3s == pytest.approx(13.1)
    assert chunk.observations[1].quality == QualityFlag.GOOD

    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality == QualityFlag.MISSING


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty():
    """An empty data array returns zero observations."""
    def handler(request):
        req = request.url.params.get("request")
        if req == "getTimeseriesList":
            return httpx.Response(200, json=MOCK_Q_SERIES_RESPONSE)
        return httpx.Response(200, json=MOCK_TS_VALUES_EMPTY)

    respx.get(KIWIS_URL).mock(side_effect=handler)

    async with BelgiumWaterinfoConnector() as conn:
        chunk = await conn.fetch_observations(
            "belgium_waterinfo:L04_00A",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_no_ts_id_found_raises():
    """When no discharge timeseries exists for the station, raises ConnectorError."""
    respx.get(KIWIS_URL).mock(
        side_effect=_route_by_request(
            MOCK_Q_SERIES_RESPONSE, MOCK_STATION_LIST_RESPONSE,
        ),
    )

    async with BelgiumWaterinfoConnector() as conn:
        with pytest.raises(ConnectorError, match="No discharge .Q. timeseries"):
            await conn.fetch_observations(
                "belgium_waterinfo:UNKNOWN",
                start=datetime(2024, 6, 1),
                end=datetime(2024, 6, 2),
            )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_invalid_timestamp_raises():
    """Invalid timestamp in timeseries data raises DataFormatError."""
    bad_ts_response = [{"data": [["NOT-A-TIMESTAMP", "100.0", "1"]]}]

    def handler(request):
        req = request.url.params.get("request")
        if req == "getTimeseriesList":
            return httpx.Response(200, json=MOCK_Q_SERIES_RESPONSE)
        return httpx.Response(200, json=bad_ts_response)

    respx.get(KIWIS_URL).mock(side_effect=handler)

    async with BelgiumWaterinfoConnector() as conn:
        with pytest.raises(DataFormatError, match="Invalid timestamp"):
            await conn.fetch_observations(
                "belgium_waterinfo:L04_00A",
                start=datetime(2024, 6, 1),
                end=datetime(2024, 6, 2),
            )


# -- Tests: _map_quality -----------------------------------------------

def test_map_quality_good():
    assert _map_quality("1") == QualityFlag.GOOD
    assert _map_quality("9") == QualityFlag.GOOD


def test_map_quality_suspect():
    assert _map_quality("20") == QualityFlag.SUSPECT
    assert _map_quality("50") == QualityFlag.SUSPECT


def test_map_quality_missing_codes():
    assert _map_quality("130") == QualityFlag.MISSING
    assert _map_quality("255") == QualityFlag.MISSING


def test_map_quality_none():
    assert _map_quality(None) == QualityFlag.MISSING


def test_map_quality_non_numeric():
    assert _map_quality("abc") == QualityFlag.RAW


# -- Tests: registration -----------------------------------------------

def test_connector_is_registered():
    """The connector is registered with the expected slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("belgium_waterinfo")
    assert cls is BelgiumWaterinfoConnector


def test_connector_class_attributes():
    """Class-level attributes match expectations."""
    assert BelgiumWaterinfoConnector.slug == "belgium_waterinfo"
    assert BelgiumWaterinfoConnector.country_codes == ["BE"]
    assert "waterinfo.be" in BelgiumWaterinfoConnector.base_url


# -- Tests: fetch_latest -----------------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest_delegates_to_observations():
    """fetch_latest fetches last 24 hours via fetch_observations."""
    def handler(request):
        req = request.url.params.get("request")
        if req == "getTimeseriesList":
            return httpx.Response(200, json=MOCK_Q_SERIES_RESPONSE)
        return httpx.Response(200, json=MOCK_TS_VALUES_EMPTY)

    respx.get(KIWIS_URL).mock(side_effect=handler)

    async with BelgiumWaterinfoConnector() as conn:
        chunk = await conn.fetch_latest("belgium_waterinfo:L04_00A")

    assert chunk.provider == "belgium_waterinfo"
    assert chunk.station_id == "belgium_waterinfo:L04_00A"
    assert len(chunk.observations) == 0


# -- Tests: station parsing edge cases ----------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_empty_native_id_skipped():
    """Rows with an empty native_id string are skipped."""
    station_list = [
        ["station_no", "station_name", "station_latitude", "station_longitude"],
        ["", "Empty ID Station", 50.0, 4.0],
        ["L04_00A", "Valid Station", 50.88, 4.07],
    ]
    respx.get(KIWIS_URL).mock(
        side_effect=_route_by_request(MOCK_Q_SERIES_RESPONSE, station_list),
    )

    async with BelgiumWaterinfoConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "L04_00A"


# -- Tests: timeseries parsing edge cases ------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_non_dict_first_element():
    """Non-dict first element in timeseries response returns empty observations."""
    def handler(request):
        req = request.url.params.get("request")
        if req == "getTimeseriesList":
            return httpx.Response(200, json=MOCK_Q_SERIES_RESPONSE)
        return httpx.Response(200, json=["not_a_dict"])

    respx.get(KIWIS_URL).mock(side_effect=handler)

    async with BelgiumWaterinfoConnector() as conn:
        chunk = await conn.fetch_observations(
            "belgium_waterinfo:L04_00A",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_unparseable_discharge_value():
    """Non-numeric discharge value results in None discharge and MISSING quality."""
    ts_response = [{"data": [
        ["2024-06-01T00:00:00.000+02:00", "not_a_number", "1"],
    ]}]

    def handler(request):
        req = request.url.params.get("request")
        if req == "getTimeseriesList":
            return httpx.Response(200, json=MOCK_Q_SERIES_RESPONSE)
        return httpx.Response(200, json=ts_response)

    respx.get(KIWIS_URL).mock(side_effect=handler)

    async with BelgiumWaterinfoConnector() as conn:
        chunk = await conn.fetch_observations(
            "belgium_waterinfo:L04_00A",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[0].quality == QualityFlag.MISSING


@pytest.mark.asyncio
@respx.mock
async def test_parse_q_series_malformed_row_skipped():
    """Malformed rows in the Q timeseries list are skipped without crashing."""
    q_series = [
        ["station_no", "ts_id", "ts_name"],
        ["L04_00A", "78123", "P.15"],
        [],  # malformed row
    ]
    respx.get(KIWIS_URL).mock(
        side_effect=_route_by_request(q_series, MOCK_STATION_LIST_RESPONSE),
    )

    async with BelgiumWaterinfoConnector() as conn:
        chunk = await conn.fetch_observations(
            "belgium_waterinfo:L04_00A",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 3


# -- Tests: null/empty coordinates -------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_null_coords_default_to_zero():
    """Stations with null or empty-string coordinates default lat/lon to 0.0."""
    station_list = [
        ["station_no", "station_name", "station_latitude", "station_longitude"],
        ["L04_00A", "Test Station", 50.88, 4.07],
        ["L06_42A", "Null Coords", None, None],
    ]
    q_series = [
        ["station_no", "ts_id", "ts_name"],
        ["L04_00A", "78123", "P.15"],
        ["L06_42A", "79200", "DagGem"],
    ]
    respx.get(KIWIS_URL).mock(
        side_effect=_route_by_request(q_series, station_list),
    )

    async with BelgiumWaterinfoConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    nullc = next(s for s in stations if s.native_id == "L06_42A")
    assert nullc.latitude == 0.0
    assert nullc.longitude == 0.0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_empty_string_coords_default_to_zero():
    """Empty-string coordinates (VMM returns '' not null) default to 0.0."""
    station_list = [
        ["station_no", "station_name", "station_latitude", "station_longitude"],
        ["L04_00A", "Empty Coords", "", ""],
    ]
    q_series = [
        ["station_no", "ts_id", "ts_name"],
        ["L04_00A", "78123", "P.15"],
    ]
    respx.get(KIWIS_URL).mock(
        side_effect=_route_by_request(q_series, station_list),
    )

    async with BelgiumWaterinfoConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].latitude == 0.0
    assert stations[0].longitude == 0.0
