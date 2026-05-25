"""Tests for the VMM Waterinfo (Belgium) KiWIS connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.belgium_vmm import BelgiumVmmConnector, _map_quality
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import QualityFlag

# -- Mock response data ------------------------------------------------

MOCK_STATION_LIST_RESPONSE = [
    [
        "station_no",
        "station_name",
        "station_latitude",
        "station_longitude",
        "catchment_area",
        "parametertype_name",
    ],
    [
        "L04_00A",
        "Dender te Denderleeuw",
        50.88,
        4.07,
        1135.0,
        "Discharge",
    ],
    [
        "L06_42A",
        "Schelde te Merelbeke",
        51.0,
        3.74,
        6850.0,
        "Discharge",
    ],
]

MOCK_STATION_LIST_EMPTY = [
    [
        "station_no",
        "station_name",
        "station_latitude",
        "station_longitude",
        "catchment_area",
        "parametertype_name",
    ],
]

MOCK_TS_LIST_RESPONSE = [
    ["ts_id", "ts_name", "station_no"],
    ["78123", "Discharge.Master", "L04_00A"],
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

BASE = "https://download.waterinfo.be/tsmdownload/KiWIS/KiWIS"
BASE_URL = BASE + "/"


# -- Tests: fetch_stations ---------------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_kiwis_response():
    """Station list is correctly parsed from the KiWIS positional array format."""
    respx.get(BASE_URL).mock(
        return_value=httpx.Response(200, json=MOCK_STATION_LIST_RESPONSE),
    )

    async with BelgiumVmmConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2

    dender = next(s for s in stations if s.native_id == "L04_00A")
    assert dender.id == "belgium_vmm:L04_00A"
    assert dender.provider == "belgium_vmm"
    assert dender.name == "Dender te Denderleeuw"
    assert dender.latitude == pytest.approx(50.88)
    assert dender.longitude == pytest.approx(4.07)
    assert dender.country_code == "BE"
    assert dender.catchment_area_km2 == pytest.approx(1135.0)

    schelde = next(s for s in stations if s.native_id == "L06_42A")
    assert schelde.name == "Schelde te Merelbeke"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty station list (headers only) returns no stations."""
    respx.get(BASE_URL).mock(
        return_value=httpx.Response(200, json=MOCK_STATION_LIST_EMPTY),
    )

    async with BelgiumVmmConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_completely_empty():
    """A completely empty response returns no stations."""
    respx.get(BASE_URL).mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with BelgiumVmmConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_bad_columns_raises():
    """Unexpected column layout raises DataFormatError."""
    bad_response = [["wrong_col1", "wrong_col2"], ["val1", "val2"]]
    respx.get(BASE_URL).mock(
        return_value=httpx.Response(200, json=bad_response),
    )

    async with BelgiumVmmConnector() as conn:
        with pytest.raises(DataFormatError, match="Unexpected column layout"):
            await conn.fetch_stations()


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_malformed_rows():
    """Rows with missing or invalid data are skipped, not fatal."""
    response = [
        [
            "station_no",
            "station_name",
            "station_latitude",
            "station_longitude",
            "catchment_area",
            "parametertype_name",
        ],
        ["L04_00A", "Dender te Denderleeuw", 50.88, 4.07, 1135.0, "Discharge"],
        ["BAD"],
    ]
    respx.get(BASE_URL).mock(
        return_value=httpx.Response(200, json=response),
    )

    async with BelgiumVmmConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "L04_00A"


# -- Tests: fetch_observations ----------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_values():
    """Timeseries values are correctly parsed into observations."""
    conn = BelgiumVmmConnector()
    conn._station_to_ts_id["L04_00A"] = "78123"

    respx.get(BASE_URL).mock(
        return_value=httpx.Response(200, json=MOCK_TS_VALUES_RESPONSE),
    )

    async with conn:
        chunk = await conn.fetch_observations(
            "belgium_vmm:L04_00A",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "belgium_vmm"
    assert chunk.station_id == "belgium_vmm:L04_00A"
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
    conn = BelgiumVmmConnector()
    conn._station_to_ts_id["L04_00A"] = "78123"

    respx.get(BASE_URL).mock(
        return_value=httpx.Response(200, json=MOCK_TS_VALUES_EMPTY),
    )

    async with conn:
        chunk = await conn.fetch_observations(
            "belgium_vmm:L04_00A",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_resolves_ts_id_when_not_cached():
    """When ts_id is not cached, fetch_observations queries getTimeseriesList first."""
    route = respx.get(BASE_URL)
    route.side_effect = [
        httpx.Response(200, json=MOCK_TS_LIST_RESPONSE),
        httpx.Response(200, json=MOCK_TS_VALUES_RESPONSE),
    ]

    async with BelgiumVmmConnector() as conn:
        chunk = await conn.fetch_observations(
            "belgium_vmm:L04_00A",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 3
    assert conn._station_to_ts_id["L04_00A"] == "78123"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_no_ts_id_found_raises():
    """When no discharge timeseries exists for the station, raises ConnectorError."""
    empty_ts_list = [
        ["ts_id", "ts_name", "station_no"],
    ]
    respx.get(BASE_URL).mock(
        return_value=httpx.Response(200, json=empty_ts_list),
    )

    async with BelgiumVmmConnector() as conn:
        with pytest.raises(ConnectorError, match="No discharge timeseries found"):
            await conn.fetch_observations(
                "belgium_vmm:UNKNOWN",
                start=datetime(2024, 6, 1),
                end=datetime(2024, 6, 2),
            )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_invalid_timestamp_raises():
    """Invalid timestamp in timeseries data raises DataFormatError."""
    bad_ts_response = [{"data": [["NOT-A-TIMESTAMP", "100.0", "1"]]}]
    conn = BelgiumVmmConnector()
    conn._station_to_ts_id["L04_00A"] = "78123"

    respx.get(BASE_URL).mock(
        return_value=httpx.Response(200, json=bad_ts_response),
    )

    async with conn:
        with pytest.raises(DataFormatError, match="Invalid timestamp"):
            await conn.fetch_observations(
                "belgium_vmm:L04_00A",
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

    cls = get_connector("belgium_vmm")
    assert cls is BelgiumVmmConnector


def test_connector_class_attributes():
    """Class-level attributes match expectations."""
    assert BelgiumVmmConnector.slug == "belgium_vmm"
    assert BelgiumVmmConnector.country_codes == ["BE"]
    assert "waterinfo.be" in BelgiumVmmConnector.base_url


# -- Tests: null/missing catchment area --------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_null_catchment():
    """Stations with null or zero catchment area get None for that field."""
    response = [
        [
            "station_no",
            "station_name",
            "station_latitude",
            "station_longitude",
            "catchment_area",
            "parametertype_name",
        ],
        ["L04_00A", "Test Station", 50.88, 4.07, None, "Discharge"],
        ["L06_42A", "Test Station 2", 51.0, 3.74, "0", "Discharge"],
    ]
    respx.get(BASE_URL).mock(
        return_value=httpx.Response(200, json=response),
    )

    async with BelgiumVmmConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    assert stations[0].catchment_area_km2 is None
    assert stations[1].catchment_area_km2 is None
