"""Tests for the Waterinfo.be (Belgium) connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.belgium_waterinfo import (
    BelgiumWaterinfoConnector,
    _map_quality,
)
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import QualityFlag

# -- Mock response data ------------------------------------------------

MOCK_STATION_LIST_RESPONSE = [
    # First element is the column header
    [
        "station_no",
        "station_name",
        "station_latitude",
        "station_longitude",
        "parametertype_name",
    ],
    # Data rows
    [
        "L04_00A",
        "Dender te Geraardsbergen",
        50.773,
        3.881,
        "Discharge",
    ],
    [
        "L06_00B",
        "Schelde te Melle",
        51.002,
        3.798,
        "Discharge",
    ],
]

MOCK_STATION_LIST_EMPTY = [
    [
        "station_no",
        "station_name",
        "station_latitude",
        "station_longitude",
        "parametertype_name",
    ],
]

MOCK_TS_LIST_RESPONSE = [
    ["ts_id", "ts_name", "station_no"],
    ["78901", "Discharge.Master", "L04_00A"],
]

MOCK_TS_VALUES_RESPONSE = [
    {
        "data": [
            ["2024-06-01T00:00:00.000+02:00", "12.5", "1"],
            ["2024-06-01T01:00:00.000+02:00", "13.2", "10"],
            ["2024-06-01T02:00:00.000+02:00", None, "130"],
        ],
    },
]

MOCK_TS_VALUES_EMPTY = [
    {
        "data": [],
    },
]

BASE = "https://www.waterinfo.be/tsmpub/KiWIS/KiWIS"
# httpx appends a trailing slash when path is empty
BASE_URL = BASE + "/"


# -- Tests: _map_quality -----------------------------------------------

def test_map_quality_good():
    assert _map_quality("1") == QualityFlag.GOOD
    assert _map_quality("9") == QualityFlag.GOOD


def test_map_quality_fair():
    assert _map_quality("10") == QualityFlag.GOOD
    assert _map_quality("19") == QualityFlag.GOOD


def test_map_quality_poor():
    assert _map_quality("20") == QualityFlag.SUSPECT
    assert _map_quality("50") == QualityFlag.SUSPECT


def test_map_quality_missing_codes():
    assert _map_quality("130") == QualityFlag.MISSING
    assert _map_quality("255") == QualityFlag.MISSING


def test_map_quality_none():
    assert _map_quality(None) == QualityFlag.MISSING


def test_map_quality_non_numeric():
    assert _map_quality("abc") == QualityFlag.RAW


# -- Tests: fetch_stations ---------------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_kiwis_response():
    """Station list is correctly parsed from the KiWIS positional array format."""
    respx.get(BASE_URL).mock(
        return_value=httpx.Response(200, json=MOCK_STATION_LIST_RESPONSE),
    )

    async with BelgiumWaterinfoConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2

    dender = next(s for s in stations if s.native_id == "L04_00A")
    assert dender.id == "belgium_waterinfo:L04_00A"
    assert dender.provider == "belgium_waterinfo"
    assert dender.name == "Dender te Geraardsbergen"
    assert dender.latitude == pytest.approx(50.773)
    assert dender.longitude == pytest.approx(3.881)
    assert dender.country_code == "BE"

    schelde = next(s for s in stations if s.native_id == "L06_00B")
    assert schelde.name == "Schelde te Melle"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty station list (headers only) returns no stations."""
    respx.get(BASE_URL).mock(
        return_value=httpx.Response(200, json=MOCK_STATION_LIST_EMPTY),
    )

    async with BelgiumWaterinfoConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_completely_empty():
    """A completely empty response returns no stations."""
    respx.get(BASE_URL).mock(
        return_value=httpx.Response(200, json=[]),
    )

    async with BelgiumWaterinfoConnector() as conn:
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

    async with BelgiumWaterinfoConnector() as conn:
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
            "parametertype_name",
        ],
        # Good row
        ["L04_00A", "Dender te Geraardsbergen", 50.773, 3.881, "Discharge"],
        # Bad row -- too short
        ["BAD"],
    ]
    respx.get(BASE_URL).mock(
        return_value=httpx.Response(200, json=response),
    )

    async with BelgiumWaterinfoConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "L04_00A"


# -- Tests: fetch_observations ----------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_values():
    """Timeseries values are correctly parsed into observations."""
    conn = BelgiumWaterinfoConnector()
    conn._station_to_ts_id["L04_00A"] = "78901"

    respx.get(BASE_URL).mock(
        return_value=httpx.Response(200, json=MOCK_TS_VALUES_RESPONSE),
    )

    async with conn:
        chunk = await conn.fetch_observations(
            "belgium_waterinfo:L04_00A",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "belgium_waterinfo"
    assert chunk.station_id == "belgium_waterinfo:L04_00A"
    assert len(chunk.observations) == 3

    # First observation -- good quality code (1)
    assert chunk.observations[0].discharge_m3s == pytest.approx(12.5)
    assert chunk.observations[0].quality == QualityFlag.GOOD

    # Second observation -- fair quality code (10)
    assert chunk.observations[1].discharge_m3s == pytest.approx(13.2)
    assert chunk.observations[1].quality == QualityFlag.GOOD

    # Third observation -- missing value, quality code 130
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality == QualityFlag.MISSING


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty():
    """An empty data array returns zero observations."""
    conn = BelgiumWaterinfoConnector()
    conn._station_to_ts_id["L04_00A"] = "78901"

    respx.get(BASE_URL).mock(
        return_value=httpx.Response(200, json=MOCK_TS_VALUES_EMPTY),
    )

    async with conn:
        chunk = await conn.fetch_observations(
            "belgium_waterinfo:L04_00A",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_resolves_ts_id_when_not_cached():
    """When ts_id is not cached, fetch_observations queries getTimeseriesList first."""
    route = respx.get(BASE_URL)
    # First call: getTimeseriesList to resolve ts_id
    route.side_effect = [
        httpx.Response(200, json=MOCK_TS_LIST_RESPONSE),
        httpx.Response(200, json=MOCK_TS_VALUES_RESPONSE),
    ]

    async with BelgiumWaterinfoConnector() as conn:
        chunk = await conn.fetch_observations(
            "belgium_waterinfo:L04_00A",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 3
    # Confirm the cache was populated
    assert conn._station_to_ts_id["L04_00A"] == "78901"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_no_ts_id_found_raises():
    """When no discharge timeseries exists for the station, raises ConnectorError."""
    empty_ts_list = [
        ["ts_id", "ts_name", "station_no"],
        # No data rows
    ]
    respx.get(BASE_URL).mock(
        return_value=httpx.Response(200, json=empty_ts_list),
    )

    async with BelgiumWaterinfoConnector() as conn:
        with pytest.raises(ConnectorError, match="No discharge timeseries found"):
            await conn.fetch_observations(
                "belgium_waterinfo:999999",
                start=datetime(2024, 6, 1),
                end=datetime(2024, 6, 2),
            )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_invalid_timestamp_raises():
    """Invalid timestamp in timeseries data raises DataFormatError."""
    bad_ts_response = [{"data": [["NOT-A-TIMESTAMP", "100.0", "1"]]}]
    conn = BelgiumWaterinfoConnector()
    conn._station_to_ts_id["L04_00A"] = "78901"

    respx.get(BASE_URL).mock(
        return_value=httpx.Response(200, json=bad_ts_response),
    )

    async with conn:
        with pytest.raises(DataFormatError, match="Invalid timestamp"):
            await conn.fetch_observations(
                "belgium_waterinfo:L04_00A",
                start=datetime(2024, 6, 1),
                end=datetime(2024, 6, 2),
            )


# -- Tests: _resolve_ts_id cache --------------------------------------

@pytest.mark.asyncio
@respx.mock
async def test_resolve_ts_id_uses_cache():
    """When the ts_id is already cached, no HTTP call is made."""
    conn = BelgiumWaterinfoConnector()
    conn._station_to_ts_id["L04_00A"] = "78901"

    # No respx mock -- any HTTP call would raise
    async with conn:
        ts_id = await conn._resolve_ts_id("L04_00A")

    assert ts_id == "78901"


@pytest.mark.asyncio
@respx.mock
async def test_resolve_ts_id_bad_columns_raises():
    """Unexpected column layout in timeseries list raises DataFormatError."""
    bad_response = [["wrong_col"], ["val1"]]
    respx.get(BASE_URL).mock(
        return_value=httpx.Response(200, json=bad_response),
    )

    async with BelgiumWaterinfoConnector() as conn:
        with pytest.raises(DataFormatError, match="Unexpected column layout"):
            await conn._resolve_ts_id("L04_00A")


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
