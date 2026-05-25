"""Tests for the Ireland OPW connector with mocked HTTP responses."""

import gzip
from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.ireland_opw import IrelandOPWConnector

MOCK_EPA_STATIONS = [
    {
        "metadata_station_name": "Ballymote Bridge",
        "metadata_station_longitude": "-8.515",
        "metadata_station_latitude": "54.085",
        "L1_DATA_AVAILABLE": True,
        "L1_ts_name": "0000125001",
    },
    {
        "metadata_station_name": "Foxford",
        "metadata_station_longitude": "-9.112",
        "metadata_station_latitude": "53.978",
        "L1_DATA_AVAILABLE": True,
        "L1_ts_name": "0000230002",
    },
    {
        "metadata_station_name": "Bad Station",
        "metadata_station_longitude": "not_a_number",
        "metadata_station_latitude": "54.0",
        "L1_DATA_AVAILABLE": False,
        "L1_ts_name": "BAD001",
    },
    {
        "metadata_station_name": "No ID Station",
        "metadata_station_longitude": "-8.0",
        "metadata_station_latitude": "53.0",
        "L1_DATA_AVAILABLE": False,
        "L1_ts_name": "",
    },
]

_DAILYMEAN_CSV = (
    "Date,Value,Quality\n"
    "2024-01-01,12.34,Good\n"
    "2024-01-02,13.50,Suspect\n"
    "2024-01-03,,Missing\n"
    "2024-01-04,15.00,\n"
)


def _gzip_bytes(text: str) -> bytes:
    """Compress a string to gzip bytes."""
    return gzip.compress(text.encode("utf-8"))


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_epa_json():
    """Stations with valid coordinates are parsed from EPA metadata."""
    respx.get(
        "https://epawebapp.epa.ie"
        "/hydronet/output/internet/layers/10/index.json"
    ).mock(return_value=httpx.Response(200, json=MOCK_EPA_STATIONS))

    async with IrelandOPWConnector() as conn:
        stations = await conn.fetch_stations()

    # Bad Station has invalid lon, No ID Station has empty ts_name
    assert len(stations) == 2
    native_ids = {s.native_id for s in stations}
    assert native_ids == {"0000125001", "0000230002"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_field_values():
    """Station fields are mapped correctly from EPA metadata."""
    respx.get(
        "https://epawebapp.epa.ie"
        "/hydronet/output/internet/layers/10/index.json"
    ).mock(return_value=httpx.Response(200, json=MOCK_EPA_STATIONS))

    async with IrelandOPWConnector() as conn:
        stations = await conn.fetch_stations()

    station = next(
        s for s in stations if s.native_id == "0000125001"
    )
    assert station.id == "ireland_opw:0000125001"
    assert station.provider == "ireland_opw"
    assert station.name == "Ballymote Bridge"
    assert station.country_code == "IE"
    assert station.latitude == pytest.approx(54.085)
    assert station.longitude == pytest.approx(-8.515)
    assert station.is_active is True


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty station list returns no stations."""
    respx.get(
        "https://epawebapp.epa.ie"
        "/hydronet/output/internet/layers/10/index.json"
    ).mock(return_value=httpx.Response(200, json=[]))

    async with IrelandOPWConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_gzip_csv():
    """Daily-mean gzipped CSV is decompressed and parsed."""
    compressed = _gzip_bytes(_DAILYMEAN_CSV)
    respx.get(
        "https://waterlevel.ie"
        "/data/dailymean/0000125001_dailymean.csv.gz"
    ).mock(return_value=httpx.Response(200, content=compressed))

    async with IrelandOPWConnector() as conn:
        chunk = await conn.fetch_observations(
            "ireland_opw:0000125001",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 4),
        )

    assert chunk.provider == "ireland_opw"
    assert chunk.station_id == "ireland_opw:0000125001"
    assert len(chunk.observations) == 4


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_quality_mapping():
    """Quality strings from the CSV are mapped to QualityFlag values."""
    compressed = _gzip_bytes(_DAILYMEAN_CSV)
    respx.get(
        "https://waterlevel.ie"
        "/data/dailymean/0000125001_dailymean.csv.gz"
    ).mock(return_value=httpx.Response(200, content=compressed))

    async with IrelandOPWConnector() as conn:
        chunk = await conn.fetch_observations(
            "ireland_opw:0000125001",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 4),
        )

    assert chunk.observations[0].quality.value == "good"
    assert chunk.observations[0].discharge_m3s == pytest.approx(12.34)
    assert chunk.observations[1].quality.value == "suspect"
    # Empty value -> MISSING regardless of quality string
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"
    # No quality string -> RAW (default)
    assert chunk.observations[3].quality.value == "raw"
    assert chunk.observations[3].discharge_m3s == pytest.approx(15.00)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_filters_by_date_range():
    """Only observations within the requested range are returned."""
    compressed = _gzip_bytes(_DAILYMEAN_CSV)
    respx.get(
        "https://waterlevel.ie"
        "/data/dailymean/0000125001_dailymean.csv.gz"
    ).mock(return_value=httpx.Response(200, content=compressed))

    async with IrelandOPWConnector() as conn:
        chunk = await conn.fetch_observations(
            "ireland_opw:0000125001",
            start=datetime(2024, 1, 2),
            end=datetime(2024, 1, 3),
        )

    assert len(chunk.observations) == 2
    timestamps = [o.timestamp for o in chunk.observations]
    assert datetime(2024, 1, 2) in timestamps
    assert datetime(2024, 1, 3) in timestamps


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty_csv():
    """A CSV with only headers returns zero observations."""
    compressed = _gzip_bytes("Date,Value,Quality\n")
    respx.get(
        "https://waterlevel.ie"
        "/data/dailymean/0000125001_dailymean.csv.gz"
    ).mock(return_value=httpx.Response(200, content=compressed))

    async with IrelandOPWConnector() as conn:
        chunk = await conn.fetch_observations(
            "ireland_opw:0000125001",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 12, 31),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_bad_gzip_raises():
    """Invalid gzip data raises DataFormatError."""
    respx.get(
        "https://waterlevel.ie"
        "/data/dailymean/0000125001_dailymean.csv.gz"
    ).mock(return_value=httpx.Response(200, content=b"not-gzip-data"))

    from csfs.core.exceptions import DataFormatError

    async with IrelandOPWConnector() as conn:
        with pytest.raises(DataFormatError, match="decompress"):
            await conn.fetch_observations(
                "ireland_opw:0000125001",
                start=datetime(2024, 1, 1),
                end=datetime(2024, 1, 2),
            )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_inactive_flag():
    """Stations with L1_DATA_AVAILABLE=False are marked inactive."""
    data = [
        {
            "metadata_station_name": "Inactive Station",
            "metadata_station_longitude": "-7.0",
            "metadata_station_latitude": "53.5",
            "L1_DATA_AVAILABLE": False,
            "L1_ts_name": "INACTIVE01",
        },
    ]
    respx.get(
        "https://epawebapp.epa.ie"
        "/hydronet/output/internet/layers/10/index.json"
    ).mock(return_value=httpx.Response(200, json=data))

    async with IrelandOPWConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].is_active is False


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_bom_encoded_csv():
    """CSV with UTF-8 BOM is parsed correctly."""
    csv_with_bom = "﻿Date,Value,Quality\n2024-06-15,99.9,Good\n"
    compressed = _gzip_bytes(csv_with_bom)
    respx.get(
        "https://waterlevel.ie"
        "/data/dailymean/0000125001_dailymean.csv.gz"
    ).mock(return_value=httpx.Response(200, content=compressed))

    async with IrelandOPWConnector() as conn:
        chunk = await conn.fetch_observations(
            "ireland_opw:0000125001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 30),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(99.9)
