"""Tests for the Ireland OPW connector with mocked HTTP responses."""

import gzip
from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.ireland_opw import IrelandOPWConnector

MOCK_GEOJSON = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "id": 1,
            "properties": {"name": "Ballymote Bridge", "ref": "0000125001"},
            "geometry": {"type": "Point", "coordinates": [-8.515, 54.085]},
        },
        {
            "type": "Feature",
            "id": 2,
            "properties": {"name": "Foxford", "ref": "0000230002"},
            "geometry": {"type": "Point", "coordinates": [-9.112, 53.978]},
        },
        {
            "type": "Feature",
            "id": 3,
            "properties": {"name": "Bad Station", "ref": "BAD"},
            "geometry": {"type": "Point", "coordinates": [-8.0, 54.0]},
        },
        {
            "type": "Feature",
            "id": 4,
            "properties": {"name": "No Geom", "ref": "0000199001"},
            "geometry": {},
        },
    ],
}

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
async def test_fetch_stations_parses_geojson():
    """Stations with valid coordinates and refs are parsed from GeoJSON."""
    respx.get("https://waterlevel.ie/geojson/").mock(
        return_value=httpx.Response(200, json=MOCK_GEOJSON),
    )

    async with IrelandOPWConnector() as conn:
        stations = await conn.fetch_stations()

    # Bad Station has ref too short, No Geom has no coordinates
    assert len(stations) == 2
    native_ids = {s.native_id for s in stations}
    assert native_ids == {"25001", "30002"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_field_values():
    """Station fields are mapped correctly from GeoJSON."""
    respx.get("https://waterlevel.ie/geojson/").mock(
        return_value=httpx.Response(200, json=MOCK_GEOJSON),
    )

    async with IrelandOPWConnector() as conn:
        stations = await conn.fetch_stations()

    station = next(s for s in stations if s.native_id == "25001")
    assert station.id == "ireland_opw:25001"
    assert station.provider == "ireland_opw"
    assert station.name == "Ballymote Bridge"
    assert station.country_code == "IE"
    assert station.latitude == pytest.approx(54.085)
    assert station.longitude == pytest.approx(-8.515)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """Empty GeoJSON falls through to CSV; empty CSV returns no stations."""
    respx.get("https://waterlevel.ie/geojson/").mock(
        return_value=httpx.Response(200, json={"type": "FeatureCollection", "features": []}),
    )
    respx.get("https://waterlevel.ie/data/station_list.csv").mock(
        return_value=httpx.Response(200, text="name,label\n"),
    )

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
        "/data/dailymean/25001_dailymean.csv.gz"
    ).mock(return_value=httpx.Response(200, content=compressed))

    async with IrelandOPWConnector() as conn:
        chunk = await conn.fetch_observations(
            "ireland_opw:25001",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 4),
        )

    assert chunk.provider == "ireland_opw"
    assert chunk.station_id == "ireland_opw:25001"
    assert len(chunk.observations) == 4


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_quality_mapping():
    """Quality strings from the CSV are mapped to QualityFlag values."""
    compressed = _gzip_bytes(_DAILYMEAN_CSV)
    respx.get(
        "https://waterlevel.ie"
        "/data/dailymean/25001_dailymean.csv.gz"
    ).mock(return_value=httpx.Response(200, content=compressed))

    async with IrelandOPWConnector() as conn:
        chunk = await conn.fetch_observations(
            "ireland_opw:25001",
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
        "/data/dailymean/25001_dailymean.csv.gz"
    ).mock(return_value=httpx.Response(200, content=compressed))

    async with IrelandOPWConnector() as conn:
        chunk = await conn.fetch_observations(
            "ireland_opw:25001",
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
        "/data/dailymean/25001_dailymean.csv.gz"
    ).mock(return_value=httpx.Response(200, content=compressed))

    async with IrelandOPWConnector() as conn:
        chunk = await conn.fetch_observations(
            "ireland_opw:25001",
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
        "/data/dailymean/25001_dailymean.csv.gz"
    ).mock(return_value=httpx.Response(200, content=b"not-gzip-data"))

    from csfs.core.exceptions import DataFormatError

    async with IrelandOPWConnector() as conn:
        with pytest.raises(DataFormatError, match="decompress"):
            await conn.fetch_observations(
                "ireland_opw:25001",
                start=datetime(2024, 1, 1),
                end=datetime(2024, 1, 2),
            )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_csv_fallback():
    """Falls back to CSV when GeoJSON fails."""
    respx.get("https://waterlevel.ie/geojson/").mock(
        return_value=httpx.Response(500),
    )
    respx.get("https://waterlevel.ie/data/station_list.csv").mock(
        return_value=httpx.Response(200, text="name,label\n01041,Sandy Mills\n"),
    )

    async with IrelandOPWConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "01041"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_bom_encoded_csv():
    """CSV with UTF-8 BOM is parsed correctly."""
    csv_with_bom = "﻿Date,Value,Quality\n2024-06-15,99.9,Good\n"
    compressed = _gzip_bytes(csv_with_bom)
    respx.get(
        "https://waterlevel.ie"
        "/data/dailymean/25001_dailymean.csv.gz"
    ).mock(return_value=httpx.Response(200, content=compressed))

    async with IrelandOPWConnector() as conn:
        chunk = await conn.fetch_observations(
            "ireland_opw:25001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 30),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(99.9)


@pytest.mark.asyncio
@respx.mock
async def test_geojson_value_error_falls_back_to_csv():
    """ValueError in geojson parsing (e.g., bad float) falls back to CSV."""
    bad_geojson = {
        "features": [
            {
                "properties": {"name": "Test", "ref": "0000125001"},
                "geometry": {
                    "type": "Point",
                    "coordinates": ["not-a-float", 54.0],
                },
            },
        ],
    }
    respx.get("https://waterlevel.ie/geojson/").mock(
        return_value=httpx.Response(200, json=bad_geojson),
    )
    respx.get("https://waterlevel.ie/data/station_list.csv").mock(
        return_value=httpx.Response(
            200, text="name,label\n01041,Sandy Mills\n",
        ),
    )

    async with IrelandOPWConnector() as conn:
        stations = await conn.fetch_stations()

    # bad geojson returns empty list -> falls back to CSV
    assert len(stations) == 1
    assert stations[0].native_id == "01041"


@pytest.mark.asyncio
@respx.mock
async def test_station_csv_skips_name_header():
    """Station CSV skips rows where native_id is 'name'."""
    csv_text = "name,label\nname,Header Row\n01041,Sandy Mills\n02001,Ballina\n"
    respx.get("https://waterlevel.ie/geojson/").mock(
        return_value=httpx.Response(500),
    )
    respx.get("https://waterlevel.ie/data/station_list.csv").mock(
        return_value=httpx.Response(200, text=csv_text),
    )

    async with IrelandOPWConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    native_ids = {s.native_id for s in stations}
    assert "01041" in native_ids
    assert "02001" in native_ids
    assert "name" not in native_ids


@pytest.mark.asyncio
@respx.mock
async def test_station_csv_short_line_skipped():
    """Station CSV lines with fewer than 2 parts are skipped."""
    csv_text = "name,label\nshort_only\n01041,Sandy Mills\n"
    respx.get("https://waterlevel.ie/geojson/").mock(
        return_value=httpx.Response(500),
    )
    respx.get("https://waterlevel.ie/data/station_list.csv").mock(
        return_value=httpx.Response(200, text=csv_text),
    )

    async with IrelandOPWConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "01041"


def test_parse_epa_stations():
    """EPA HydroNet JSON station index is parsed correctly."""
    conn = IrelandOPWConnector.__new__(IrelandOPWConnector)
    conn.slug = "ireland_opw"

    epa_data = [
        {
            "L1_ts_name": "0001",
            "metadata_station_name": "River Lee",
            "metadata_station_latitude": "51.8985",
            "metadata_station_longitude": "-8.4756",
            "L1_DATA_AVAILABLE": True,
        },
        {
            "L1_ts_name": "0002",
            "metadata_station_name": "River Shannon",
            "metadata_station_latitude": "52.6638",
            "metadata_station_longitude": "-7.2555",
            "L1_DATA_AVAILABLE": False,
        },
        {
            "L1_ts_name": "",
            "metadata_station_name": "Empty ID",
            "metadata_station_latitude": "53.0",
            "metadata_station_longitude": "-7.0",
        },
        {
            "L1_ts_name": "0003",
            "metadata_station_name": "Bad Coords",
            "metadata_station_latitude": "not-a-number",
            "metadata_station_longitude": "-7.0",
        },
    ]

    stations = conn._parse_epa_stations(epa_data)

    assert len(stations) == 2
    assert stations[0].native_id == "0001"
    assert stations[0].name == "River Lee"
    assert stations[0].latitude == pytest.approx(51.8985)
    assert stations[0].is_active is True
    assert stations[1].native_id == "0002"
    assert stations[1].is_active is False


@pytest.mark.asyncio
@respx.mock
async def test_dailymean_csv_slash_date_format():
    """Daily-mean CSV with slash date format (YYYY/MM/DD HH:MM:SS) is parsed."""
    csv_text = "Date,Value,Quality\n2024/01/15 08:00:00,22.5,Good\n2024/01/16 08:00:00,23.0,\n"
    compressed = _gzip_bytes(csv_text)
    respx.get(
        "https://waterlevel.ie"
        "/data/dailymean/25001_dailymean.csv.gz"
    ).mock(return_value=httpx.Response(200, content=compressed))

    async with IrelandOPWConnector() as conn:
        chunk = await conn.fetch_observations(
            "ireland_opw:25001",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 31),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(22.5)
    assert chunk.observations[0].quality.value == "good"


@pytest.mark.asyncio
@respx.mock
async def test_dailymean_csv_date_only_slash_format():
    """Daily-mean CSV with YYYY/MM/DD format (no time) is parsed."""
    csv_text = "Date,Value,Quality\n2024/01/15,22.5,Good\n"
    compressed = _gzip_bytes(csv_text)
    respx.get(
        "https://waterlevel.ie"
        "/data/dailymean/25001_dailymean.csv.gz"
    ).mock(return_value=httpx.Response(200, content=compressed))

    async with IrelandOPWConnector() as conn:
        chunk = await conn.fetch_observations(
            "ireland_opw:25001",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 31),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(22.5)


@pytest.mark.asyncio
@respx.mock
async def test_dailymean_csv_unparseable_date():
    """Rows with completely unparseable dates are skipped."""
    csv_text = "Date,Value,Quality\nnot-a-date,22.5,Good\n2024-01-01,10.0,Good\n"
    compressed = _gzip_bytes(csv_text)
    respx.get(
        "https://waterlevel.ie"
        "/data/dailymean/25001_dailymean.csv.gz"
    ).mock(return_value=httpx.Response(200, content=compressed))

    async with IrelandOPWConnector() as conn:
        chunk = await conn.fetch_observations(
            "ireland_opw:25001",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 12, 31),
        )

    assert len(chunk.observations) == 1


@pytest.mark.asyncio
@respx.mock
async def test_dailymean_csv_null_discharge_overrides_quality():
    """When discharge is None, quality is forced to MISSING."""
    csv_text = "Date,Value,Quality\n2024-01-01,,Good\n2024-01-02,abc,Suspect\n"
    compressed = _gzip_bytes(csv_text)
    respx.get(
        "https://waterlevel.ie"
        "/data/dailymean/25001_dailymean.csv.gz"
    ).mock(return_value=httpx.Response(200, content=compressed))

    async with IrelandOPWConnector() as conn:
        chunk = await conn.fetch_observations(
            "ireland_opw:25001",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 1, 31),
        )

    # Both rows: empty value -> None, unparseable value -> None
    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[0].quality.value == "missing"
    assert chunk.observations[1].discharge_m3s is None
    assert chunk.observations[1].quality.value == "missing"
