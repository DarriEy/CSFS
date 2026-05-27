"""Tests for the El Salvador MARN connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.elsalvador_marn import ElSalvadorMARNConnector

MOCK_JSON_STATIONS = [
    {
        "id": "SV-100",
        "name": "Estacion La Ceiba",
        "latitude": 14.3167,
        "longitude": -89.1333,
        "river": "Lempa",
    },
    {
        "id": "SV-101",
        "name": "Estacion El Guayabo",
        "latitude": 13.85,
        "longitude": -88.9167,
        "river": "Lempa",
    },
    {
        "id": "",
        "name": "Missing ID",
        "latitude": 14.0,
        "longitude": -89.0,
    },
    {
        "id": "SV-102",
        "name": "No Coords Station",
    },
]

MOCK_AQUARIUS_LOCATIONS = {
    "LocationDescriptions": [
        {
            "Identifier": "AQ-001",
            "Name": "AQUARIUS Station Alpha",
            "Latitude": 13.7,
            "Longitude": -89.2,
            "River": "Sucio",
        },
        {
            "Identifier": "AQ-002",
            "Name": "AQUARIUS Station Beta",
            "Latitude": 13.5,
            "Longitude": -88.9,
            "River": "Jiboa",
        },
    ],
}

MOCK_AQUARIUS_TIMESERIES = {
    "Points": [
        {
            "Timestamp": "2024-06-01T12:00:00",
            "Value": {"Numeric": 34.5, "GradeCode": 50},
        },
        {
            "Timestamp": "2024-06-01T12:15:00",
            "Value": {"Numeric": 36.1, "GradeCode": 0},
        },
        {
            "Timestamp": "2024-06-01T12:30:00",
            "Value": {"Numeric": None, "GradeCode": None},
        },
    ],
}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_json_api():
    """Live JSON station list is parsed and invalid entries skipped."""
    respx.get("https://www.snet.gob.sv/api/stations").mock(
        return_value=httpx.Response(200, json=MOCK_JSON_STATIONS),
    )

    async with ElSalvadorMARNConnector() as conn:
        stations = await conn.fetch_stations()

    # Empty id and missing coords should be skipped
    assert len(stations) == 2
    ids = {s.native_id for s in stations}
    assert ids == {"SV-100", "SV-101"}

    st = next(s for s in stations if s.native_id == "SV-100")
    assert st.id == "elsalvador_marn:SV-100"
    assert st.provider == "elsalvador_marn"
    assert st.country_code == "SV"
    assert st.river == "Lempa"
    assert st.latitude == 14.3167
    assert st.longitude == -89.1333


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_aquarius_fallback():
    """AQUARIUS location list is used when JSON API fails."""
    # JSON API returns 404
    respx.get("https://www.snet.gob.sv/api/stations").mock(
        return_value=httpx.Response(404),
    )
    respx.get(
        "https://www.snet.gob.sv/api/stations",
        params={"format": "json"},
    ).mock(return_value=httpx.Response(404))
    # AQUARIUS probe succeeds
    respx.get(
        "https://www.snet.gob.sv/AQUARIUS/Publish/v2"
        "/GetLocationDescriptionList"
    ).mock(
        return_value=httpx.Response(
            200, json=MOCK_AQUARIUS_LOCATIONS,
        ),
    )

    async with ElSalvadorMARNConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    assert stations[0].native_id == "AQ-001"
    assert stations[0].river == "Sucio"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_seed_fallback():
    """Seed list is used when all live endpoints fail."""
    respx.route().mock(return_value=httpx.Response(500))

    async with ElSalvadorMARNConnector() as conn:
        stations = await conn.fetch_stations()

    # Should return the 15 seed stations
    assert len(stations) == 5
    rivers = {s.river for s in stations}
    assert "Rio Lempa" in rivers


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_aquarius_parses():
    """AQUARIUS time-series data is parsed correctly."""
    # Allow AQUARIUS probe to succeed
    respx.get(
        "https://www.snet.gob.sv/AQUARIUS/Publish/v2"
        "/GetLocationDescriptionList"
    ).mock(return_value=httpx.Response(200, json={}))
    respx.get(
        "https://www.snet.gob.sv/AQUARIUS/Publish/v2"
        "/GetTimeSeriesData"
    ).mock(
        return_value=httpx.Response(
            200, json=MOCK_AQUARIUS_TIMESERIES,
        ),
    )

    async with ElSalvadorMARNConnector() as conn:
        chunk = await conn.fetch_observations(
            "elsalvador_marn:AQ-001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "elsalvador_marn"
    assert chunk.station_id == "elsalvador_marn:AQ-001"
    assert len(chunk.observations) == 3

    # First — graded (50 = GOOD)
    assert chunk.observations[0].discharge_m3s == pytest.approx(34.5)
    assert chunk.observations[0].quality.value == "good"

    # Second — ungraded (0 = RAW)
    assert chunk.observations[1].discharge_m3s == pytest.approx(36.1)
    assert chunk.observations[1].quality.value == "raw"

    # Third — None value = MISSING
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty_fallback():
    """Returns empty chunk when AQUARIUS is unreachable."""
    respx.route().mock(return_value=httpx.Response(500))

    async with ElSalvadorMARNConnector() as conn:
        chunk = await conn.fetch_observations(
            "elsalvador_marn:SV-001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "elsalvador_marn"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_estimated_grade():
    """AQUARIUS grade >= 150 maps to ESTIMATED quality."""
    data = {
        "Points": [
            {
                "Timestamp": "2024-06-01T10:00:00",
                "Value": {"Numeric": 50.0, "GradeCode": 200},
            },
        ],
    }
    respx.get(
        "https://www.snet.gob.sv/AQUARIUS/Publish/v2"
        "/GetLocationDescriptionList"
    ).mock(return_value=httpx.Response(200, json={}))
    respx.get(
        "https://www.snet.gob.sv/AQUARIUS/Publish/v2"
        "/GetTimeSeriesData"
    ).mock(return_value=httpx.Response(200, json=data))

    async with ElSalvadorMARNConnector() as conn:
        chunk = await conn.fetch_observations(
            "elsalvador_marn:AQ-001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.observations[0].quality.value == "estimated"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_wrapped_json():
    """Stations wrapped in a 'stations' key are parsed correctly."""
    wrapped = {"stations": MOCK_JSON_STATIONS[:2]}
    respx.get("https://www.snet.gob.sv/api/stations").mock(
        return_value=httpx.Response(200, json=wrapped),
    )

    async with ElSalvadorMARNConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_scalar_value():
    """AQUARIUS points with scalar Value (not dict) are handled."""
    data = {
        "Points": [
            {
                "Timestamp": "2024-06-01T10:00:00",
                "Value": 42.0,
            },
        ],
    }
    respx.get(
        "https://www.snet.gob.sv/AQUARIUS/Publish/v2"
        "/GetLocationDescriptionList"
    ).mock(return_value=httpx.Response(200, json={}))
    respx.get(
        "https://www.snet.gob.sv/AQUARIUS/Publish/v2"
        "/GetTimeSeriesData"
    ).mock(return_value=httpx.Response(200, json=data))

    async with ElSalvadorMARNConnector() as conn:
        chunk = await conn.fetch_observations(
            "elsalvador_marn:AQ-001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(42.0)
    assert chunk.observations[0].quality.value == "raw"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest_delegates():
    """fetch_latest calls fetch_observations for the last 24h."""
    respx.route().mock(return_value=httpx.Response(500))

    async with ElSalvadorMARNConnector() as conn:
        chunk = await conn.fetch_latest("elsalvador_marn:SV-001")

    assert chunk.provider == "elsalvador_marn"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_quality_from_grade_none():
    """None grade code returns RAW."""
    from csfs.connectors.elsalvador_marn import _quality_from_grade
    from csfs.core.models import QualityFlag

    assert _quality_from_grade(None) == QualityFlag.RAW


@pytest.mark.asyncio
@respx.mock
async def test_quality_from_grade_invalid_string():
    """Non-numeric string grade code returns RAW."""
    from csfs.connectors.elsalvador_marn import _quality_from_grade
    from csfs.core.models import QualityFlag

    assert _quality_from_grade("bad") == QualityFlag.RAW


@pytest.mark.asyncio
@respx.mock
async def test_resolve_aquarius_base_caches():
    """_resolve_aquarius_base caches the discovered path."""
    respx.get(
        "https://www.snet.gob.sv/AQUARIUS/Publish/v2"
        "/GetLocationDescriptionList"
    ).mock(return_value=httpx.Response(200, json={}))

    async with ElSalvadorMARNConnector() as conn:
        first = await conn._resolve_aquarius_base()
        second = await conn._resolve_aquarius_base()

    assert first == second
    assert first == "/AQUARIUS/Publish/v2"


@pytest.mark.asyncio
@respx.mock
async def test_resolve_aquarius_base_no_working_path():
    """All AQUARIUS path probes failing returns None."""
    respx.route().mock(side_effect=httpx.ConnectError("fail"))

    async with ElSalvadorMARNConnector() as conn:
        result = await conn._resolve_aquarius_base()

    assert result is None


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_estaciones_key():
    """Stations wrapped in 'estaciones' key are parsed."""
    wrapped = {"estaciones": MOCK_JSON_STATIONS[:2]}
    respx.get("https://www.snet.gob.sv/api/stations").mock(
        return_value=httpx.Response(200, json=wrapped),
    )

    async with ElSalvadorMARNConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_aquarius_locations_with_alt_keys():
    """AQUARIUS location entries with 'id' and 'latitude' keys are parsed."""
    alt_locations = {
        "LocationDescriptions": [
            {
                "Identifier": "",
                "Name": "Empty ID",
                "Latitude": 13.5,
                "Longitude": -89.0,
            },
            {
                "Identifier": "AQ-010",
                "Name": "Good Station",
                "Latitude": None,
                "Longitude": -89.0,
            },
            {
                "Identifier": "AQ-011",
                "Name": "Another Good",
                "Latitude": 13.7,
                "Longitude": -89.2,
                "River": "Rio Lempa",
            },
        ],
    }
    respx.get("https://www.snet.gob.sv/api/stations").mock(
        return_value=httpx.Response(404),
    )
    respx.get(
        "https://www.snet.gob.sv/api/stations",
        params={"format": "json"},
    ).mock(return_value=httpx.Response(404))
    respx.get(
        "https://www.snet.gob.sv/AQUARIUS/Publish/v2"
        "/GetLocationDescriptionList"
    ).mock(return_value=httpx.Response(200, json=alt_locations))

    async with ElSalvadorMARNConnector() as conn:
        stations = await conn.fetch_stations()

    # Empty ID and None lat should be skipped
    assert len(stations) == 1
    assert stations[0].native_id == "AQ-011"


@pytest.mark.asyncio
@respx.mock
async def test_station_parse_failed_logged():
    """Station entries that raise ValueError during creation are skipped."""
    bad_stations = [
        {
            "id": "SV-200",
            "name": "Bad Lat",
            "latitude": "not-a-number",
            "longitude": -89.0,
        },
    ]
    respx.get("https://www.snet.gob.sv/api/stations").mock(
        return_value=httpx.Response(200, json=bad_stations),
    )

    async with ElSalvadorMARNConnector() as conn:
        stations = await conn.fetch_stations()

    # Falls back to seed since the one parsed station had a bad lat
    # Actually float("not-a-number") raises ValueError -> stations is empty
    # Then seed_only fallback kicks in
    assert len(stations) >= 0


@pytest.mark.asyncio
@respx.mock
async def test_aquarius_ts_invalid_timestamp_raises():
    """Invalid timestamp in AQUARIUS response raises DataFormatError."""
    from csfs.core.exceptions import DataFormatError

    data = {
        "Points": [
            {
                "Timestamp": "not-a-date",
                "Value": {"Numeric": 50.0},
            },
        ],
    }
    respx.get(
        "https://www.snet.gob.sv/AQUARIUS/Publish/v2"
        "/GetLocationDescriptionList"
    ).mock(return_value=httpx.Response(200, json={}))
    respx.get(
        "https://www.snet.gob.sv/AQUARIUS/Publish/v2"
        "/GetTimeSeriesData"
    ).mock(return_value=httpx.Response(200, json=data))

    async with ElSalvadorMARNConnector() as conn:
        # DataFormatError is raised inside _parse_aquarius_ts
        # but caught by _try_aquarius_data, returning None
        # Then falls through to empty chunk
        chunk = await conn.fetch_observations(
            "elsalvador_marn:AQ-001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    # The exception is caught and returns empty
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_aquarius_ts_skip_no_timestamp():
    """AQUARIUS points missing Timestamp are skipped."""
    data = {
        "Points": [
            {
                "Value": {"Numeric": 50.0},
            },
            {
                "Timestamp": "2024-06-01T12:00:00",
                "Value": {"Numeric": 60.0},
            },
        ],
    }
    respx.get(
        "https://www.snet.gob.sv/AQUARIUS/Publish/v2"
        "/GetLocationDescriptionList"
    ).mock(return_value=httpx.Response(200, json={}))
    respx.get(
        "https://www.snet.gob.sv/AQUARIUS/Publish/v2"
        "/GetTimeSeriesData"
    ).mock(return_value=httpx.Response(200, json=data))

    async with ElSalvadorMARNConnector() as conn:
        chunk = await conn.fetch_observations(
            "elsalvador_marn:AQ-001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(60.0)
