"""Tests for Environment Canada connector with mocked HTTP responses."""

from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx

from csfs.connectors.environment_canada import EnvironmentCanadaConnector

MOCK_STATIONS_RESPONSE = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [-75.6972, 45.3876],
            },
            "properties": {
                "STATION_NUMBER": "02LA004",
                "STATION_NAME": "RIDEAU RIVER AT OTTAWA",
                "PROV_TERR_STATE_LOC": "ON",
                "DRAINAGE_AREA_GROSS": 3830.0,
            },
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [-123.1207, 49.2827],
            },
            "properties": {
                "STATION_NUMBER": "08MH024",
                "STATION_NAME": "FRASER RIVER AT HOPE",
                "PROV_TERR_STATE_LOC": "BC",
                "DRAINAGE_AREA_GROSS": 217000.0,
            },
        },
    ],
}

MOCK_DAILY_MEAN_RESPONSE = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-75.6972, 45.3876]},
            "properties": {
                "STATION_NUMBER": "02LA004",
                "DATE": "2024-06-01T00:00:00Z",
                "DISCHARGE": 12.5,
                "DISCHARGE_SYMBOL_EN": "A",
            },
        },
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-75.6972, 45.3876]},
            "properties": {
                "STATION_NUMBER": "02LA004",
                "DATE": "2024-06-02T00:00:00Z",
                "DISCHARGE": 11.8,
                "DISCHARGE_SYMBOL_EN": "E",
            },
        },
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-75.6972, 45.3876]},
            "properties": {
                "STATION_NUMBER": "02LA004",
                "DATE": "2024-06-03T00:00:00Z",
                "DISCHARGE": None,
                "DISCHARGE_SYMBOL_EN": "",
            },
        },
    ],
}

MOCK_EMPTY_RESPONSE = {
    "type": "FeatureCollection",
    "features": [],
}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_features():
    respx.get("https://api.weather.gc.ca/collections/hydrometric-stations/items").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with EnvironmentCanadaConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2

    rideau = stations[0]
    assert rideau.native_id == "02LA004"
    assert rideau.name == "RIDEAU RIVER AT OTTAWA"
    assert rideau.id == "environment_canada:02LA004"
    assert rideau.provider == "environment_canada"
    assert rideau.country_code == "CA"
    assert rideau.latitude == pytest.approx(45.3876)
    assert rideau.longitude == pytest.approx(-75.6972)
    assert rideau.catchment_area_km2 == pytest.approx(3830.0)

    fraser = stations[1]
    assert fraser.native_id == "08MH024"
    assert fraser.catchment_area_km2 == pytest.approx(217000.0)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_empty():
    respx.get("https://api.weather.gc.ca/collections/hydrometric-stations/items").mock(
        return_value=httpx.Response(200, json=MOCK_EMPTY_RESPONSE)
    )

    async with EnvironmentCanadaConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_daily_mean():
    respx.get("https://api.weather.gc.ca/collections/hydrometric-daily-mean/items").mock(
        return_value=httpx.Response(200, json=MOCK_DAILY_MEAN_RESPONSE)
    )

    async with EnvironmentCanadaConnector() as conn:
        chunk = await conn.fetch_observations(
            "environment_canada:02LA004",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 3),
        )

    assert chunk.station_id == "environment_canada:02LA004"
    assert chunk.provider == "environment_canada"
    assert len(chunk.observations) == 3

    # First observation: approved, good quality
    assert chunk.observations[0].discharge_m3s == pytest.approx(12.5)
    assert chunk.observations[0].quality.value == "good"

    # Second observation: estimated
    assert chunk.observations[1].discharge_m3s == pytest.approx(11.8)
    assert chunk.observations[1].quality.value == "estimated"

    # Third observation: null discharge -> missing
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty():
    respx.get("https://api.weather.gc.ca/collections/hydrometric-daily-mean/items").mock(
        return_value=httpx.Response(200, json=MOCK_EMPTY_RESPONSE)
    )

    async with EnvironmentCanadaConnector() as conn:
        chunk = await conn.fetch_observations(
            "environment_canada:02LA004",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 3),
        )

    assert len(chunk.observations) == 0
    assert chunk.station_id == "environment_canada:02LA004"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_pagination():
    """Verify the connector paginates when a full page is returned."""
    # Build a full page of 500 features
    full_page = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-75.0, 45.0]},
                "properties": {
                    "STATION_NUMBER": f"0{i:04d}",
                    "STATION_NAME": f"STATION {i}",
                    "PROV_TERR_STATE_LOC": "ON",
                    "DRAINAGE_AREA_GROSS": None,
                },
            }
            for i in range(500)
        ],
    }

    route = respx.get(
        "https://api.weather.gc.ca/collections/hydrometric-stations/items"
    )
    route.side_effect = [
        httpx.Response(200, json=full_page),
        httpx.Response(200, json=MOCK_STATIONS_RESPONSE),  # partial page -> stop
    ]

    async with EnvironmentCanadaConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 502  # 500 + 2
    assert route.call_count == 2


@pytest.mark.asyncio
@respx.mock
async def test_quality_flag_mapping():
    """Verify all quality symbol mappings work correctly."""
    features = []
    symbols_and_expected = [
        ("A", "good"),
        ("B", "estimated"),
        ("D", "suspect"),
        ("E", "estimated"),
        ("R", "suspect"),
        ("S", "suspect"),
        ("", "raw"),
    ]
    for i, (symbol, _) in enumerate(symbols_and_expected):
        features.append({
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-75.0, 45.0]},
            "properties": {
                "STATION_NUMBER": "02LA004",
                "DATE": f"2024-06-{i + 1:02d}T00:00:00Z",
                "DISCHARGE": 10.0,
                "DISCHARGE_SYMBOL_EN": symbol,
            },
        })

    respx.get("https://api.weather.gc.ca/collections/hydrometric-daily-mean/items").mock(
        return_value=httpx.Response(200, json={
            "type": "FeatureCollection",
            "features": features,
        })
    )

    async with EnvironmentCanadaConnector() as conn:
        chunk = await conn.fetch_observations(
            "environment_canada:02LA004",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 7),
        )

    for obs, (_, expected_quality) in zip(chunk.observations, symbols_and_expected):
        assert obs.quality.value == expected_quality, (
            f"Expected {expected_quality} but got {obs.quality.value}"
        )


MOCK_REALTIME_RESPONSE = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-75.6972, 45.3876]},
            "properties": {
                "STATION_NUMBER": "02LA004",
                "DATETIME": "2026-05-25T12:00:00Z",
                "DISCHARGE": 18.5,
                "DISCHARGE_SYMBOL_EN": "A",
            },
        },
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-75.6972, 45.3876]},
            "properties": {
                "STATION_NUMBER": "02LA004",
                "DATETIME": "2026-05-25T12:05:00Z",
                "DISCHARGE": 19.0,
                "DISCHARGE_SYMBOL_EN": None,
            },
        },
    ],
}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_with_province_filter():
    """Province filter param is passed when configured."""
    route = respx.get(
        "https://api.weather.gc.ca/collections/hydrometric-stations/items"
    )
    route.mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE),
    )

    async with EnvironmentCanadaConnector(config={"provinces": ["ON"]}) as conn:
        stations = await conn.fetch_stations(provinces=["ON"])

    assert len(stations) == 2
    assert route.call_count == 1


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_realtime_recent():
    """Recent observations use the realtime endpoint."""
    respx.get(
        "https://api.weather.gc.ca/collections/hydrometric-realtime/items"
    ).mock(
        return_value=httpx.Response(200, json=MOCK_REALTIME_RESPONSE),
    )

    # Use dates well within the 30-day realtime window
    now = datetime.now(UTC)
    start = now - timedelta(hours=12)
    end = now

    async with EnvironmentCanadaConnector() as conn:
        chunk = await conn.fetch_observations(
            "environment_canada:02LA004",
            start=start,
            end=end,
        )

    assert chunk.provider == "environment_canada"
    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(18.5)
    assert chunk.observations[0].quality.value == "good"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_mixed_hist_and_realtime():
    """Date ranges spanning the cutoff fetch from both daily-mean and realtime."""
    rt_route = respx.get(
        "https://api.weather.gc.ca/collections/hydrometric-realtime/items"
    )
    rt_route.mock(
        return_value=httpx.Response(200, json=MOCK_REALTIME_RESPONSE),
    )
    dm_route = respx.get(
        "https://api.weather.gc.ca/collections/hydrometric-daily-mean/items"
    )
    dm_route.mock(
        return_value=httpx.Response(200, json=MOCK_DAILY_MEAN_RESPONSE),
    )

    now = datetime.now(UTC)
    # Start before the 30-day cutoff, end in the realtime window
    start = now - timedelta(days=60)
    end = now

    async with EnvironmentCanadaConnector() as conn:
        chunk = await conn.fetch_observations(
            "environment_canada:02LA004",
            start=start,
            end=end,
        )

    assert chunk.provider == "environment_canada"
    # Combines daily-mean and realtime observations
    assert len(chunk.observations) == 5  # 3 daily-mean + 2 realtime
    assert rt_route.call_count >= 1
    assert dm_route.call_count >= 1


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest():
    """fetch_latest fetches recent 24 hours of observations."""
    respx.get(
        "https://api.weather.gc.ca/collections/hydrometric-realtime/items"
    ).mock(
        return_value=httpx.Response(200, json=MOCK_REALTIME_RESPONSE),
    )

    async with EnvironmentCanadaConnector() as conn:
        chunk = await conn.fetch_latest("environment_canada:02LA004")

    assert chunk.provider == "environment_canada"
    assert len(chunk.observations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_realtime_pagination():
    """Realtime observations paginate when a full page (2000) is returned."""
    full_page_features = [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-75.0, 45.0]},
            "properties": {
                "STATION_NUMBER": "02LA004",
                "DATETIME": f"2026-05-25T{(i % 24):02d}:{(i % 60):02d}:00Z",
                "DISCHARGE": 10.0 + i * 0.1,
                "DISCHARGE_SYMBOL_EN": "A",
            },
        }
        for i in range(2000)
    ]
    full_page = {"type": "FeatureCollection", "features": full_page_features}
    partial_page = {"type": "FeatureCollection", "features": MOCK_REALTIME_RESPONSE["features"]}

    route = respx.get(
        "https://api.weather.gc.ca/collections/hydrometric-realtime/items"
    )
    route.side_effect = [
        httpx.Response(200, json=full_page),
        httpx.Response(200, json=partial_page),
    ]

    now = datetime.now(UTC)

    async with EnvironmentCanadaConnector() as conn:
        chunk = await conn.fetch_observations(
            "environment_canada:02LA004",
            start=now - timedelta(hours=6),
            end=now,
        )

    assert len(chunk.observations) == 2002  # 2000 + 2
    assert route.call_count == 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_daily_mean_pagination():
    """Daily-mean observations paginate when a full page (1000) is returned."""
    full_page_features = [
        {
            "type": "Feature",
            "geometry": {"type": "Point", "coordinates": [-75.0, 45.0]},
            "properties": {
                "STATION_NUMBER": "02LA004",
                "DATE": f"2020-{(i % 12) + 1:02d}-{(i % 28) + 1:02d}T00:00:00Z",
                "DISCHARGE": 10.0 + i,
                "DISCHARGE_SYMBOL_EN": "A",
            },
        }
        for i in range(1000)
    ]
    full_page = {"type": "FeatureCollection", "features": full_page_features}
    partial_page = {"type": "FeatureCollection", "features": MOCK_DAILY_MEAN_RESPONSE["features"]}

    route = respx.get(
        "https://api.weather.gc.ca/collections/hydrometric-daily-mean/items"
    )
    route.side_effect = [
        httpx.Response(200, json=full_page),
        httpx.Response(200, json=partial_page),
    ]

    async with EnvironmentCanadaConnector() as conn:
        chunk = await conn.fetch_observations(
            "environment_canada:02LA004",
            start=datetime(2020, 1, 1),
            end=datetime(2020, 12, 31),
        )

    assert len(chunk.observations) == 1003  # 1000 + 3
    assert route.call_count == 2


@pytest.mark.asyncio
@respx.mock
async def test_parse_station_feature_missing_native_id():
    """Features with missing STATION_NUMBER are skipped."""
    data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-75.0, 45.0]},
                "properties": {
                    "STATION_NAME": "Missing ID",
                    "PROV_TERR_STATE_LOC": "ON",
                },
            },
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": []},
                "properties": {
                    "STATION_NUMBER": "02LA999",
                    "STATION_NAME": "Missing coords",
                },
            },
        ],
    }
    respx.get(
        "https://api.weather.gc.ca/collections/hydrometric-stations/items"
    ).mock(return_value=httpx.Response(200, json=data))

    async with EnvironmentCanadaConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_parse_realtime_feature_missing_datetime():
    """Realtime features without DATETIME are skipped."""
    data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-75.0, 45.0]},
                "properties": {
                    "STATION_NUMBER": "02LA004",
                    "DISCHARGE": 10.0,
                },
            },
        ],
    }
    respx.get(
        "https://api.weather.gc.ca/collections/hydrometric-realtime/items"
    ).mock(return_value=httpx.Response(200, json=data))

    now = datetime.now(UTC)

    async with EnvironmentCanadaConnector() as conn:
        chunk = await conn.fetch_observations(
            "environment_canada:02LA004",
            start=now - timedelta(hours=1),
            end=now,
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_parse_realtime_null_discharge():
    """Realtime features with null DISCHARGE get MISSING quality."""
    data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-75.0, 45.0]},
                "properties": {
                    "STATION_NUMBER": "02LA004",
                    "DATETIME": "2026-05-25T12:00:00Z",
                    "DISCHARGE": None,
                    "DISCHARGE_SYMBOL_EN": "A",
                },
            },
        ],
    }
    respx.get(
        "https://api.weather.gc.ca/collections/hydrometric-realtime/items"
    ).mock(return_value=httpx.Response(200, json=data))

    now = datetime.now(UTC)

    async with EnvironmentCanadaConnector() as conn:
        chunk = await conn.fetch_observations(
            "environment_canada:02LA004",
            start=now - timedelta(hours=1),
            end=now,
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[0].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_parse_daily_mean_missing_date():
    """Daily-mean features without DATE are skipped."""
    data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {"type": "Point", "coordinates": [-75.0, 45.0]},
                "properties": {
                    "STATION_NUMBER": "02LA004",
                    "DISCHARGE": 10.0,
                },
            },
        ],
    }
    respx.get(
        "https://api.weather.gc.ca/collections/hydrometric-daily-mean/items"
    ).mock(return_value=httpx.Response(200, json=data))

    async with EnvironmentCanadaConnector() as conn:
        chunk = await conn.fetch_observations(
            "environment_canada:02LA004",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 3),
        )

    assert len(chunk.observations) == 0
