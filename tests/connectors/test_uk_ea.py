"""Tests for the UK Environment Agency connector with mocked HTTP responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.uk_ea import UKEnvironmentAgencyConnector
from csfs.core.exceptions import ConnectorError
from csfs.core.models import Resolution, Variable

MOCK_STATIONS_RESPONSE = {
    "items": [
        {
            "notation": "3400TH",
            "stationReference": "3400TH",
            "label": "Kingston",
            "lat": 51.41,
            "long": -0.31,
            "riverName": "Thames",
            "catchmentArea": 9948.0,
        },
        {
            "notation": "F1707",
            "label": "Bewdley",
            "lat": 52.38,
            "long": -2.32,
            "riverName": ["Severn"],
            "catchmentArea": [4325.0],
        },
        {
            "notation": "",
            "lat": 51.0,
            "long": -1.0,
        },
    ],
    "links": [],
}

MOCK_STATIONS_PAGINATED_P1 = {
    "items": [
        {
            "notation": "3400TH",
            "label": "Kingston",
            "lat": 51.41,
            "long": -0.31,
            "riverName": "Thames",
        },
    ],
    "links": [
        {"rel": "next", "href": "https://environment.data.gov.uk/hydrology/id/stations?_offset=1"},
    ],
}

MOCK_STATIONS_PAGINATED_P2 = {
    "items": [
        {
            "notation": "F1707",
            "label": "Bewdley",
            "lat": 52.38,
            "long": -2.32,
        },
    ],
    "links": [],
}

MOCK_MEASURES_RESPONSE = {
    "items": [
        {"notation": "3400TH-flow-i-900-m3s-qualified", "parameterName": "Water Flow"},
        {"notation": "3400TH-level-i-900-m-qualified", "parameterName": "Water Level"},
    ]
}

MOCK_MEASURES_FLOW_ONLY = {
    "items": [
        {"notation": "3400TH-flow-i-900-m3s-qualified", "parameterName": "Water Flow"},
    ]
}

MOCK_MEASURES_DAILY_FLOW_ONLY = {
    "items": [
        {"notation": "3400TH-flow-m-86400-m3s-qualified", "parameterName": "Water Flow"},
    ]
}

MOCK_MEASURES_NO_FLOW = {
    "items": [
        {"notation": "3400TH-level-i-900-m-qualified", "parameterName": "Water Level"},
    ]
}

MOCK_MEASURES_NEITHER = {
    "items": [
        {"notation": "3400TH-rainfall-t-900-mm-qualified", "parameterName": "Rainfall"},
    ]
}

MOCK_LEVEL_READINGS_RESPONSE = {
    "items": [
        {
            "dateTime": "2024-06-01T12:00:00Z",
            "value": 3.42,
            "quality": "Good",
        },
        {
            "dateTime": "2024-06-01T12:15:00Z",
            "value": 3.44,
            "quality": "Unchecked",
        },
    ],
    "links": [],
}

MOCK_READINGS_RESPONSE = {
    "items": [
        {
            "dateTime": "2024-06-01T12:00:00Z",
            "value": 42.5,
            "quality": "Good",
        },
        {
            "dateTime": "2024-06-01T12:15:00Z",
            "value": 43.1,
            "quality": "Suspect",
        },
        {
            "dateTime": "2024-06-01T12:30:00Z",
            "value": 41.0,
            "quality": "Estimated",
        },
        {
            "dateTime": "2024-06-01T12:45:00Z",
            "value": 40.2,
            "quality": "",
        },
    ],
    "links": [],
}

MOCK_READINGS_MALFORMED = {
    "items": [
        {
            "dateTime": "2024-06-01T12:00:00Z",
            "value": 42.5,
            "quality": "Good",
        },
        {
            "value": 999,
        },
        {
            "dateTime": "not-a-date",
            "value": "not-a-number",
        },
    ],
    "links": [],
}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_correctly():
    respx.get("https://environment.data.gov.uk/hydrology/id/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with UKEnvironmentAgencyConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    kingston = next(s for s in stations if s.native_id == "3400TH")
    assert kingston.id == "uk_ea:3400TH"
    assert kingston.provider == "uk_ea"
    assert kingston.name == "Kingston"
    assert kingston.country_code == "GB"
    assert kingston.river == "Thames"
    assert kingston.catchment_area_km2 == pytest.approx(9948.0)
    assert kingston.latitude == pytest.approx(51.41)
    assert kingston.longitude == pytest.approx(-0.31)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_list_fields():
    """River name and catchment area as lists are handled."""
    respx.get("https://environment.data.gov.uk/hydrology/id/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with UKEnvironmentAgencyConnector() as conn:
        stations = await conn.fetch_stations()

    bewdley = next(s for s in stations if s.native_id == "F1707")
    assert bewdley.river == "Severn"
    assert bewdley.catchment_area_km2 == pytest.approx(4325.0)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_incomplete():
    """Stations without notation or coordinates are skipped."""
    respx.get("https://environment.data.gov.uk/hydrology/id/stations").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with UKEnvironmentAgencyConnector() as conn:
        stations = await conn.fetch_stations()

    assert not any(s.native_id == "" for s in stations)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_pagination():
    """Paginated station responses are followed."""
    route = respx.get(url__startswith="https://environment.data.gov.uk/hydrology/id/stations")
    route.side_effect = [
        httpx.Response(200, json=MOCK_STATIONS_PAGINATED_P1),
        httpx.Response(200, json=MOCK_STATIONS_PAGINATED_P2),
    ]

    async with UKEnvironmentAgencyConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    assert route.call_count == 2


@pytest.mark.asyncio
@respx.mock
async def test_find_flow_measure_prefers_instantaneous():
    """_find_flow_measure picks the preferred measure suffix."""
    respx.get(
        "https://environment.data.gov.uk/hydrology/id/measures",
        params={"station.stationReference": "3400TH"},
    ).mock(
        return_value=httpx.Response(200, json=MOCK_MEASURES_RESPONSE)
    )

    async with UKEnvironmentAgencyConnector() as conn:
        measure = await conn._find_flow_measure("3400TH")

    assert measure == "3400TH-flow-i-900-m3s-qualified"


@pytest.mark.asyncio
@respx.mock
async def test_find_level_measure_prefers_instantaneous():
    """_find_level_measure picks the i-900 level measure."""
    respx.get(
        "https://environment.data.gov.uk/hydrology/id/measures",
        params={"station.stationReference": "3400TH"},
    ).mock(
        return_value=httpx.Response(200, json=MOCK_MEASURES_RESPONSE)
    )

    async with UKEnvironmentAgencyConnector() as conn:
        measure = await conn._find_level_measure("3400TH")

    assert measure == "3400TH-level-i-900-m-qualified"


@pytest.mark.asyncio
@respx.mock
async def test_find_level_measure_returns_none_when_no_level():
    """No level measures returns None."""
    respx.get(
        "https://environment.data.gov.uk/hydrology/id/measures",
        params={"station.stationReference": "3400TH"},
    ).mock(
        return_value=httpx.Response(200, json=MOCK_MEASURES_FLOW_ONLY)
    )

    async with UKEnvironmentAgencyConnector() as conn:
        measure = await conn._find_level_measure("3400TH")

    assert measure is None


@pytest.mark.asyncio
@respx.mock
async def test_find_flow_measure_returns_none_when_no_flow():
    """No flow measures returns None."""
    respx.get(
        "https://environment.data.gov.uk/hydrology/id/measures",
        params={"station.stationReference": "3400TH"},
    ).mock(
        return_value=httpx.Response(200, json=MOCK_MEASURES_NO_FLOW)
    )

    async with UKEnvironmentAgencyConnector() as conn:
        measure = await conn._find_flow_measure("3400TH")

    assert measure is None


@pytest.mark.asyncio
@respx.mock
async def test_find_flow_measure_handles_error():
    """HTTP error in measure discovery returns None."""
    respx.get(
        "https://environment.data.gov.uk/hydrology/id/measures",
        params={"station.stationReference": "3400TH"},
    ).mock(
        return_value=httpx.Response(500)
    )

    async with UKEnvironmentAgencyConnector() as conn:
        measure = await conn._find_flow_measure("3400TH")

    assert measure is None


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_flow_and_level_readings():
    """Discharge and stage are both emitted, tagged with their resolutions."""
    respx.get(
        "https://environment.data.gov.uk/hydrology/id/measures",
        params={"station.stationReference": "3400TH"},
    ).mock(
        return_value=httpx.Response(200, json=MOCK_MEASURES_RESPONSE)
    )
    respx.get(url__startswith="https://environment.data.gov.uk/hydrology/id/measures/3400TH-flow-").mock(
        return_value=httpx.Response(200, json=MOCK_READINGS_RESPONSE)
    )
    respx.get(url__startswith="https://environment.data.gov.uk/hydrology/id/measures/3400TH-level-").mock(
        return_value=httpx.Response(200, json=MOCK_LEVEL_READINGS_RESPONSE)
    )

    async with UKEnvironmentAgencyConnector() as conn:
        chunk = await conn.fetch_observations(
            "uk_ea:3400TH",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert chunk.provider == "uk_ea"
    assert chunk.station_id == "uk_ea:3400TH"

    flows = [o for o in chunk.observations if o.variable is Variable.DISCHARGE]
    assert len(flows) == 4
    assert all(o.resolution is Resolution.INSTANTANEOUS for o in flows)
    assert flows[0].value == pytest.approx(42.5)
    assert flows[0].discharge_m3s == pytest.approx(42.5)
    assert flows[0].quality.value == "good"
    assert flows[1].quality.value == "suspect"
    assert flows[2].quality.value == "estimated"
    assert flows[3].quality.value == "raw"

    stages = [o for o in chunk.observations if o.variable is Variable.STAGE]
    assert len(stages) == 2
    assert all(o.resolution is Resolution.INSTANTANEOUS for o in stages)
    # EA levels are already in metres — no conversion.
    assert stages[0].value == pytest.approx(3.42)
    assert stages[1].value == pytest.approx(3.44)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_daily_mean_flow_resolution():
    """m-86400 flow measures are tagged Resolution.DAILY_MEAN."""
    respx.get(
        "https://environment.data.gov.uk/hydrology/id/measures",
        params={"station.stationReference": "3400TH"},
    ).mock(
        return_value=httpx.Response(200, json=MOCK_MEASURES_DAILY_FLOW_ONLY)
    )
    respx.get(url__startswith="https://environment.data.gov.uk/hydrology/id/measures/").mock(
        return_value=httpx.Response(200, json=MOCK_READINGS_RESPONSE)
    )

    async with UKEnvironmentAgencyConnector() as conn:
        chunk = await conn.fetch_observations(
            "uk_ea:3400TH",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 4
    assert all(o.variable is Variable.DISCHARGE for o in chunk.observations)
    assert all(o.resolution is Resolution.DAILY_MEAN for o in chunk.observations)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_level_only_station_yields_stage():
    """A station with only level measures yields stage instead of raising."""
    respx.get(
        "https://environment.data.gov.uk/hydrology/id/measures",
        params={"station.stationReference": "3400TH"},
    ).mock(
        return_value=httpx.Response(200, json=MOCK_MEASURES_NO_FLOW)
    )
    respx.get(url__startswith="https://environment.data.gov.uk/hydrology/id/measures/").mock(
        return_value=httpx.Response(200, json=MOCK_LEVEL_READINGS_RESPONSE)
    )

    async with UKEnvironmentAgencyConnector() as conn:
        chunk = await conn.fetch_observations(
            "uk_ea:3400TH",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 2
    assert all(o.variable is Variable.STAGE for o in chunk.observations)
    assert chunk.observations[0].value == pytest.approx(3.42)
    assert chunk.observations[0].resolution is Resolution.INSTANTANEOUS


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_no_measure_raises():
    """ConnectorError raised when neither flow nor level measures exist."""
    respx.get(
        "https://environment.data.gov.uk/hydrology/id/measures",
        params={"station.stationReference": "3400TH"},
    ).mock(
        return_value=httpx.Response(200, json=MOCK_MEASURES_NEITHER)
    )

    async with UKEnvironmentAgencyConnector() as conn:
        with pytest.raises(ConnectorError, match="No flow or level measure"):
            await conn.fetch_observations(
                "uk_ea:3400TH",
                start=datetime(2024, 6, 1),
                end=datetime(2024, 6, 2),
            )


@pytest.mark.asyncio
@respx.mock
async def test_parse_readings_skips_malformed():
    """Malformed readings are skipped without raising."""
    respx.get(
        "https://environment.data.gov.uk/hydrology/id/measures",
        params={"station.stationReference": "3400TH"},
    ).mock(
        return_value=httpx.Response(200, json=MOCK_MEASURES_FLOW_ONLY)
    )
    respx.get(url__startswith="https://environment.data.gov.uk/hydrology/id/measures/").mock(
        return_value=httpx.Response(200, json=MOCK_READINGS_MALFORMED)
    )

    async with UKEnvironmentAgencyConnector() as conn:
        chunk = await conn.fetch_observations(
            "uk_ea:3400TH",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(42.5)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_empty():
    respx.get("https://environment.data.gov.uk/hydrology/id/stations").mock(
        return_value=httpx.Response(200, json={"items": [], "links": []})
    )

    async with UKEnvironmentAgencyConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_stale_preferred_flow_measure_falls_back_to_daily_mean():
    """The preferred i-900 flow feed can freeze upstream (seen 2026-06-15);
    an empty preferred measure must fall through to the next candidate."""
    respx.get(
        "https://environment.data.gov.uk/hydrology/id/measures",
        params={"station.stationReference": "3400TH"},
    ).mock(return_value=httpx.Response(200, json={"items": [
        {"notation": "guid-flow-i-900-m3s-qualified", "parameterName": "Flow"},
        {"notation": "guid-flow-m-86400-m3s-qualified", "parameterName": "Flow"},
    ]}))
    respx.get(
        "https://environment.data.gov.uk/hydrology/id/measures/guid-flow-i-900-m3s-qualified/readings"
    ).mock(return_value=httpx.Response(200, json={"items": []}))
    respx.get(
        "https://environment.data.gov.uk/hydrology/id/measures/guid-flow-m-86400-m3s-qualified/readings"
    ).mock(return_value=httpx.Response(200, json={"items": [
        {"dateTime": "2026-07-12T09:00:00", "value": 8.775, "quality": "Good"},
    ]}))

    async with UKEnvironmentAgencyConnector() as conn:
        chunk = await conn.fetch_observations(
            "uk_ea:3400TH",
            datetime(2026, 7, 11, tzinfo=UTC),
            datetime(2026, 7, 13, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    obs = chunk.observations[0]
    assert obs.variable is Variable.DISCHARGE
    assert obs.resolution is Resolution.DAILY_MEAN
    assert obs.value == pytest.approx(8.775)


@pytest.mark.asyncio
@respx.mock
async def test_measures_resolve_for_guid_station_via_path_form():
    """GUID stations (EA 2026-07 migration) resolve via /id/stations/<guid>/measures,
    not the stationReference query — the connector must try both."""
    guid = "052d0819-2a32-47df-9b99-c243c9c8235b"
    base = "https://environment.data.gov.uk/hydrology"
    # Path form works for GUIDs; stationReference query is empty.
    respx.get(f"{base}/id/stations/{guid}/measures").mock(
        return_value=httpx.Response(200, json={"items": [
            {"notation": f"{guid}-flow-i-900-m3s-qualified", "parameterName": "Flow"},
        ]})
    )
    respx.get(
        f"{base}/id/measures", params={"station.stationReference": guid},
    ).mock(return_value=httpx.Response(200, json={"items": []}))
    respx.get(f"{base}/id/measures/{guid}-flow-i-900-m3s-qualified/readings").mock(
        return_value=httpx.Response(200, json={"items": [
            {"dateTime": "2026-05-12T20:00:00", "value": 0.505, "quality": "Good"},
        ]})
    )

    async with UKEnvironmentAgencyConnector() as conn:
        chunk = await conn.fetch_observations(
            f"uk_ea:{guid}",
            datetime(2026, 5, 1, tzinfo=UTC),
            datetime(2026, 5, 20, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].variable is Variable.DISCHARGE
    assert chunk.observations[0].value == pytest.approx(0.505)
