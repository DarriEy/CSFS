"""Tests for Ireland EPA HydroNet / OPW connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.ireland_epa import IrelandEPAConnector

MOCK_GEOJSON_STATIONS = {
    "type": "FeatureCollection",
    "features": [
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [-6.2603, 53.3498],
            },
            "properties": {
                "station_ref": "09001",
                "station_name": "Islandbridge",
                "river_name": "Liffey",
                "catchment_area": 1256.0,
            },
        },
        {
            "type": "Feature",
            "geometry": {
                "type": "Point",
                "coordinates": [-8.6267, 52.6638],
            },
            "properties": {
                "station_ref": "25001",
                "station_name": "Castleconnell",
                "river_name": "Shannon",
            },
        },
    ],
}

MOCK_FLAT_STATIONS = [
    {
        "station_ref": "09001",
        "station_name": "Islandbridge",
        "latitude": 53.3498,
        "longitude": -6.2603,
        "river_name": "Liffey",
    },
]

MOCK_EPA_OBSERVATIONS = [
    {
        "datetime": "2024-06-01T00:00:00",
        "value": 12.5,
        "quality": "good",
    },
    {
        "datetime": "2024-06-02T00:00:00",
        "value": 14.3,
        "quality": "suspect",
    },
    {
        "datetime": "2024-06-03T00:00:00",
        "value": 11.8,
        "quality": "estimated",
    },
]

MOCK_EPA_OBSERVATIONS_WITH_NULL = [
    {
        "datetime": "2024-06-01T00:00:00",
        "value": 12.5,
        "quality": "good",
    },
    {
        "datetime": "2024-06-02T00:00:00",
        "value": None,
        "quality": "good",
    },
]

MOCK_OPW_CSV = (
    "timestamp,value\n"
    "2024-06-01T00:00:00,3.45\n"
    "2024-06-02T00:00:00,3.67\n"
    "2024-06-03T00:00:00,3.12\n"
)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_geojson():
    """Station list is parsed from GeoJSON FeatureCollection."""
    respx.get(
        "https://epawebapp.epa.ie/hydronet/output/internet/layers/10"
    ).mock(
        return_value=httpx.Response(200, json=MOCK_GEOJSON_STATIONS)
    )

    async with IrelandEPAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2

    native_ids = {s.native_id for s in stations}
    assert native_ids == {"09001", "25001"}

    islandbridge = next(
        s for s in stations if s.native_id == "09001"
    )
    assert islandbridge.id == "ireland_epa:09001"
    assert islandbridge.provider == "ireland_epa"
    assert islandbridge.name == "Islandbridge"
    assert islandbridge.country_code == "IE"
    assert islandbridge.river == "Liffey"
    assert islandbridge.latitude == pytest.approx(53.3498)
    assert islandbridge.longitude == pytest.approx(-6.2603)
    assert islandbridge.catchment_area_km2 == pytest.approx(1256.0)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty GeoJSON returns no stations."""
    respx.get(
        "https://epawebapp.epa.ie/hydronet/output/internet/layers/10"
    ).mock(
        return_value=httpx.Response(
            200, json={"type": "FeatureCollection", "features": []},
        )
    )

    async with IrelandEPAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_missing_ref():
    """Stations without station_ref are silently skipped."""
    data = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [-6.26, 53.35],
                },
                "properties": {
                    "station_name": "Ghost",
                    "river_name": "Nowhere",
                },
            },
            {
                "type": "Feature",
                "geometry": {
                    "type": "Point",
                    "coordinates": [-8.63, 52.66],
                },
                "properties": {
                    "station_ref": "25001",
                    "station_name": "Castleconnell",
                },
            },
        ],
    }
    respx.get(
        "https://epawebapp.epa.ie/hydronet/output/internet/layers/10"
    ).mock(return_value=httpx.Response(200, json=data))

    async with IrelandEPAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "25001"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_falls_back_to_esri():
    """When GeoJSON endpoint fails, fallback Esri endpoint is used."""
    respx.get(
        "https://epawebapp.epa.ie/hydronet/output/internet/layers/10"
    ).mock(return_value=httpx.Response(500))

    respx.get("https://epawebapp.epa.ie/Esri/data.ashx").mock(
        return_value=httpx.Response(200, json=MOCK_FLAT_STATIONS)
    )

    async with IrelandEPAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "09001"
    assert stations[0].name == "Islandbridge"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_epa_json():
    """EPA HydroNet observations are correctly parsed with quality."""
    respx.get(
        "https://epawebapp.epa.ie/hydronet/output/internet/data/09001"
    ).mock(
        return_value=httpx.Response(200, json=MOCK_EPA_OBSERVATIONS)
    )

    async with IrelandEPAConnector() as conn:
        chunk = await conn.fetch_observations(
            "ireland_epa:09001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 3),
        )

    assert chunk.provider == "ireland_epa"
    assert chunk.station_id == "ireland_epa:09001"
    assert len(chunk.observations) == 3

    assert chunk.observations[0].discharge_m3s == pytest.approx(12.5)
    assert chunk.observations[0].quality.value == "good"

    assert chunk.observations[1].discharge_m3s == pytest.approx(14.3)
    assert chunk.observations[1].quality.value == "suspect"

    assert chunk.observations[2].discharge_m3s == pytest.approx(11.8)
    assert chunk.observations[2].quality.value == "estimated"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_null_value_is_missing():
    """A null value in EPA observations yields MISSING quality."""
    respx.get(
        "https://epawebapp.epa.ie/hydronet/output/internet/data/09001"
    ).mock(
        return_value=httpx.Response(
            200, json=MOCK_EPA_OBSERVATIONS_WITH_NULL,
        )
    )

    async with IrelandEPAConnector() as conn:
        chunk = await conn.fetch_observations(
            "ireland_epa:09001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 3),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[1].discharge_m3s is None
    assert chunk.observations[1].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty_epa_response():
    """Empty EPA observation array returns zero observations."""
    respx.get(
        "https://epawebapp.epa.ie/hydronet/output/internet/data/09001"
    ).mock(return_value=httpx.Response(200, json=[]))

    async with IrelandEPAConnector() as conn:
        chunk = await conn.fetch_observations(
            "ireland_epa:09001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_falls_back_to_opw():
    """When EPA observations fail, OPW CSV endpoint is used."""
    respx.get(
        "https://epawebapp.epa.ie/hydronet/output/internet/data/09001"
    ).mock(return_value=httpx.Response(500))

    respx.get(
        "https://waterlevel.ie/data/month/09001/2024/06"
    ).mock(return_value=httpx.Response(200, text=MOCK_OPW_CSV))

    async with IrelandEPAConnector() as conn:
        chunk = await conn.fetch_observations(
            "ireland_epa:09001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 3),
        )

    assert chunk.provider == "ireland_epa"
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(3.45)
    assert chunk.observations[0].quality.value == "raw"


@pytest.mark.asyncio
@respx.mock
async def test_opw_csv_filters_by_date_range():
    """OPW CSV observations outside the requested range are excluded."""
    csv_text = (
        "timestamp,value\n"
        "2024-05-30T00:00:00,1.00\n"
        "2024-06-01T00:00:00,3.45\n"
        "2024-06-02T00:00:00,3.67\n"
        "2024-06-10T00:00:00,9.99\n"
    )

    respx.get(
        "https://epawebapp.epa.ie/hydronet/output/internet/data/09001"
    ).mock(return_value=httpx.Response(500))

    respx.get(
        "https://waterlevel.ie/data/month/09001/2024/06"
    ).mock(return_value=httpx.Response(200, text=csv_text))

    async with IrelandEPAConnector() as conn:
        chunk = await conn.fetch_observations(
            "ireland_epa:09001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 5),
        )

    assert len(chunk.observations) == 2
    values = [o.discharge_m3s for o in chunk.observations]
    assert values == [pytest.approx(3.45), pytest.approx(3.67)]


@pytest.mark.asyncio
@respx.mock
async def test_opw_csv_handles_comment_lines():
    """Lines starting with '#' in OPW CSV are ignored."""
    csv_text = (
        "# Station 09001\n"
        "# Units: m3/s\n"
        "timestamp,value\n"
        "2024-06-01T00:00:00,3.45\n"
    )

    respx.get(
        "https://epawebapp.epa.ie/hydronet/output/internet/data/09001"
    ).mock(return_value=httpx.Response(500))

    respx.get(
        "https://waterlevel.ie/data/month/09001/2024/06"
    ).mock(return_value=httpx.Response(200, text=csv_text))

    async with IrelandEPAConnector() as conn:
        chunk = await conn.fetch_observations(
            "ireland_epa:09001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 30),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(3.45)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_date_params():
    """EPA data endpoint receives correct from/to date parameters."""
    route = respx.get(
        "https://epawebapp.epa.ie/hydronet/output/internet/data/09001"
    ).mock(return_value=httpx.Response(200, json=[]))

    async with IrelandEPAConnector() as conn:
        await conn.fetch_observations(
            "ireland_epa:09001",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 7),
        )

    assert route.called
    request = route.calls[0].request
    url_str = str(request.url)
    assert "from=2024-06-01" in url_str
    assert "to=2024-06-07" in url_str
    assert "type=flow" in url_str


@pytest.mark.asyncio
@respx.mock
async def test_opw_spans_multiple_months():
    """OPW fetches span multiple months when the range crosses a boundary."""
    csv_may = (
        "timestamp,value\n"
        "2024-05-31T00:00:00,2.00\n"
    )
    csv_june = (
        "timestamp,value\n"
        "2024-06-01T00:00:00,3.00\n"
    )

    respx.get(
        "https://epawebapp.epa.ie/hydronet/output/internet/data/09001"
    ).mock(return_value=httpx.Response(500))

    respx.get(
        "https://waterlevel.ie/data/month/09001/2024/05"
    ).mock(return_value=httpx.Response(200, text=csv_may))

    respx.get(
        "https://waterlevel.ie/data/month/09001/2024/06"
    ).mock(return_value=httpx.Response(200, text=csv_june))

    async with IrelandEPAConnector() as conn:
        chunk = await conn.fetch_observations(
            "ireland_epa:09001",
            start=datetime(2024, 5, 31),
            end=datetime(2024, 6, 1),
        )

    assert len(chunk.observations) == 2
    values = sorted(o.discharge_m3s for o in chunk.observations)
    assert values == [pytest.approx(2.0), pytest.approx(3.0)]


@pytest.mark.asyncio
@respx.mock
async def test_connector_registered():
    """Connector is discoverable via the registry."""
    from csfs.core.registry import get_connector

    cls = get_connector("ireland_epa")
    assert cls is IrelandEPAConnector
