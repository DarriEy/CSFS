"""Tests for the EStreams (European Streamflow Dataset) connector."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.estreams import EStreamsConnector
from csfs.core.exceptions import ConnectorError, DataFormatError

# ------------------------------------------------------------------
# Mock data
# ------------------------------------------------------------------

MOCK_ZENODO_RECORD = {
    "id": 13154470,
    "files": [
        {
            "key": "EStreams_gauging_stations.csv",
            "links": {
                "self": "https://zenodo.org/api/records/13154470/files/EStreams_gauging_stations.csv/content",
            },
        },
        {
            "key": "EStreams_streamflow_indices.csv",
            "links": {
                "self": "https://zenodo.org/api/records/13154470/files/EStreams_streamflow_indices.csv/content",
            },
        },
    ],
}

MOCK_CATALOGUE_CSV = (
    "provider_id,code_basins,provider_country,provider_name,"
    "lat,lon,river_name,catchment_area\n"
    "LU_ADMIN,LU001,LU,Ettelbruck,49.8472,6.1042,Alzette,356.0\n"
    "LU_ADMIN,LU002,LU,Mersch,49.7500,6.1000,Alzette,235.5\n"
    "AL_HYDRO,AL001,AL,Permet,40.2339,20.3514,Vjosa,1520.0\n"
    "ME_HYDRO,ME001,ME,Podgorica,42.4304,19.2594,Moraca,2628.0\n"
    "MK_HYDRO,MK001,MK,Skopje,41.9981,21.4254,Vardar,4100.0\n"
    "DE_ADMIN,DE001,DE,Koeln,50.9375,6.9603,Rhein,144000.0\n"
    "AT_ADMIN,AT001,AT,Wien,48.2100,16.3800,Donau,101700.0\n"
)

MOCK_CATALOGUE_CSV_MINIMAL = (
    "code_basins,provider_country,provider_name,lat,lon\n"
    "LU099,LU,Remich,49.5450,6.3670\n"
)

MOCK_CATALOGUE_CSV_MISSING_COORDS = (
    "code_basins,provider_country,provider_name,lat,lon\n"
    "LU100,LU,BadStation,,\n"
    "LU101,LU,GoodStation,49.6,6.1\n"
)


# ------------------------------------------------------------------
# Tests
# ------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_filters_target_countries():
    """Only stations from LU, AL, ME, MK are returned."""
    respx.get("https://zenodo.org/api/records/13154470").mock(
        return_value=httpx.Response(200, json=MOCK_ZENODO_RECORD),
    )
    catalogue_url = MOCK_ZENODO_RECORD["files"][0]["links"]["self"]
    respx.get(catalogue_url).mock(
        return_value=httpx.Response(200, text=MOCK_CATALOGUE_CSV),
    )

    async with EStreamsConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 5
    country_codes = {s.country_code for s in stations}
    assert country_codes == {"LU", "AL", "ME", "MK"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_field_mapping():
    """Station fields are correctly mapped from the catalogue CSV."""
    respx.get("https://zenodo.org/api/records/13154470").mock(
        return_value=httpx.Response(200, json=MOCK_ZENODO_RECORD),
    )
    catalogue_url = MOCK_ZENODO_RECORD["files"][0]["links"]["self"]
    respx.get(catalogue_url).mock(
        return_value=httpx.Response(200, text=MOCK_CATALOGUE_CSV),
    )

    async with EStreamsConnector() as conn:
        stations = await conn.fetch_stations()

    lu = next(s for s in stations if s.native_id == "LU001")
    assert lu.id == "estreams:LU001"
    assert lu.provider == "estreams"
    assert lu.name == "Ettelbruck"
    assert lu.country_code == "LU"
    assert lu.river == "Alzette"
    assert lu.latitude == pytest.approx(49.8472)
    assert lu.longitude == pytest.approx(6.1042)
    assert lu.catchment_area_km2 == pytest.approx(356.0)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_missing_coordinates():
    """Stations with missing lat/lon are skipped."""
    respx.get("https://zenodo.org/api/records/13154470").mock(
        return_value=httpx.Response(200, json=MOCK_ZENODO_RECORD),
    )
    catalogue_url = MOCK_ZENODO_RECORD["files"][0]["links"]["self"]
    respx.get(catalogue_url).mock(
        return_value=httpx.Response(
            200, text=MOCK_CATALOGUE_CSV_MISSING_COORDS,
        ),
    )

    async with EStreamsConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "LU101"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_zenodo_api_error():
    """ConnectorError raised when Zenodo API returns an error."""
    respx.get("https://zenodo.org/api/records/13154470").mock(
        return_value=httpx.Response(500),
    )

    async with EStreamsConnector() as conn:
        with pytest.raises(ConnectorError):
            await conn.fetch_stations()


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_no_csv_in_record():
    """DataFormatError raised when record has no CSV files."""
    record_no_csv = {
        "id": 13154470,
        "files": [
            {
                "key": "readme.txt",
                "links": {"self": "https://zenodo.org/readme.txt"},
            },
        ],
    }
    respx.get("https://zenodo.org/api/records/13154470").mock(
        return_value=httpx.Response(200, json=record_no_csv),
    )

    async with EStreamsConnector() as conn:
        with pytest.raises(DataFormatError):
            await conn.fetch_stations()


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_returns_empty():
    """fetch_observations returns an empty TimeSeriesChunk (no raw Q)."""
    async with EStreamsConnector() as conn:
        chunk = await conn.fetch_observations(
            "estreams:LU001",
            start=datetime(2024, 1, 1),
            end=datetime(2024, 12, 31),
        )

    assert chunk.station_id == "estreams:LU001"
    assert chunk.provider == "estreams"
    assert len(chunk.observations) == 0
    assert chunk.fetched_at is not None


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_minimal_columns():
    """Stations are parsed even when optional columns are absent."""
    respx.get("https://zenodo.org/api/records/13154470").mock(
        return_value=httpx.Response(200, json=MOCK_ZENODO_RECORD),
    )
    catalogue_url = MOCK_ZENODO_RECORD["files"][0]["links"]["self"]
    respx.get(catalogue_url).mock(
        return_value=httpx.Response(200, text=MOCK_CATALOGUE_CSV_MINIMAL),
    )

    async with EStreamsConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    st = stations[0]
    assert st.native_id == "LU099"
    assert st.name == "Remich"
    assert st.river is None
    assert st.catchment_area_km2 is None


@pytest.mark.asyncio
@respx.mock
async def test_connector_registration():
    """The connector is registered under the 'estreams' slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("estreams")
    assert cls is EStreamsConnector
