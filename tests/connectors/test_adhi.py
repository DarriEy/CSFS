"""Tests for the ADHI (African Database of Hydrometric Indices) connector."""

from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from csfs.connectors.adhi import _SEED_STATIONS, ADHI_COUNTRY_CODES, ADHIConnector

# ------------------------------------------------------------------
# Mock DataVerse API responses
# ------------------------------------------------------------------

MOCK_DATAVERSE_RESPONSE = {
    "status": "OK",
    "data": {
        "id": 99999,
        "persistentUrl": "https://doi.org/10.23708/LXGXQ9",
        "latestVersion": {
            "versionState": "RELEASED",
            "files": [
                {
                    "dataFile": {
                        "id": 5001,
                        "filename": "ADHI_stations_metadata.tab",
                        "filesize": 245000,
                    },
                },
                {
                    "dataFile": {
                        "id": 5002,
                        "filename": "ADHI_monthly_discharge.tab",
                        "filesize": 12000000,
                    },
                },
                {
                    "dataFile": {
                        "id": 5003,
                        "filename": "README.txt",
                        "filesize": 3200,
                    },
                },
            ],
        },
    },
}

MOCK_STATION_METADATA_TAB = (
    "station_code\tstation_name\tlatitude\tlongitude"
    "\tcountry_code\triver\tcatchment_area\n"
    "ADHI-NG-0001\tNIGER AT LOKOJA\t7.80\t6.74\tNG\tNIGER\t2074000\n"
    "ADHI-KE-0001\tTANA AT GARISSA\t-0.46\t39.64\tKE\tTANA\t32500\n"
    "ADHI-ZA-0001\tORANGE AT VIOOLSDRIF\t-28.77\t17.73"
    "\tZA\tORANGE\t850530\n"
)

MOCK_STATION_METADATA_MISSING_COORDS = (
    "station_code\tstation_name\tlatitude\tlongitude\tcountry_code\n"
    "ADHI-BAD\tNo Coords\t\t\tXX\n"
    "ADHI-OK\tGood Station\t10.0\t20.0\tNG\n"
)

MOCK_DISCHARGE_DATA_TAB = (
    "station_code\tdate\tdischarge\tquality\n"
    "ADHI-NG-0001\t1970-01\t1250.5\t0\n"
    "ADHI-NG-0001\t1970-02\t980.0\t0\n"
    "ADHI-NG-0001\t1970-03\t-999.0\t3\n"
    "ADHI-NG-0001\t1970-04\t1100.2\t1\n"
    "ADHI-KE-0001\t1970-01\t55.3\t0\n"
    "ADHI-KE-0001\t1970-02\t48.1\t0\n"
)

MOCK_DISCHARGE_LOCAL_CSV = (
    "station_code,date,discharge,quality\n"
    "ADHI-NG-0001,1970-01,1250.5,0\n"
    "ADHI-NG-0001,1970-02,980.0,0\n"
    "ADHI-NG-0001,1970-03,-999.0,3\n"
    "ADHI-NG-0001,1970-06,500.0,0\n"
)


# ------------------------------------------------------------------
# Station listing tests
# ------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_seed_fallback():
    """When DataVerse is unavailable, falls back to seed stations."""
    respx.get(
        "https://dataverse.ird.fr/api/datasets/:persistentId/",
    ).mock(side_effect=httpx.ConnectError("connection refused"))

    async with ADHIConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)
    first = stations[0]
    assert first.provider == "adhi"
    assert first.id.startswith("adhi:")


@pytest.mark.asyncio
@respx.mock
async def test_seed_station_ids_are_canonical():
    """Every seed station has properly formatted CSFS station IDs."""
    respx.get(
        "https://dataverse.ird.fr/api/datasets/:persistentId/",
    ).mock(side_effect=httpx.ConnectError("connection refused"))

    async with ADHIConnector() as conn:
        stations = await conn.fetch_stations()

    for station in stations:
        assert station.id == f"adhi:{station.native_id}"
        assert station.provider == "adhi"
        assert station.latitude != 0.0 or station.longitude != 0.0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_from_api():
    """Stations are fetched and parsed from the DataVerse API."""
    respx.get(
        "https://dataverse.ird.fr/api/datasets/:persistentId/",
    ).mock(
        return_value=httpx.Response(200, json=MOCK_DATAVERSE_RESPONSE),
    )
    respx.get(
        "https://dataverse.ird.fr/api/access/datafile/5001",
    ).mock(
        return_value=httpx.Response(
            200, text=MOCK_STATION_METADATA_TAB,
        ),
    )

    async with ADHIConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 3
    ng = next(s for s in stations if s.native_id == "ADHI-NG-0001")
    assert ng.name == "NIGER AT LOKOJA"
    assert ng.country_code == "NG"
    assert ng.river == "NIGER"
    assert ng.latitude == pytest.approx(7.80)
    assert ng.longitude == pytest.approx(6.74)
    assert ng.catchment_area_km2 == pytest.approx(2074000.0)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_missing_coordinates():
    """Stations with missing lat/lon are skipped during parsing."""
    respx.get(
        "https://dataverse.ird.fr/api/datasets/:persistentId/",
    ).mock(
        return_value=httpx.Response(200, json=MOCK_DATAVERSE_RESPONSE),
    )
    respx.get(
        "https://dataverse.ird.fr/api/access/datafile/5001",
    ).mock(
        return_value=httpx.Response(
            200, text=MOCK_STATION_METADATA_MISSING_COORDS,
        ),
    )

    async with ADHIConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "ADHI-OK"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_api_error_falls_back_to_seed():
    """When the DataVerse API returns a server error, seed list is used."""
    respx.get(
        "https://dataverse.ird.fr/api/datasets/:persistentId/",
    ).mock(
        return_value=httpx.Response(500),
    )

    async with ADHIConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)


# ------------------------------------------------------------------
# Observation tests
# ------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_from_api():
    """Discharge data is fetched and parsed from DataVerse."""
    respx.get(
        "https://dataverse.ird.fr/api/datasets/:persistentId/",
    ).mock(
        return_value=httpx.Response(200, json=MOCK_DATAVERSE_RESPONSE),
    )
    respx.get(
        "https://dataverse.ird.fr/api/access/datafile/5002",
    ).mock(
        return_value=httpx.Response(
            200, text=MOCK_DISCHARGE_DATA_TAB,
        ),
    )

    async with ADHIConnector() as conn:
        chunk = await conn.fetch_observations(
            "adhi:ADHI-NG-0001",
            start=datetime(1970, 1, 1, tzinfo=UTC),
            end=datetime(1970, 12, 31, tzinfo=UTC),
        )

    assert chunk.station_id == "adhi:ADHI-NG-0001"
    assert chunk.provider == "adhi"
    assert len(chunk.observations) == 4

    # First obs: good quality
    assert chunk.observations[0].discharge_m3s == pytest.approx(1250.5)
    assert chunk.observations[0].quality.value == "good"

    # Third obs: missing value (-999)
    assert chunk.observations[2].discharge_m3s is None
    assert chunk.observations[2].quality.value == "missing"

    # Fourth obs: estimated
    assert chunk.observations[3].discharge_m3s == pytest.approx(1100.2)
    assert chunk.observations[3].quality.value == "estimated"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_filters_by_station():
    """Only observations for the requested station are returned."""
    respx.get(
        "https://dataverse.ird.fr/api/datasets/:persistentId/",
    ).mock(
        return_value=httpx.Response(200, json=MOCK_DATAVERSE_RESPONSE),
    )
    respx.get(
        "https://dataverse.ird.fr/api/access/datafile/5002",
    ).mock(
        return_value=httpx.Response(
            200, text=MOCK_DISCHARGE_DATA_TAB,
        ),
    )

    async with ADHIConnector() as conn:
        chunk = await conn.fetch_observations(
            "adhi:ADHI-KE-0001",
            start=datetime(1970, 1, 1, tzinfo=UTC),
            end=datetime(1970, 12, 31, tzinfo=UTC),
        )

    assert chunk.station_id == "adhi:ADHI-KE-0001"
    assert len(chunk.observations) == 2
    assert chunk.observations[0].discharge_m3s == pytest.approx(55.3)


@pytest.mark.asyncio
async def test_fetch_observations_from_local_file(tmp_path: Path):
    """Observations are read from a local pre-downloaded CSV file."""
    data_file = tmp_path / "ADHI-NG-0001.csv"
    data_file.write_text(MOCK_DISCHARGE_LOCAL_CSV, encoding="utf-8")

    async with ADHIConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "adhi:ADHI-NG-0001",
            start=datetime(1970, 1, 1, tzinfo=UTC),
            end=datetime(1970, 3, 31, tzinfo=UTC),
        )

    assert chunk.station_id == "adhi:ADHI-NG-0001"
    assert len(chunk.observations) == 3

    # -999 should be mapped to missing
    missing = chunk.observations[2]
    assert missing.discharge_m3s is None
    assert missing.quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_no_data_returns_empty():
    """Without data_dir or API, returns empty chunk."""
    respx.get(
        "https://dataverse.ird.fr/api/datasets/:persistentId/",
    ).mock(side_effect=httpx.ConnectError("connection refused"))

    async with ADHIConnector() as conn:
        chunk = await conn.fetch_observations(
            "adhi:ADHI-XX-9999",
            start=datetime(1970, 1, 1, tzinfo=UTC),
            end=datetime(1970, 12, 31, tzinfo=UTC),
        )

    assert chunk.station_id == "adhi:ADHI-XX-9999"
    assert chunk.provider == "adhi"
    assert len(chunk.observations) == 0


# ------------------------------------------------------------------
# Registration and metadata tests
# ------------------------------------------------------------------


@pytest.mark.asyncio
async def test_connector_registration():
    """The connector is registered under the 'adhi' slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("adhi")
    assert cls is ADHIConnector


@pytest.mark.asyncio
async def test_country_codes_comprehensive():
    """ADHI covers a comprehensive list of African countries."""
    assert len(ADHI_COUNTRY_CODES) >= 40
    # Spot-check major countries
    for code in ("NG", "ZA", "EG", "KE", "ET", "CD", "GH", "TZ"):
        assert code in ADHI_COUNTRY_CODES
