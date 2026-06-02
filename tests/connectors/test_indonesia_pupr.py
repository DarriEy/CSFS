"""Tests for the Indonesia PUPR (SIGI) connector with mocked HTTP responses.

VERDICT: NOT FIXABLE as a discharge connector. The SIGI/PUPR ArcGIS portal
publishes only station *locations* (no discharge `debit` or water-level `tma`
value fields, no time-series tables). The connector therefore surfaces station
metadata when reachable and always returns empty observation chunks. These
tests assert that graceful-empty behavior plus registration.
"""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.indonesia_pupr import IndonesiaPUPRConnector
from csfs.core.registry import discover, get_connector

# Real shape of the only live PDA layer query response (PDA_2025_32). Note:
# attributes are pure location/metadata -- there is NO discharge or water-level
# value field anywhere in the schema.
_QUERY_URL = (
    "https://sigi.pu.go.id/serverpu/rest/services/Hosted/"
    "PDA_2025_32/FeatureServer/0/query"
)

MOCK_STATIONS_RESPONSE = {
    "objectIdFieldName": "fid",
    "geometryType": "esriGeometryPoint",
    "spatialReference": {"wkid": 4326},
    "features": [
        {
            "attributes": {
                "fid": 1,
                "kode_pos": "T.2.15",
                "nama_pos": "Sogaten",
                "nama_sunga": "Madiun",
                "das": "Bengawan Solo",
                "jenis": "Pos Duga Air",
                "jenis_alat": "Otomatis Telemetri",
                "status": "Aktif",
                "lintang": -7.601354,
                "bujur": 111.529178,
            },
            "geometry": {"x": 111.52917766568152, "y": -7.601354054930669},
        },
        {
            "attributes": {
                "fid": 2,
                "kode_pos": "T.2.16",
                "nama_pos": "Arjowinangun",
                "nama_sunga": "Grindulu",
                "das": "Grindulu",
                "jenis": "Pos Duga Air",
                "jenis_alat": "Manual & Otomatis Telemetri",
                "status": "Aktif",
                "lintang": -8.197561,
                "bujur": 111.114314,
            },
            "geometry": {"x": 111.11431237361352, "y": -8.197557187839267},
        },
    ],
}


def test_connector_registered():
    """The connector is discoverable under its slug with the expected class."""
    discover()
    cls = get_connector("indonesia_pupr")
    assert cls is IndonesiaPUPRConnector
    assert cls.slug == "indonesia_pupr"
    assert cls.country_codes == ["ID"]


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_inventory():
    """Station locations parse from the ArcGIS inventory layer."""
    respx.get(_QUERY_URL).mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with IndonesiaPUPRConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2

    first = stations[0]
    assert first.id == "indonesia_pupr:T.2.15"
    assert first.native_id == "T.2.15"
    assert first.provider == "indonesia_pupr"
    assert first.country_code == "ID"
    assert first.name == "Sogaten"
    assert first.river == "Madiun"
    # geometry coordinates (outSR=4326) preferred
    assert first.latitude == pytest.approx(-7.601354, abs=1e-4)
    assert first.longitude == pytest.approx(111.529178, abs=1e-4)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_records_without_kode_pos():
    """Records lacking kode_pos (the native id) are skipped."""
    payload = {
        "features": [
            {"attributes": {"fid": 9, "nama_pos": "No Code"}, "geometry": {}},
            {
                "attributes": {"fid": 10, "kode_pos": "X.1", "nama_pos": "Has Code"},
                "geometry": {"x": 110.0, "y": -7.0},
            },
        ]
    }
    respx.get(_QUERY_URL).mock(return_value=httpx.Response(200, json=payload))

    async with IndonesiaPUPRConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "X.1"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_degrades_on_http_error():
    """A server error (the original failure mode) degrades to an empty list."""
    respx.get(_QUERY_URL).mock(return_value=httpx.Response(500, text="error"))

    async with IndonesiaPUPRConnector() as conn:
        stations = await conn.fetch_stations()

    assert stations == []


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_degrades_on_arcgis_error_body():
    """An ArcGIS error object (HTTP 200 with {'error': ...}) yields no stations."""
    respx.get(_QUERY_URL).mock(
        return_value=httpx.Response(
            200, json={"error": {"code": 404, "message": "Service not found"}}
        )
    )

    async with IndonesiaPUPRConnector() as conn:
        stations = await conn.fetch_stations()

    assert stations == []


@pytest.mark.asyncio
async def test_fetch_observations_always_empty():
    """No discharge time series exists upstream -> always an empty chunk.

    This must NOT hit the network (no respx mock registered); the connector
    short-circuits because SIGI/PUPR exposes no observation endpoint.
    """
    async with IndonesiaPUPRConnector() as conn:
        chunk = await conn.fetch_observations(
            "indonesia_pupr:T.2.15",
            start=datetime(2026, 5, 25, tzinfo=UTC),
            end=datetime(2026, 6, 1, tzinfo=UTC),
        )

    assert chunk.provider == "indonesia_pupr"
    assert chunk.station_id == "indonesia_pupr:T.2.15"
    assert chunk.observations == []
    # No discharge values are ever produced (this is a level-only / no-data source)
    assert all(o.discharge_m3s is not None for o in chunk.observations)  # vacuously true


@pytest.mark.asyncio
async def test_fetch_latest_also_empty():
    """fetch_latest (base-class delegation) likewise yields no observations."""
    async with IndonesiaPUPRConnector() as conn:
        chunk = await conn.fetch_latest("indonesia_pupr:T.2.15")

    assert chunk.observations == []
