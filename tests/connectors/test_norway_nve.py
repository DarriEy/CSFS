"""Tests for the NVE HydAPI (Norway) connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.norway_nve import NorwayNVEConnector

STATIONS_URL = "https://hydapi.nve.no/api/v1/Stations"
OBSERVATIONS_URL = "https://hydapi.nve.no/api/v1/Observations"


@pytest.fixture(autouse=True)
def _isolate_hydapi(tmp_path, monkeypatch):
    """Ensure tests never read the developer's real ~/.hydapi key file."""
    monkeypatch.setattr(
        "csfs.connectors.norway_nve._KEY_FILE", tmp_path / "no-such-hydapi",
    )


def _station(sid, name, lat, lon, river="Glomma", area=354.2, *, discharge=True):
    """Build a station entry; discharge=False omits the 1001 series."""
    series = []
    if discharge:
        series.append({
            "parameter": "1001",
            "parameterName": "Vannføring",
            "resolutionList": [{"resTime": 1440, "method": "Mean"}],
        })
    return {
        "stationId": sid,
        "stationName": name,
        "latitude": lat,
        "longitude": lon,
        "riverName": river,
        "drainageBasinArea": area,
        "masl": 100.0,
        "seriesList": series,
    }


def _stations(*entries):
    return {"data": list(entries)}


MOCK_STATIONS_RESPONSE = _stations(
    _station("2.32.0", "Gryta", 60.12, 11.45, "Glomma", 354.2),
    _station("12.209.0", "Sjodalsvatn", 61.45, 9.32, "Sjoa", 487.0),
)

MOCK_OBSERVATIONS_RESPONSE = {
    "data": [{
        "stationId": "2.32.0",
        "parameter": "1001",
        "observations": [
            {"time": "2024-06-01T00:00:00Z", "value": 12.3, "correction": 1},
            {"time": "2024-06-02T00:00:00Z", "value": 14.7, "correction": 0},
            {"time": "2024-06-03T00:00:00Z", "value": 11.0, "correction": 2},
        ],
    }]
}

MOCK_OBSERVATIONS_WITH_NULL = {
    "data": [{
        "stationId": "2.32.0",
        "parameter": "1001",
        "observations": [
            {"time": "2024-06-01T00:00:00Z", "value": 12.3, "correction": 1},
            {"time": "2024-06-02T00:00:00Z", "value": None, "correction": 0},
        ],
    }]
}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_correctly():
    respx.get(STATIONS_URL).mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE),
    )

    async with NorwayNVEConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    assert {s.native_id for s in stations} == {"2.32.0", "12.209.0"}

    gryta = next(s for s in stations if s.native_id == "2.32.0")
    assert gryta.id == "norway_nve:2.32.0"
    assert gryta.name == "Gryta"
    assert gryta.country_code == "NO"
    assert gryta.river == "Glomma"
    assert gryta.catchment_area_km2 == pytest.approx(354.2)
    assert gryta.elevation_m == pytest.approx(100.0)
    assert gryta.latitude == pytest.approx(60.12)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_filters_non_discharge():
    """Stations whose series list lacks a 1001 (discharge) series are dropped."""
    resp = _stations(
        _station("2.32.0", "Gryta", 60.12, 11.45),
        _station("1.10.0", "Level Only", 59.0, 11.0, discharge=False),
    )
    respx.get(STATIONS_URL).mock(return_value=httpx.Response(200, json=resp))

    async with NorwayNVEConnector() as conn:
        stations = await conn.fetch_stations()

    assert {s.native_id for s in stations} == {"2.32.0"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    respx.get(STATIONS_URL).mock(
        return_value=httpx.Response(200, json={"data": []}),
    )

    async with NorwayNVEConnector() as conn:
        stations = await conn.fetch_stations()

    assert stations == []


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_json():
    respx.get(OBSERVATIONS_URL).mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE),
    )

    async with NorwayNVEConnector() as conn:
        chunk = await conn.fetch_observations(
            "norway_nve:2.32.0",
            start=datetime(2024, 6, 1), end=datetime(2024, 6, 7),
        )

    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == pytest.approx(12.3)
    assert chunk.observations[0].quality.value == "good"        # correction 1
    assert chunk.observations[1].quality.value == "raw"         # correction 0
    assert chunk.observations[2].quality.value == "estimated"   # correction 2


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_null_value_is_missing():
    respx.get(OBSERVATIONS_URL).mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_WITH_NULL),
    )

    async with NorwayNVEConnector() as conn:
        chunk = await conn.fetch_observations(
            "norway_nve:2.32.0",
            start=datetime(2024, 6, 1), end=datetime(2024, 6, 3),
        )

    assert len(chunk.observations) == 2
    assert chunk.observations[1].discharge_m3s is None
    assert chunk.observations[1].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty_data():
    respx.get(OBSERVATIONS_URL).mock(
        return_value=httpx.Response(200, json={"data": []}),
    )

    async with NorwayNVEConnector() as conn:
        chunk = await conn.fetch_observations(
            "norway_nve:2.32.0",
            start=datetime(2024, 6, 1), end=datetime(2024, 6, 2),
        )

    assert chunk.observations == []


@pytest.mark.asyncio
@respx.mock
async def test_api_key_from_config_takes_precedence():
    respx.get(STATIONS_URL).mock(return_value=httpx.Response(200, json={"data": []}))

    async with NorwayNVEConnector(config={"api_key": "test-key-123"}) as conn:
        assert conn.client.headers["X-API-Key"] == "test-key-123"
        await conn.fetch_stations()


@pytest.mark.asyncio
@respx.mock
async def test_api_key_read_from_hydapi_file(tmp_path, monkeypatch):
    """The key is read verbatim from ~/.hydapi (handles the trailing '==')."""
    keyfile = tmp_path / ".hydapi"
    keyfile.write_text("# NVE key\nKEY-FROM-FILE/abc==\n")
    monkeypatch.setattr("csfs.connectors.norway_nve._KEY_FILE", keyfile)
    respx.get(STATIONS_URL).mock(return_value=httpx.Response(200, json={"data": []}))

    async with NorwayNVEConnector() as conn:
        assert conn.client.headers["X-API-Key"] == "KEY-FROM-FILE/abc=="
        await conn.fetch_stations()


@pytest.mark.asyncio
@respx.mock
async def test_no_api_key_header_when_no_key_available():
    respx.get(STATIONS_URL).mock(return_value=httpx.Response(200, json={"data": []}))

    async with NorwayNVEConnector() as conn:
        assert "X-API-Key" not in conn.client.headers
        await conn.fetch_stations()


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_reference_time_format():
    route = respx.get(OBSERVATIONS_URL).mock(
        return_value=httpx.Response(200, json={"data": []}),
    )

    async with NorwayNVEConnector() as conn:
        await conn.fetch_observations(
            "norway_nve:2.32.0",
            start=datetime(2024, 6, 1), end=datetime(2024, 6, 7),
        )

    assert "ReferenceTime=2024-06-01%2F2024-06-07" in str(route.calls[0].request.url)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_entries_without_station_id():
    resp = _stations(
        {"stationName": "Ghost", "latitude": 60.0, "longitude": 10.0, "seriesList": []},
        _station("5.1.0", "Real Station", 61.0, 11.0, "Namsen"),
    )
    respx.get(STATIONS_URL).mock(return_value=httpx.Response(200, json=resp))

    async with NorwayNVEConnector() as conn:
        stations = await conn.fetch_stations()

    assert [s.native_id for s in stations] == ["5.1.0"]


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parse_error_skips():
    bad = _station("bad.station", "Bad", "not-a-number", 10.0)
    good = _station("5.1.0", "Good", 61.0, 11.0)
    respx.get(STATIONS_URL).mock(
        return_value=httpx.Response(200, json=_stations(bad, good)),
    )

    async with NorwayNVEConnector() as conn:
        stations = await conn.fetch_stations()

    assert [s.native_id for s in stations] == ["5.1.0"]


def test_correction_to_quality_none():
    from csfs.connectors.norway_nve import _correction_to_quality
    from csfs.core.models import QualityFlag

    assert _correction_to_quality(None) == QualityFlag.RAW


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest_delegates():
    respx.get(OBSERVATIONS_URL).mock(
        return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE),
    )

    async with NorwayNVEConnector() as conn:
        chunk = await conn.fetch_latest("norway_nve:2.32.0")

    assert chunk.station_id == "norway_nve:2.32.0"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_invalid_timestamp_raises():
    from csfs.core.exceptions import DataFormatError

    bad_data = {"data": [{
        "stationId": "2.32.0", "parameter": "1001",
        "observations": [{"time": "not-a-date", "value": 12.3, "correction": 1}],
    }]}
    respx.get(OBSERVATIONS_URL).mock(return_value=httpx.Response(200, json=bad_data))

    async with NorwayNVEConnector() as conn:
        with pytest.raises(DataFormatError, match="Invalid timestamp"):
            await conn.fetch_observations(
                "norway_nve:2.32.0",
                start=datetime(2024, 6, 1), end=datetime(2024, 6, 7),
            )
