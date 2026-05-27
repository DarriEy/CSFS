"""Tests for Afghanistan USGS connector with mocked HTTP responses."""

from datetime import datetime

import httpx
import pytest
import respx

from csfs.connectors.afghanistan_usgs import AfghanistanUSGSConnector

# ---------------------------------------------------------------------------
# Mock data
# ---------------------------------------------------------------------------

MOCK_RDB_RESPONSE = (
    "# comment line\n"
    "# another comment\n"
    "agency_cd\tsite_no\tstation_nm\tdec_lat_va\tdec_long_va\tdrain_area_va\n"
    "5s\t15s\t50s\t16s\t16s\t16s\n"
    "USGS\t390831069120000\tKUNDUZ RIVER AT CHAR DARA\t36.81\t68.80\t9342\n"
    "USGS\t343000068000000\tHELMAND RIVER AT DEHRAWUD\t32.94\t66.07\t11081\n"
)

MOCK_DV_RESPONSE = {
    "value": {
        "timeSeries": [{
            "values": [{
                "value": [
                    {
                        "value": "3500",
                        "dateTime": "2024-06-01T00:00:00.000",
                        "qualifiers": ["A"],
                    },
                    {
                        "value": "3200",
                        "dateTime": "2024-06-02T00:00:00.000",
                        "qualifiers": ["P"],
                    },
                    {
                        "value": "-999999",
                        "dateTime": "2024-06-03T00:00:00.000",
                        "qualifiers": ["P"],
                    },
                ]
            }]
        }]
    }
}

MOCK_DV_EMPTY = {"value": {"timeSeries": []}}


# ---------------------------------------------------------------------------
# Station listing tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_bbox():
    """Stations are fetched via bounding-box query."""
    respx.get("https://waterservices.usgs.gov/nwis/site/").mock(
        return_value=httpx.Response(200, text=MOCK_RDB_RESPONSE),
    )

    async with AfghanistanUSGSConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    first = stations[0]
    assert first.provider == "afghanistan_usgs"
    assert first.id == "afghanistan_usgs:390831069120000"
    assert first.native_id == "390831069120000"
    assert first.name == "KUNDUZ RIVER AT CHAR DARA"
    assert first.latitude == pytest.approx(36.81)
    assert first.longitude == pytest.approx(68.80)
    assert first.country_code == "AF"
    # Drainage area converted from sq mi to km2
    assert first.catchment_area_km2 == pytest.approx(9342 * 2.58999, rel=1e-3)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fallback_to_seed():
    """If the bBox query fails, connector falls back to seed list."""
    respx.get("https://waterservices.usgs.gov/nwis/site/").mock(
        return_value=httpx.Response(500),
    )

    async with AfghanistanUSGSConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(AfghanistanUSGSConnector._SEED_STATIONS)
    assert all(s.country_code == "AF" for s in stations)
    assert all(s.provider == "afghanistan_usgs" for s in stations)


@pytest.mark.asyncio
async def test_seed_station_ids_are_canonical():
    """Every seed station has a properly formatted CSFS station ID."""
    async with AfghanistanUSGSConnector() as conn:
        # Force seed path by not mocking any HTTP
        stations = conn._build_seed_stations()

    for station in stations:
        assert station.id == f"afghanistan_usgs:{station.native_id}"
        assert station.provider == "afghanistan_usgs"
        assert station.country_code == "AF"
        assert station.latitude != 0.0 or station.longitude != 0.0


# ---------------------------------------------------------------------------
# Observation tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_json():
    """Daily-value JSON is parsed with CFS -> m3/s conversion."""
    respx.get("https://waterservices.usgs.gov/nwis/dv/").mock(
        return_value=httpx.Response(200, json=MOCK_DV_RESPONSE),
    )

    async with AfghanistanUSGSConnector() as conn:
        chunk = await conn.fetch_observations(
            "afghanistan_usgs:390831069120000",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 3),
        )

    assert chunk.station_id == "afghanistan_usgs:390831069120000"
    assert chunk.provider == "afghanistan_usgs"
    assert len(chunk.observations) == 3

    # First observation: approved quality, converted from CFS
    obs0 = chunk.observations[0]
    assert obs0.discharge_m3s == pytest.approx(3500 * 0.0283168, rel=1e-3)
    assert obs0.quality.value == "good"

    # Second observation: provisional -> raw
    obs1 = chunk.observations[1]
    assert obs1.discharge_m3s == pytest.approx(3200 * 0.0283168, rel=1e-3)
    assert obs1.quality.value == "raw"

    # Third observation: sentinel -999999 -> missing
    obs2 = chunk.observations[2]
    assert obs2.discharge_m3s is None
    assert obs2.quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_empty_timeseries():
    """Empty timeSeries array returns an empty chunk."""
    respx.get("https://waterservices.usgs.gov/nwis/dv/").mock(
        return_value=httpx.Response(200, json=MOCK_DV_EMPTY),
    )

    async with AfghanistanUSGSConnector() as conn:
        chunk = await conn.fetch_observations(
            "afghanistan_usgs:390831069120000",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 3),
        )

    assert len(chunk.observations) == 0
    assert chunk.provider == "afghanistan_usgs"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_strips_prefix():
    """Station ID prefix is stripped when querying USGS API."""
    route = respx.get("https://waterservices.usgs.gov/nwis/dv/").mock(
        return_value=httpx.Response(200, json=MOCK_DV_EMPTY),
    )

    async with AfghanistanUSGSConnector() as conn:
        await conn.fetch_observations(
            "afghanistan_usgs:390831069120000",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 3),
        )

    # Verify the native ID was sent to the API (no prefix)
    request = route.calls[0].request
    assert "390831069120000" in str(request.url)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_bad_json_raises():
    """Unexpected JSON structure raises DataFormatError."""
    respx.get("https://waterservices.usgs.gov/nwis/dv/").mock(
        return_value=httpx.Response(200, json={"unexpected": "data"}),
    )

    from csfs.core.exceptions import DataFormatError

    async with AfghanistanUSGSConnector() as conn:
        with pytest.raises(DataFormatError):
            await conn.fetch_observations(
                "afghanistan_usgs:390831069120000",
                start=datetime(2024, 6, 1),
                end=datetime(2024, 6, 3),
            )


# ---------------------------------------------------------------------------
# Coverage gap tests — no RDB header found
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_no_rdb_header_falls_back():
    """RDB response without header falls back to seed list."""

    respx.get("https://waterservices.usgs.gov/nwis/site/").mock(
        return_value=httpx.Response(200, text="# No data here\n"),
    )

    async with AfghanistanUSGSConnector() as conn:
        stations = await conn.fetch_stations()

    # Falls back to seed stations
    assert len(stations) == len(AfghanistanUSGSConnector._SEED_STATIONS)


# ---------------------------------------------------------------------------
# Coverage gap tests — short RDB row
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_short_row_skipped():
    """RDB rows with fewer fields than the header are skipped."""
    rdb_text = (
        "# comment\n"
        "agency_cd\tsite_no\tstation_nm\tdec_lat_va\tdec_long_va\tdrain_area_va\n"
        "5s\t15s\t50s\t16s\t16s\t16s\n"
        "USGS\t390831069120000\tKUNDUZ RIVER AT CHAR DARA\t36.81\t68.80\t9342\n"
        "USGS\tSHORT\n"  # too short
    )
    respx.get("https://waterservices.usgs.gov/nwis/site/").mock(
        return_value=httpx.Response(200, text=rdb_text),
    )

    async with AfghanistanUSGSConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "390831069120000"


# ---------------------------------------------------------------------------
# Coverage gap tests — station parse ValueError/KeyError
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_invalid_lat_lon_skipped():
    """Rows with non-numeric lat/lon are skipped."""
    rdb_text = (
        "# comment\n"
        "agency_cd\tsite_no\tstation_nm\tdec_lat_va\tdec_long_va\tdrain_area_va\n"
        "5s\t15s\t50s\t16s\t16s\t16s\n"
        "USGS\t390831069120000\tKUNDUZ\t36.81\t68.80\t9342\n"
        "USGS\t999999999999999\tBAD STATION\tnot_a_number\tnot_a_number\t1000\n"
    )
    respx.get("https://waterservices.usgs.gov/nwis/site/").mock(
        return_value=httpx.Response(200, text=rdb_text),
    )

    async with AfghanistanUSGSConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "390831069120000"


# ---------------------------------------------------------------------------
# Coverage gap tests — drainage area parse failure
# ---------------------------------------------------------------------------


def test_parse_drainage_area_invalid():
    """Non-numeric drainage area returns None."""
    assert AfghanistanUSGSConnector._parse_drainage_area("not_a_number") is None
    assert AfghanistanUSGSConnector._parse_drainage_area("") is None
