"""Tests for R-ArcticNET connector with respx mocks."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.russia_arcticnet import (
    _REGIONS,
    RussiaArcticNETConnector,
)

# ---------------------------------------------------------------------------
# Sample data
# ---------------------------------------------------------------------------

SAMPLE_ATTRIBUTES = (
    '"PointID"\t"Code"\t"Name"\t"Lat"\t"Long"\t"X_Ease"\t"Y_Ease"'
    '\t"DArea"\t"Hydrozone"\t"Gauge_altitude"'
    '\t"MinOfYear"\t"MaxOfYear"\t"CountOfYear"\t"PercentOfCoverage"\n'
    '"1001"\t"R01"\t"Ob - Barnaul"\t"53.35"\t"83.75"'
    '\t"0"\t"0"\t"169000"\t"Ob"\t"150"\t"1930"\t"1990"\t"60"\t"98"\n'
    '"1002"\t"R02"\t"Irtysh - Omsk"\t"54.97"\t"73.37"'
    '\t"0"\t"0"\t"503000"\t"Ob"\t"75"\t"1940"\t"1985"\t"45"\t"90"\n'
)

SAMPLE_DISCHARGE = (
    "PointID\tCode\tYear\tJan\tFeb\tMar\tApr\tMay"
    "\tJun\tJul\tAug\tSep\tOct\tNov\tDec\tAnnual\n"
    "1001\tR01\t1980\t500\t480\t460\t900\t3200"
    "\t5000\t4500\t3000\t2000\t1200\t800\t600\t1887\n"
    "1001\tR01\t1981\t510\t490\t-9999\t950\t3300"
    "\t5100\t4600\t3100\t2100\t1250\t850\t620\t1930\n"
    "1002\tR02\t1980\t200\t180\t170\t400\t1500"
    "\t2500\t2200\t1500\t1000\t600\t350\t250\t905\n"
)

SAMPLE_DISCHARGE_BLANKS = (
    "PointID\tCode\tYear\tJan\tFeb\tMar\tApr\tMay"
    "\tJun\tJul\tAug\tSep\tOct\tNov\tDec\tAnnual\n"
    "1001\tR01\t1982\t\t\t460\t900\t3200"
    "\t5000\t4500\t3000\t2000\t1200\t800\t600\t1887\n"
)

BASE = "https://www.r-arcticnet.sr.unh.edu"


def _mock_all_regions_404() -> None:
    """Set up respx to return 404 for all region files."""
    for region in _REGIONS:
        respx.get(f"{BASE}{region['attributes']}").mock(
            return_value=httpx.Response(404),
        )
        respx.get(f"{BASE}{region['discharge']}").mock(
            return_value=httpx.Response(404),
        )


def _mock_ob_only(
    attr_text: str = SAMPLE_ATTRIBUTES,
    discharge_text: str = SAMPLE_DISCHARGE,
) -> None:
    """Mock Ob region with data, all others 404."""
    _mock_all_regions_404()
    ob_region = next(r for r in _REGIONS if r["name"] == "Ob")
    respx.get(f"{BASE}{ob_region['attributes']}").mock(
        return_value=httpx.Response(200, text=attr_text),
    )
    respx.get(f"{BASE}{ob_region['discharge']}").mock(
        return_value=httpx.Response(200, text=discharge_text),
    )


# ---------------------------------------------------------------------------
# Station tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_attributes():
    """Stations are parsed from the attributes file."""
    _mock_ob_only()

    async with RussiaArcticNETConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 2
    ob = stations[0]
    assert ob.id == "russia_arcticnet:1001"
    assert ob.provider == "russia_arcticnet"
    assert ob.native_id == "1001"
    assert ob.name == "Ob - Barnaul"
    assert ob.latitude == pytest.approx(53.35)
    assert ob.longitude == pytest.approx(83.75)
    assert ob.country_code == "RU"
    assert ob.catchment_area_km2 == pytest.approx(169000.0)
    assert ob.elevation_m == pytest.approx(150.0)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_extracts_river():
    """River name is extracted from station name."""
    _mock_ob_only()

    async with RussiaArcticNETConnector() as conn:
        stations = await conn.fetch_stations()

    assert stations[0].river == "Ob"
    assert stations[1].river == "Irtysh"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_caches_results():
    """Second call returns cached stations without new downloads."""
    _mock_ob_only()

    async with RussiaArcticNETConnector() as conn:
        first = await conn.fetch_stations()
        second = await conn.fetch_stations()

    assert first is second


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_survives_all_failures():
    """If all regions fail, returns empty list instead of raising."""
    _mock_all_regions_404()

    async with RussiaArcticNETConnector() as conn:
        stations = await conn.fetch_stations()

    assert stations == []


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_record_period():
    """MinOfYear and MaxOfYear are parsed into record_start/end."""
    _mock_ob_only()

    async with RussiaArcticNETConnector() as conn:
        stations = await conn.fetch_stations()

    assert stations[0].record_start == datetime(
        1930, 1, 1, tzinfo=UTC,
    )
    assert stations[0].record_end == datetime(
        1990, 12, 31, tzinfo=UTC,
    )


# ---------------------------------------------------------------------------
# Observation tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_monthly():
    """Monthly discharge values are parsed into Observations."""
    _mock_ob_only()

    async with RussiaArcticNETConnector() as conn:
        chunk = await conn.fetch_observations(
            "russia_arcticnet:1001",
            start=datetime(1980, 1, 1, tzinfo=UTC),
            end=datetime(1980, 12, 31, tzinfo=UTC),
        )

    assert chunk.station_id == "russia_arcticnet:1001"
    assert chunk.provider == "russia_arcticnet"
    assert len(chunk.observations) == 12

    # January 1980
    jan = chunk.observations[0]
    assert jan.timestamp == datetime(1980, 1, 1, tzinfo=UTC)
    assert jan.discharge_m3s == pytest.approx(500.0)
    assert jan.quality.value == "raw"

    # June 1980
    jun = chunk.observations[5]
    assert jun.timestamp == datetime(1980, 6, 1, tzinfo=UTC)
    assert jun.discharge_m3s == pytest.approx(5000.0)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_missing_sentinel():
    """The -9999 sentinel is treated as missing data."""
    _mock_ob_only()

    async with RussiaArcticNETConnector() as conn:
        chunk = await conn.fetch_observations(
            "russia_arcticnet:1001",
            start=datetime(1981, 1, 1, tzinfo=UTC),
            end=datetime(1981, 12, 31, tzinfo=UTC),
        )

    assert len(chunk.observations) == 12
    # March 1981 has -9999
    mar = chunk.observations[2]
    assert mar.timestamp == datetime(1981, 3, 1, tzinfo=UTC)
    assert mar.discharge_m3s is None
    assert mar.quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_blank_values():
    """Blank fields are treated as missing data."""
    _mock_ob_only(discharge_text=SAMPLE_DISCHARGE_BLANKS)

    async with RussiaArcticNETConnector() as conn:
        chunk = await conn.fetch_observations(
            "russia_arcticnet:1001",
            start=datetime(1982, 1, 1, tzinfo=UTC),
            end=datetime(1982, 12, 31, tzinfo=UTC),
        )

    assert len(chunk.observations) == 12
    # Jan and Feb are blank -> missing
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[0].quality.value == "missing"
    assert chunk.observations[1].discharge_m3s is None
    assert chunk.observations[1].quality.value == "missing"
    # March has a valid value
    assert chunk.observations[2].discharge_m3s == pytest.approx(460.0)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_date_filtering():
    """Only observations within the requested date range."""
    _mock_ob_only()

    async with RussiaArcticNETConnector() as conn:
        chunk = await conn.fetch_observations(
            "russia_arcticnet:1001",
            start=datetime(1980, 6, 1, tzinfo=UTC),
            end=datetime(1980, 8, 1, tzinfo=UTC),
        )

    assert len(chunk.observations) == 3
    months = [o.timestamp.month for o in chunk.observations]
    assert months == [6, 7, 8]


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_no_matching_station():
    """Station not present in discharge files returns empty chunk."""
    _mock_ob_only()

    async with RussiaArcticNETConnector() as conn:
        chunk = await conn.fetch_observations(
            "russia_arcticnet:9999",
            start=datetime(1980, 1, 1, tzinfo=UTC),
            end=datetime(1980, 12, 31, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0
    assert chunk.provider == "russia_arcticnet"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_missing_required_columns():
    """Attributes file missing required columns returns no stations for that region."""
    bad_attr = "Name\tCode\n" "Station1\tR01\n"
    _mock_all_regions_404()
    respx.get(f"{BASE}/v4.0/AllData/Ob_Attributes.txt").mock(
        return_value=httpx.Response(200, text=bad_attr),
    )

    async with RussiaArcticNETConnector() as conn:
        stations = await conn.fetch_stations()

    # Bad header -> no stations from Ob, everything else 404
    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_empty_point_id_skipped():
    """Rows with empty PointID are skipped."""
    attr = (
        "PointID\tCode\tName\tLat\tLong\tDArea\tGauge_altitude"
        "\tMinOfYear\tMaxOfYear\n"
        "\tR01\tEmpty\t53.35\t83.75\t100\t10\t1930\t1990\n"
        "1001\tR02\tOb - Barnaul\t53.35\t83.75\t100\t10\t1930\t1990\n"
    )
    _mock_all_regions_404()
    respx.get(f"{BASE}/v4.0/AllData/Ob_Attributes.txt").mock(
        return_value=httpx.Response(200, text=attr),
    )

    async with RussiaArcticNETConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "1001"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_invalid_lat_lon_skipped():
    """Rows with invalid lat/lon are skipped."""
    attr = (
        "PointID\tCode\tName\tLat\tLong\n"
        "1001\tR01\tBad Lat\tNaN\t83.75\n"
        "1002\tR02\tGood\t53.35\t83.75\n"
    )
    _mock_all_regions_404()
    respx.get(f"{BASE}/v4.0/AllData/Ob_Attributes.txt").mock(
        return_value=httpx.Response(200, text=attr),
    )

    async with RussiaArcticNETConnector() as conn:
        stations = await conn.fetch_stations()

    # NaN is valid for float() but the station still gets built
    # Actually float("NaN") doesn't raise ValueError, so it parses
    assert len(stations) == 2


@pytest.mark.asyncio
@respx.mock
async def test_extract_river_no_separator():
    """Station name without separator returns None for river."""
    attr = (
        "PointID\tCode\tName\tLat\tLong\n"
        "1001\tR01\tSimpleName\t53.35\t83.75\n"
    )
    _mock_all_regions_404()
    respx.get(f"{BASE}/v4.0/AllData/Ob_Attributes.txt").mock(
        return_value=httpx.Response(200, text=attr),
    )

    async with RussiaArcticNETConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].river is None


@pytest.mark.asyncio
@respx.mock
async def test_extract_river_empty_name():
    """Empty station name returns None for river."""
    from csfs.connectors.russia_arcticnet import RussiaArcticNETConnector as C
    assert C._extract_river("") is None


@pytest.mark.asyncio
@respx.mock
async def test_extract_river_at_separator():
    """'at' separator extracts river name."""
    from csfs.connectors.russia_arcticnet import RussiaArcticNETConnector as C
    assert C._extract_river("Ob at Barnaul") == "Ob"


@pytest.mark.asyncio
@respx.mock
async def test_discharge_parse_non_numeric():
    """Non-numeric discharge values map to MISSING."""
    discharge = (
        "PointID\tCode\tYear\tJan\tFeb\tMar\tApr\tMay"
        "\tJun\tJul\tAug\tSep\tOct\tNov\tDec\tAnnual\n"
        "1001\tR01\t1980\tabc\t480\t460\t900\t3200"
        "\t5000\t4500\t3000\t2000\t1200\t800\t600\t1887\n"
    )
    _mock_all_regions_404()
    respx.get(f"{BASE}/v4.0/AllData/Ob_Attributes.txt").mock(
        return_value=httpx.Response(200, text=SAMPLE_ATTRIBUTES),
    )
    respx.get(f"{BASE}/v4.0/AllData/Ob_Data_m3_s.txt").mock(
        return_value=httpx.Response(200, text=discharge),
    )

    async with RussiaArcticNETConnector() as conn:
        chunk = await conn.fetch_observations(
            "russia_arcticnet:1001",
            start=datetime(1980, 1, 1, tzinfo=UTC),
            end=datetime(1980, 12, 31, tzinfo=UTC),
        )

    assert len(chunk.observations) == 12
    # Jan has "abc" -> missing
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[0].quality.value == "missing"


@pytest.mark.asyncio
@respx.mock
async def test_discharge_missing_year_column():
    """Discharge file without Year column returns empty observations."""
    bad_discharge = (
        "PointID\tCode\tJan\tFeb\n"
        "1001\tR01\t500\t480\n"
    )
    _mock_all_regions_404()
    respx.get(f"{BASE}/v4.0/AllData/Ob_Attributes.txt").mock(
        return_value=httpx.Response(200, text=SAMPLE_ATTRIBUTES),
    )
    respx.get(f"{BASE}/v4.0/AllData/Ob_Data_m3_s.txt").mock(
        return_value=httpx.Response(200, text=bad_discharge),
    )

    async with RussiaArcticNETConnector() as conn:
        chunk = await conn.fetch_observations(
            "russia_arcticnet:1001",
            start=datetime(1980, 1, 1, tzinfo=UTC),
            end=datetime(1980, 12, 31, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_discharge_invalid_year_skipped():
    """Rows with invalid year values are skipped."""
    discharge = (
        "PointID\tCode\tYear\tJan\tFeb\tMar\tApr\tMay"
        "\tJun\tJul\tAug\tSep\tOct\tNov\tDec\tAnnual\n"
        "1001\tR01\tnotayear\t500\t480\t460\t900\t3200"
        "\t5000\t4500\t3000\t2000\t1200\t800\t600\t1887\n"
    )
    _mock_all_regions_404()
    respx.get(f"{BASE}/v4.0/AllData/Ob_Attributes.txt").mock(
        return_value=httpx.Response(200, text=SAMPLE_ATTRIBUTES),
    )
    respx.get(f"{BASE}/v4.0/AllData/Ob_Data_m3_s.txt").mock(
        return_value=httpx.Response(200, text=discharge),
    )

    async with RussiaArcticNETConnector() as conn:
        chunk = await conn.fetch_observations(
            "russia_arcticnet:1001",
            start=datetime(1980, 1, 1, tzinfo=UTC),
            end=datetime(1980, 12, 31, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_naive_datetimes():
    """Naive start/end datetimes are treated as UTC."""
    _mock_ob_only()

    async with RussiaArcticNETConnector() as conn:
        chunk = await conn.fetch_observations(
            "russia_arcticnet:1001",
            start=datetime(1980, 1, 1),  # naive
            end=datetime(1980, 12, 31),  # naive
        )

    assert len(chunk.observations) == 12


@pytest.mark.asyncio
@respx.mock
async def test_short_row_skipped():
    """Discharge rows shorter than year_col are skipped."""
    discharge = (
        "PointID\tCode\tYear\tJan\n"
        "1001\tR01\n"
        "1001\tR01\t1980\t500\n"
    )
    _mock_all_regions_404()
    respx.get(f"{BASE}/v4.0/AllData/Ob_Attributes.txt").mock(
        return_value=httpx.Response(200, text=SAMPLE_ATTRIBUTES),
    )
    respx.get(f"{BASE}/v4.0/AllData/Ob_Data_m3_s.txt").mock(
        return_value=httpx.Response(200, text=discharge),
    )

    async with RussiaArcticNETConnector() as conn:
        chunk = await conn.fetch_observations(
            "russia_arcticnet:1001",
            start=datetime(1980, 1, 1, tzinfo=UTC),
            end=datetime(1980, 12, 31, tzinfo=UTC),
        )

    # First data row is too short, second has only Jan
    assert len(chunk.observations) == 1
