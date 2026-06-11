"""Tests for the SMHI (Sweden) hydrology connector with mocked HTTP responses."""

from datetime import UTC, datetime, timedelta

import httpx
import pytest
import respx

from csfs.connectors.sweden_smhi import SwedenSMHIConnector, _quality_from_smhi
from csfs.core.models import QualityFlag

SMHI_BASE = "https://opendata-download-hydroobs.smhi.se/api"

MOCK_STATIONS_RESPONSE = {
    "station": [
        {
            "key": "1",
            "name": "Abisko",
            "latitude": 68.35,
            "longitude": 18.82,
            "active": True,
        },
        {
            "key": "2",
            "name": "Karesuando",
            "latitude": 68.44,
            "longitude": 22.49,
            "active": False,
        },
        {
            "key": "3",
            "name": "Kiruna",
            "latitude": 67.86,
            "longitude": 20.22,
            "active": True,
        },
    ],
}

# Observations spanning 2024-06-01 00:00 to 2024-06-01 02:00 UTC
# Epoch ms for 2024-06-01T00:00:00Z = 1717200000000
# Epoch ms for 2024-06-01T01:00:00Z = 1717203600000
# Epoch ms for 2024-06-01T02:00:00Z = 1717207200000
# Epoch ms for 2024-06-15T00:00:00Z = 1718409600000  (outside typical test range)
MOCK_OBSERVATIONS_RESPONSE = {
    "value": [
        {
            "date": 1717200000000,
            "value": 42.5,
            "quality": "G",
        },
        {
            "date": 1717203600000,
            "value": 43.1,
            "quality": "Controlled",
        },
        {
            "date": 1717207200000,
            "value": 41.0,
            "quality": "Y",
        },
        {
            "date": 1718409600000,
            "value": 50.0,
            "quality": "G",
        },
    ],
}


# ------------------------------------------------------------------
# Station tests
# ------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_parses_all():
    """All stations in the response are returned."""
    respx.get(f"{SMHI_BASE}/version/latest/parameter/1.json").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with SwedenSMHIConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 3
    native_ids = {s.native_id for s in stations}
    assert native_ids == {"1", "2", "3"}


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_fields():
    """Station fields are correctly mapped."""
    respx.get(f"{SMHI_BASE}/version/latest/parameter/1.json").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with SwedenSMHIConnector() as conn:
        stations = await conn.fetch_stations()

    abisko = next(s for s in stations if s.native_id == "1")
    assert abisko.id == "sweden_smhi:1"
    assert abisko.provider == "sweden_smhi"
    assert abisko.name == "Abisko"
    assert abisko.latitude == 68.35
    assert abisko.longitude == 18.82
    assert abisko.country_code == "SE"
    assert abisko.is_active is True


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_inactive_flag():
    """Inactive stations are parsed with is_active=False."""
    respx.get(f"{SMHI_BASE}/version/latest/parameter/1.json").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with SwedenSMHIConnector() as conn:
        stations = await conn.fetch_stations()

    karesuando = next(s for s in stations if s.native_id == "2")
    assert karesuando.is_active is False


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_handles_empty():
    """An empty station array returns no stations."""
    respx.get(f"{SMHI_BASE}/version/latest/parameter/1.json").mock(
        return_value=httpx.Response(200, json={"station": []})
    )

    async with SwedenSMHIConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_skips_missing_key():
    """Stations without a 'key' field are silently skipped."""
    data = {
        "station": [
            {"name": "No Key", "latitude": 60.0, "longitude": 18.0, "active": True},
            {"key": "99", "name": "Has Key", "latitude": 61.0, "longitude": 19.0, "active": True},
        ]
    }
    respx.get(f"{SMHI_BASE}/version/latest/parameter/1.json").mock(
        return_value=httpx.Response(200, json=data)
    )

    async with SwedenSMHIConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    assert stations[0].native_id == "99"


# ------------------------------------------------------------------
# Observation tests
# ------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_filters_by_date_range():
    """Only observations within [start, end] are returned."""
    respx.get(
        f"{SMHI_BASE}/version/latest/parameter/1/station/1/period/corrected-archive/data.json"
    ).mock(return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE))

    async with SwedenSMHIConnector() as conn:
        chunk = await conn.fetch_observations(
            "sweden_smhi:1",
            start=datetime(2024, 6, 1, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 1, 2, 0, tzinfo=UTC),
        )

    # The fourth observation (June 15) should be filtered out
    assert len(chunk.observations) == 3
    assert chunk.station_id == "sweden_smhi:1"
    assert chunk.provider == "sweden_smhi"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_quality_mapping():
    """SMHI quality codes are correctly mapped to CSFS quality flags."""
    respx.get(
        f"{SMHI_BASE}/version/latest/parameter/1/station/1/period/corrected-archive/data.json"
    ).mock(return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE))

    async with SwedenSMHIConnector() as conn:
        chunk = await conn.fetch_observations(
            "sweden_smhi:1",
            start=datetime(2024, 6, 1, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 1, 2, 0, tzinfo=UTC),
        )

    # "G" -> GOOD
    assert chunk.observations[0].quality == QualityFlag.GOOD
    # "Controlled" -> GOOD
    assert chunk.observations[1].quality == QualityFlag.GOOD
    # "Y" -> SUSPECT
    assert chunk.observations[2].quality == QualityFlag.SUSPECT


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_discharge_values():
    """Discharge values are parsed as floats in m3/s."""
    respx.get(
        f"{SMHI_BASE}/version/latest/parameter/1/station/1/period/corrected-archive/data.json"
    ).mock(return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE))

    async with SwedenSMHIConnector() as conn:
        chunk = await conn.fetch_observations(
            "sweden_smhi:1",
            start=datetime(2024, 6, 1, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 1, 2, 0, tzinfo=UTC),
        )

    assert chunk.observations[0].discharge_m3s == pytest.approx(42.5)
    assert chunk.observations[1].discharge_m3s == pytest.approx(43.1)
    assert chunk.observations[2].discharge_m3s == pytest.approx(41.0)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_null_value():
    """A null discharge value results in MISSING quality."""
    data = {
        "value": [
            {"date": 1717200000000, "value": None, "quality": "G"},
        ]
    }
    respx.get(
        f"{SMHI_BASE}/version/latest/parameter/1/station/1/period/corrected-archive/data.json"
    ).mock(return_value=httpx.Response(200, json=data))

    async with SwedenSMHIConnector() as conn:
        chunk = await conn.fetch_observations(
            "sweden_smhi:1",
            start=datetime(2024, 6, 1, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 2, 0, 0, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s is None
    assert chunk.observations[0].quality == QualityFlag.MISSING


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_handles_empty():
    """An empty value array returns zero observations."""
    respx.get(
        f"{SMHI_BASE}/version/latest/parameter/1/station/1/period/corrected-archive/data.json"
    ).mock(return_value=httpx.Response(200, json={"value": []}))

    async with SwedenSMHIConnector() as conn:
        chunk = await conn.fetch_observations(
            "sweden_smhi:1",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_strips_prefix():
    """The connector correctly strips its slug prefix from the station ID."""
    respx.get(
        f"{SMHI_BASE}/version/latest/parameter/1/station/42/period/corrected-archive/data.json"
    ).mock(return_value=httpx.Response(200, json={"value": []}))

    async with SwedenSMHIConnector() as conn:
        chunk = await conn.fetch_observations(
            "sweden_smhi:42",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert chunk.station_id == "sweden_smhi:42"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_naive_datetimes():
    """Naive start/end datetimes are treated as UTC for filtering."""
    respx.get(
        f"{SMHI_BASE}/version/latest/parameter/1/station/1/period/corrected-archive/data.json"
    ).mock(return_value=httpx.Response(200, json=MOCK_OBSERVATIONS_RESPONSE))

    async with SwedenSMHIConnector() as conn:
        chunk = await conn.fetch_observations(
            "sweden_smhi:1",
            start=datetime(2024, 6, 1, 0, 0),  # naive
            end=datetime(2024, 6, 1, 1, 30),    # naive
        )

    # Should include 00:00 and 01:00 but not 02:00
    assert len(chunk.observations) == 2


# ------------------------------------------------------------------
# 15-minute product (parameter 2, config={"resolution": "15min"})
# ------------------------------------------------------------------

# Four 15-min steps starting 2024-06-01T00:00:00Z (epoch ms; 900000 ms apart),
# plus one far outside the test window. Recent unchecked SMHI data carries
# quality "O"; values are m³/s as served (no conversion).
MOCK_15MIN_OBSERVATIONS_RESPONSE = {
    "value": [
        {"date": 1717200000000, "value": 186.0, "quality": "O"},
        {"date": 1717200900000, "value": 186.5, "quality": "O"},
        {"date": 1717201800000, "value": 187.0, "quality": "G"},
        {"date": 1717202700000, "value": 187.5, "quality": "Y"},
        {"date": 1718409600000, "value": 50.0, "quality": "G"},
    ],
}


def test_resolution_default_is_daily():
    conn = SwedenSMHIConnector()
    assert conn.resolution == "daily"
    assert conn._parameter == 1


def test_resolution_15min_selects_parameter_2():
    conn = SwedenSMHIConnector(config={"resolution": "15min"})
    assert conn.resolution == "15min"
    assert conn._parameter == 2


def test_resolution_unknown_raises():
    from csfs.core.exceptions import ConnectorError

    with pytest.raises(ConnectorError, match="resolution"):
        SwedenSMHIConnector(config={"resolution": "hourly"})


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_15min_uses_parameter_2():
    """With resolution=15min the station roster comes from parameter 2."""
    route = respx.get(f"{SMHI_BASE}/version/latest/parameter/2.json").mock(
        return_value=httpx.Response(200, json=MOCK_STATIONS_RESPONSE)
    )

    async with SwedenSMHIConnector(config={"resolution": "15min"}) as conn:
        stations = await conn.fetch_stations()

    assert route.called
    assert len(stations) == 3


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_15min_historical_uses_corrected_archive():
    """Historical 15-min windows download the parameter-2 corrected archive."""
    route = respx.get(
        f"{SMHI_BASE}/version/latest/parameter/2/station/2357/period/corrected-archive/data.json"
    ).mock(return_value=httpx.Response(200, json=MOCK_15MIN_OBSERVATIONS_RESPONSE))

    async with SwedenSMHIConnector(config={"resolution": "15min"}) as conn:
        chunk = await conn.fetch_observations(
            "sweden_smhi:2357",
            start=datetime(2024, 6, 1, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 1, 1, 0, tzinfo=UTC),
        )

    assert route.called
    # The June 15 value is filtered out client-side
    assert len(chunk.observations) == 4
    # Epoch-ms -> UTC and m³/s passthrough are identical to the daily path
    assert chunk.observations[0].timestamp == datetime(2024, 6, 1, 0, 0, tzinfo=UTC)
    assert chunk.observations[1].timestamp == datetime(2024, 6, 1, 0, 15, tzinfo=UTC)
    assert chunk.observations[0].discharge_m3s == pytest.approx(186.0)
    assert chunk.observations[3].discharge_m3s == pytest.approx(187.5)
    # Quality codes: O -> RAW, G -> GOOD, Y -> SUSPECT
    assert chunk.observations[0].quality == QualityFlag.RAW
    assert chunk.observations[2].quality == QualityFlag.GOOD
    assert chunk.observations[3].quality == QualityFlag.SUSPECT


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_15min_recent_window_uses_latest_day():
    """Windows starting within the last 24 h fetch the small latest-day file."""
    route = respx.get(
        f"{SMHI_BASE}/version/latest/parameter/2/station/2357/period/latest-day/data.json"
    ).mock(return_value=httpx.Response(200, json={"value": []}))

    now = datetime.now(UTC)
    async with SwedenSMHIConnector(config={"resolution": "15min"}) as conn:
        chunk = await conn.fetch_observations(
            "sweden_smhi:2357",
            start=now - timedelta(hours=6),
            end=now,
        )

    assert route.called
    assert chunk.station_id == "sweden_smhi:2357"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest_15min_uses_latest_day():
    """fetch_latest (last 24 h) takes the cheap latest-day path for 15-min data."""
    route = respx.get(
        f"{SMHI_BASE}/version/latest/parameter/2/station/2357/period/latest-day/data.json"
    ).mock(return_value=httpx.Response(200, json={"value": []}))

    async with SwedenSMHIConnector(config={"resolution": "15min"}) as conn:
        chunk = await conn.fetch_latest("sweden_smhi:2357")

    assert route.called
    assert chunk.provider == "sweden_smhi"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_daily_recent_window_keeps_corrected_archive():
    """The default daily product always uses corrected-archive, even for recent windows."""
    route = respx.get(
        f"{SMHI_BASE}/version/latest/parameter/1/station/1/period/corrected-archive/data.json"
    ).mock(return_value=httpx.Response(200, json={"value": []}))

    now = datetime.now(UTC)
    async with SwedenSMHIConnector() as conn:
        await conn.fetch_observations("sweden_smhi:1", start=now - timedelta(hours=6), end=now)

    assert route.called


def test_select_period_boundaries():
    """Recent 15-min windows pick latest-day; old windows pick corrected-archive."""
    conn = SwedenSMHIConnector(config={"resolution": "15min"})
    assert conn._select_period(datetime.now(UTC) - timedelta(hours=1)) == "latest-day"
    assert conn._select_period(datetime(2020, 1, 1, tzinfo=UTC)) == "corrected-archive"

    daily = SwedenSMHIConnector()
    assert daily._select_period(datetime.now(UTC) - timedelta(hours=1)) == "corrected-archive"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_15min_naive_recent_start_uses_latest_day():
    """Naive (no tzinfo) datetimes are treated as UTC before period selection."""
    route = respx.get(
        f"{SMHI_BASE}/version/latest/parameter/2/station/2357/period/latest-day/data.json"
    ).mock(return_value=httpx.Response(200, json={"value": []}))

    naive_now = datetime.now(UTC).replace(tzinfo=None)
    async with SwedenSMHIConnector(config={"resolution": "15min"}) as conn:
        await conn.fetch_observations(
            "sweden_smhi:2357",
            start=naive_now - timedelta(hours=6),
            end=naive_now,
        )

    assert route.called


# ------------------------------------------------------------------
# Quality mapping unit tests
# ------------------------------------------------------------------


def test_quality_from_smhi_good():
    assert _quality_from_smhi("G") == QualityFlag.GOOD


def test_quality_from_smhi_controlled():
    assert _quality_from_smhi("Controlled") == QualityFlag.GOOD


def test_quality_from_smhi_suspect():
    assert _quality_from_smhi("Y") == QualityFlag.SUSPECT


def test_quality_from_smhi_unchecked_o():
    """SMHI 'O' (orange) = okontrollerade/unchecked values -> RAW."""
    assert _quality_from_smhi("O") == QualityFlag.RAW


def test_quality_from_smhi_unknown_falls_through_to_raw():
    """Unknown/future codes deliberately map to RAW (treated as unchecked)."""
    assert _quality_from_smhi("SomeOtherCode") == QualityFlag.RAW
    assert _quality_from_smhi("R") == QualityFlag.RAW


def test_quality_from_smhi_strips_whitespace():
    assert _quality_from_smhi("  G  ") == QualityFlag.GOOD


# ------------------------------------------------------------------
# Registration test
# ------------------------------------------------------------------


def test_connector_is_registered():
    """The connector registers itself under the expected slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("sweden_smhi")
    assert cls is SwedenSMHIConnector


def test_connector_metadata():
    """Verify class-level attributes."""
    assert SwedenSMHIConnector.slug == "sweden_smhi"
    assert SwedenSMHIConnector.country_codes == ["SE"]
    assert "smhi" in SwedenSMHIConnector.base_url


@pytest.mark.asyncio
@respx.mock
async def test_fetch_latest_delegates():
    """fetch_latest calls fetch_observations for the last 24h."""
    respx.get(
        f"{SMHI_BASE}/version/latest/parameter/1/station/1/period/corrected-archive/data.json"
    ).mock(return_value=httpx.Response(200, json={"value": []}))

    async with SwedenSMHIConnector() as conn:
        chunk = await conn.fetch_latest("sweden_smhi:1")

    assert chunk.provider == "sweden_smhi"
    assert chunk.station_id == "sweden_smhi:1"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_station_parse_error_skips():
    """Entries that raise ValueError/KeyError during Station creation are skipped."""
    data = {
        "station": [
            {
                "key": "bad",
                "name": "Bad Station",
                "latitude": "not-a-number",
                "longitude": 18.0,
                "active": True,
            },
            {
                "key": "99",
                "name": "Good Station",
                "latitude": 61.0,
                "longitude": 19.0,
                "active": True,
            },
        ]
    }
    respx.get(f"{SMHI_BASE}/version/latest/parameter/1.json").mock(
        return_value=httpx.Response(200, json=data)
    )

    async with SwedenSMHIConnector() as conn:
        stations = await conn.fetch_stations()

    # "not-a-number" triggers ValueError in float(), caught by except block
    # Only the good station should be parsed
    assert len(stations) == 1
    assert stations[0].native_id == "99"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_invalid_timestamp_raises():
    """An invalid epoch timestamp raises DataFormatError."""
    from csfs.core.exceptions import DataFormatError

    data = {
        "value": [
            {"date": 99999999999999999, "value": 42.5, "quality": "G"},
        ]
    }
    respx.get(
        f"{SMHI_BASE}/version/latest/parameter/1/station/1/period/corrected-archive/data.json"
    ).mock(return_value=httpx.Response(200, json=data))

    async with SwedenSMHIConnector() as conn:
        with pytest.raises(DataFormatError, match="Invalid epoch timestamp"):
            await conn.fetch_observations(
                "sweden_smhi:1",
                start=datetime(2024, 6, 1, 0, 0, tzinfo=UTC),
                end=datetime(2024, 6, 2, 0, 0, tzinfo=UTC),
            )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_skips_null_date():
    """Observations with null date are skipped."""
    data = {
        "value": [
            {"date": None, "value": 42.5, "quality": "G"},
            {"date": 1717200000000, "value": 43.0, "quality": "G"},
        ]
    }
    respx.get(
        f"{SMHI_BASE}/version/latest/parameter/1/station/1/period/corrected-archive/data.json"
    ).mock(return_value=httpx.Response(200, json=data))

    async with SwedenSMHIConnector() as conn:
        chunk = await conn.fetch_observations(
            "sweden_smhi:1",
            start=datetime(2024, 6, 1, 0, 0, tzinfo=UTC),
            end=datetime(2024, 6, 2, 0, 0, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(43.0)
