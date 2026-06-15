"""Tests for the germany_nrw connector (OpenGeodata.NRW discharge archive)."""

import io
import zipfile
from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.germany_nrw import GermanyNRWConnector
from csfs.core.models import QualityFlag
from csfs.core.registry import get_connector

_Q = "https://www.opengeodata.nrw.de/produkte/umwelt_klima/wasser/oberflaechengewaesser/hydro/q"

MOCK_INDEX = {
    "datasets": [
        {
            "name": "Edereinzugsgebiet_Abfluesse_CSV",
            "title": "Edereinzugsgebiet Abflüsse NRW als CSV",
            "files": [
                {"name": "Edereinzugsgebiet-NRW-Q_2010-2019_EPSG25832_CSV.zip"},
                {"name": "Edereinzugsgebiet-NRW-Q_2020-2029_EPSG25832_CSV.zip"},
            ],
        },
    ],
}

_CSV = (
    "station_name;station_no;dateTime;value[m³/s]\n"
    "Beddelhausen;4281591000100;2026-01-03T11:45:00+01:00;2.07\n"  # 10:45 UTC
    "Beddelhausen;4281591000100;2026-01-03T12:00:00+01:00;NA\n"
    "Beddelhausen;4281591000100;2019-01-01T00:00:00+01:00;9.99\n"  # out of window
)


def _zip(member: str, content: str) -> bytes:
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as zf:
        zf.writestr(member, content)
    return buf.getvalue()


def test_connector_is_registered():
    assert get_connector("germany_nrw") is GermanyNRWConnector


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations_enumerates_from_latest_zip():
    respx.get(f"{_Q}/index.json").mock(
        return_value=httpx.Response(200, json=MOCK_INDEX),
    )
    # Only the latest decade (2020-2029) is downloaded for enumeration.
    respx.get(f"{_Q}/Edereinzugsgebiet-NRW-Q_2020-2029_EPSG25832_CSV.zip").mock(
        return_value=httpx.Response(200, content=_zip(
            "4281591000100_Beddelhausen_2020-2029_Abfluss_m3s.csv", _CSV,
        )),
    )

    async with GermanyNRWConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == 1
    s = stations[0]
    assert s.native_id == "4281591000100"
    assert s.name == "Beddelhausen"
    assert s.country_code == "DE"
    assert s.provider == "germany_nrw"


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_and_filters():
    respx.get(f"{_Q}/index.json").mock(
        return_value=httpx.Response(200, json=MOCK_INDEX),
    )
    zip_bytes = _zip("4281591000100_Beddelhausen_2020-2029_Abfluss_m3s.csv", _CSV)
    respx.get(f"{_Q}/Edereinzugsgebiet-NRW-Q_2020-2029_EPSG25832_CSV.zip").mock(
        return_value=httpx.Response(200, content=zip_bytes),
    )

    async with GermanyNRWConnector() as conn:
        chunk = await conn.fetch_observations(
            "germany_nrw:4281591000100",
            start=datetime(2026, 1, 1, tzinfo=UTC),
            end=datetime(2026, 2, 1, tzinfo=UTC),
        )

    # The 2019 row is outside the window; 2 rows remain (one real, one NA).
    assert len(chunk.observations) == 2
    real = chunk.observations[0]
    assert real.discharge_m3s == pytest.approx(2.07)
    assert real.quality == QualityFlag.RAW
    # CET 11:45 -> 10:45 UTC.
    assert real.timestamp == datetime(2026, 1, 3, 10, 45, tzinfo=UTC)
    assert chunk.observations[1].discharge_m3s is None
    assert chunk.observations[1].quality == QualityFlag.MISSING


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_unknown_station_empty():
    respx.get(f"{_Q}/index.json").mock(
        return_value=httpx.Response(200, json=MOCK_INDEX),
    )
    respx.get(f"{_Q}/Edereinzugsgebiet-NRW-Q_2020-2029_EPSG25832_CSV.zip").mock(
        return_value=httpx.Response(200, content=_zip(
            "4281591000100_Beddelhausen_2020-2029_Abfluss_m3s.csv", _CSV,
        )),
    )

    async with GermanyNRWConnector() as conn:
        chunk = await conn.fetch_observations(
            "germany_nrw:9999999999999",
            start=datetime(2026, 1, 1, tzinfo=UTC),
            end=datetime(2026, 2, 1, tzinfo=UTC),
        )
    assert chunk.observations == []


def test_parse_member_name():
    nid, name = GermanyNRWConnector._parse_member_name(
        "4281591000100_Beddelhausen_2020-2029_Abfluss_m3s.csv",
    )
    assert nid == "4281591000100"
    assert name == "Beddelhausen"
