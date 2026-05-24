"""Tests for the Pakistan IRSA/WAPDA connector with mocked HTTP and local CSV."""

from datetime import UTC, datetime
from pathlib import Path

import httpx
import pytest
import respx

from csfs.connectors.pakistan_wapda import (
    _SEED_STATIONS,
    CUSEC_TO_M3S,
    PakistanWAPDAConnector,
)

# ---------------------------------------------------------------------------
# Sample CSV data (Kaggle format, cusecs)
# ---------------------------------------------------------------------------

SAMPLE_KAGGLE_CSV = """\
Date,Tarbela_Inflow,Mangla_Inflow,Marala
2024-06-01,50000,30000,20000
2024-06-02,52000,31000,21000
2024-06-03,48000,29000,19000
"""

SAMPLE_KAGGLE_CSV_MISSING = """\
Date,Tarbela_Inflow,Mangla_Inflow
2024-06-01,50000,30000
2024-06-02,NA,31000
2024-06-03,48000,nan
"""


# ---------------------------------------------------------------------------
# Station listing tests
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_stations_returns_seed_list():
    """Seed list returns all curated stations."""
    async with PakistanWAPDAConnector() as conn:
        stations = await conn.fetch_stations()

    assert len(stations) == len(_SEED_STATIONS)


@pytest.mark.asyncio
async def test_fetch_stations_metadata():
    """Seed stations have correct metadata fields."""
    async with PakistanWAPDAConnector() as conn:
        stations = await conn.fetch_stations()

    tarbela = next(
        s for s in stations if s.native_id == "tarbela"
    )
    assert tarbela.id == "pakistan_wapda:tarbela"
    assert tarbela.provider == "pakistan_wapda"
    assert tarbela.name == "Tarbela Dam"
    assert tarbela.latitude == pytest.approx(34.089)
    assert tarbela.longitude == pytest.approx(72.693)
    assert tarbela.country_code == "PK"
    assert tarbela.river == "Indus"
    assert tarbela.catchment_area_km2 == pytest.approx(168000.0)


# ---------------------------------------------------------------------------
# Observation tests — local CSV
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_fetch_observations_csv_parses_correctly(
    tmp_path: Path,
):
    """Kaggle CSV with cusec values is parsed and converted to m3/s."""
    csv_file = tmp_path / "pakistan_rivers.csv"
    csv_file.write_text(SAMPLE_KAGGLE_CSV, encoding="utf-8")

    async with PakistanWAPDAConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "pakistan_wapda:tarbela",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 3, tzinfo=UTC),
        )

    assert chunk.station_id == "pakistan_wapda:tarbela"
    assert chunk.provider == "pakistan_wapda"
    assert len(chunk.observations) == 3

    expected_m3s = 50000 * CUSEC_TO_M3S
    assert chunk.observations[0].discharge_m3s == pytest.approx(
        expected_m3s,
    )
    assert chunk.observations[0].quality.value == "raw"


@pytest.mark.asyncio
async def test_fetch_observations_csv_handles_missing(
    tmp_path: Path,
):
    """NA and nan values are treated as missing."""
    csv_file = tmp_path / "pakistan_rivers.csv"
    csv_file.write_text(
        SAMPLE_KAGGLE_CSV_MISSING, encoding="utf-8",
    )

    async with PakistanWAPDAConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "pakistan_wapda:tarbela",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 3
    # First row has valid data
    assert chunk.observations[0].discharge_m3s is not None
    # Second row: NA -> missing
    assert chunk.observations[1].discharge_m3s is None
    assert chunk.observations[1].quality.value == "missing"


@pytest.mark.asyncio
async def test_fetch_observations_no_data_dir():
    """Without data_dir and no web, returns empty chunk."""
    async with PakistanWAPDAConnector() as conn:
        chunk = await conn.fetch_observations(
            "pakistan_wapda:tarbela",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 3, tzinfo=UTC),
        )

    assert chunk.station_id == "pakistan_wapda:tarbela"
    assert chunk.provider == "pakistan_wapda"
    assert len(chunk.observations) == 0


@pytest.mark.asyncio
async def test_fetch_observations_csv_date_filtering(
    tmp_path: Path,
):
    """Only observations within [start, end] are returned."""
    csv_file = tmp_path / "pakistan_rivers.csv"
    csv_file.write_text(SAMPLE_KAGGLE_CSV, encoding="utf-8")

    async with PakistanWAPDAConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "pakistan_wapda:tarbela",
            start=datetime(2024, 6, 2, tzinfo=UTC),
            end=datetime(2024, 6, 2, tzinfo=UTC),
        )

    assert len(chunk.observations) == 1
    assert chunk.observations[0].timestamp.day == 2


# ---------------------------------------------------------------------------
# Web endpoint tests (respx mocks)
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_irsa_endpoint():
    """IRSA HTML with table rows is parsed for discharge data."""
    html_body = (
        "<html><body><table>\n"
        "<tr><td>01/06/2024</td><td>50000</td></tr>\n"
        "<tr><td>02/06/2024</td><td>52000</td></tr>\n"
        "</table></body></html>"
    )
    respx.get("http://pakirsa.gov.pk/DailyData.aspx").mock(
        return_value=httpx.Response(200, text=html_body),
    )

    async with PakistanWAPDAConnector() as conn:
        chunk = await conn.fetch_observations(
            "pakistan_wapda:tarbela",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 3, tzinfo=UTC),
        )

    assert chunk.provider == "pakistan_wapda"
    assert len(chunk.observations) == 2
    expected_m3s = 50000 * CUSEC_TO_M3S
    assert chunk.observations[0].discharge_m3s == pytest.approx(
        expected_m3s,
    )


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_web_fallback_to_csv(
    tmp_path: Path,
):
    """Falls back to CSV when both web endpoints fail."""
    respx.get("http://pakirsa.gov.pk/DailyData.aspx").mock(
        return_value=httpx.Response(500),
    )
    respx.get(
        "https://www.wapda.gov.pk/index.php/river-flow-data",
    ).mock(
        return_value=httpx.Response(503),
    )

    csv_file = tmp_path / "data.csv"
    csv_file.write_text(SAMPLE_KAGGLE_CSV, encoding="utf-8")

    async with PakistanWAPDAConnector(
        config={"data_dir": str(tmp_path)},
    ) as conn:
        chunk = await conn.fetch_observations(
            "pakistan_wapda:tarbela",
            start=datetime(2024, 6, 1, tzinfo=UTC),
            end=datetime(2024, 6, 3, tzinfo=UTC),
        )

    assert len(chunk.observations) == 3


@pytest.mark.asyncio
async def test_connector_registration():
    """Connector is registered under the correct slug."""
    from csfs.core.registry import get_connector

    cls = get_connector("pakistan_wapda")
    assert cls is PakistanWAPDAConnector


@pytest.mark.asyncio
async def test_seed_station_ids_are_canonical():
    """Every seed station has a properly formatted CSFS station ID."""
    async with PakistanWAPDAConnector() as conn:
        stations = await conn.fetch_stations()

    for station in stations:
        assert station.id == f"pakistan_wapda:{station.native_id}"
        assert station.provider == "pakistan_wapda"
        assert station.country_code == "PK"
