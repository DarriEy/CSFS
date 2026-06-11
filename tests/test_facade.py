"""Tests for the public ``csfs`` facade and the Arrow/pandas store queries."""

from __future__ import annotations

import subprocess
import sys
from datetime import datetime

import httpx
import pyarrow as pa
import pytest
import respx

import csfs
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.store.duckdb_store import DuckDBStore

# ---------------------------------------------------------------------------
# Import surface
# ---------------------------------------------------------------------------


def test_all_names_resolve():
    for name in csfs.__all__:
        assert getattr(csfs, name) is not None, name


def test_version():
    assert csfs.__version__ == "0.2.0"
    assert "__version__" in csfs.__all__


def test_facade_reexports_canonical_objects():
    assert csfs.DuckDBStore is DuckDBStore
    assert csfs.Station is Station
    assert csfs.Observation is Observation
    assert csfs.TimeSeriesChunk is TimeSeriesChunk


def test_facade_does_not_eagerly_import_connectors():
    # Connectors stay lazy behind discover(); a fresh interpreter importing
    # csfs must not pull in any csfs.connectors submodule.
    code = (
        "import csfs, sys; "
        "loaded = [m for m in sys.modules if m.startswith('csfs.connectors')]; "
        "assert not loaded, loaded"
    )
    subprocess.run([sys.executable, "-c", code], check=True)


# ---------------------------------------------------------------------------
# Store fixtures (mirror tests/test_store.py seeding)
# ---------------------------------------------------------------------------


@pytest.fixture
def second_station() -> Station:
    return Station(
        id="usgs:01638500",
        provider="usgs",
        native_id="01638500",
        name="Potomac River at Point of Rocks",
        latitude=39.2736,
        longitude=-77.5425,
        country_code="US",
        river="Potomac",
    )


@pytest.fixture
def second_chunk() -> TimeSeriesChunk:
    return TimeSeriesChunk(
        station_id="usgs:01638500",
        provider="usgs",
        observations=[
            Observation(
                station_id="usgs:01638500",
                timestamp=datetime(2024, 6, 1, 12, 0),
                discharge_m3s=99.0,
                quality=QualityFlag.RAW,
            ),
        ],
        fetched_at=datetime(2024, 6, 2, 12, 0),
    )


@pytest.fixture
async def seeded_store(tmp_path, sample_station, sample_chunk, second_station, second_chunk):
    async with DuckDBStore(tmp_path / "facade.duckdb") as store:
        await store.upsert_stations([sample_station, second_station])
        await store.append_observations(sample_chunk)
        await store.append_observations(second_chunk)
        yield store


# ---------------------------------------------------------------------------
# open_store
# ---------------------------------------------------------------------------


async def test_open_store_round_trip(tmp_path, sample_station, sample_chunk):
    db = tmp_path / "open.duckdb"
    async with csfs.open_store(db, read_only=False) as store:
        await store.upsert_stations([sample_station])
        await store.append_observations(sample_chunk)

    # Defaults to read-only — the safe mode for analysis.
    store2 = csfs.open_store(db)
    assert isinstance(store2, DuckDBStore)
    assert store2._read_only is True
    async with store2 as store:
        obs = await store.get_observations(sample_station.id)
        assert len(obs) == 2


# ---------------------------------------------------------------------------
# Arrow queries
# ---------------------------------------------------------------------------


async def test_get_observations_arrow(seeded_store, sample_station):
    table = await seeded_store.get_observations_arrow(sample_station.id)
    assert isinstance(table, pa.Table)
    assert table.num_rows == 2
    assert set(table.column_names) == {"station_id", "timestamp", "discharge_m3s", "quality"}
    assert table.column("discharge_m3s").to_pylist() == [150.5, 145.2]


async def test_get_observations_arrow_time_filters(seeded_store, sample_station):
    table = await seeded_store.get_observations_arrow(
        sample_station.id,
        start=datetime(2024, 6, 1, 12, 0),
        end=datetime(2024, 6, 3),
    )
    assert table.num_rows == 1
    assert table.column("discharge_m3s").to_pylist() == [145.2]


async def test_get_observations_arrow_station_list(seeded_store, sample_station, second_station):
    table = await seeded_store.get_observations_arrow([sample_station.id, second_station.id])
    assert table.num_rows == 3
    assert set(table.column("station_id").to_pylist()) == {sample_station.id, second_station.id}
    # Ordered by timestamp ascending.
    timestamps = table.column("timestamp").to_pylist()
    assert timestamps == sorted(timestamps)


async def test_get_observations_arrow_rejects_empty_list(seeded_store):
    with pytest.raises(ValueError, match="non-empty"):
        await seeded_store.get_observations_arrow([])


async def test_get_stations_arrow(seeded_store):
    table = await seeded_store.get_stations_arrow(provider="usgs")
    assert isinstance(table, pa.Table)
    assert table.num_rows == 2
    assert "latitude" in table.column_names

    boxed = await seeded_store.get_stations_arrow(bbox=(-77.3, 38.0, -77.0, 39.0))
    assert boxed.num_rows == 1
    assert boxed.column("id").to_pylist() == ["usgs:01646500"]


# ---------------------------------------------------------------------------
# pandas queries (optional extra)
# ---------------------------------------------------------------------------


async def test_get_observations_df_single_station(seeded_store, sample_station):
    pd = pytest.importorskip("pandas")
    df = await seeded_store.get_observations_df(sample_station.id)
    assert isinstance(df, pd.DataFrame)
    assert df.index.name == "timestamp"
    assert df.index.is_monotonic_increasing
    assert list(df.columns) == ["discharge_m3s", "quality"]  # station_id dropped
    assert df["discharge_m3s"].tolist() == [150.5, 145.2]


async def test_get_observations_df_multi_station(seeded_store, sample_station, second_station):
    pytest.importorskip("pandas")
    df = await seeded_store.get_observations_df([sample_station.id, second_station.id])
    assert "station_id" in df.columns  # kept for multi-station queries
    assert len(df) == 3
    assert df.index.is_monotonic_increasing


async def test_get_stations_df(seeded_store):
    pytest.importorskip("pandas")
    df = await seeded_store.get_stations_df(country_code="US")
    assert len(df) == 2
    assert {"id", "provider", "latitude", "longitude"} <= set(df.columns)


async def test_df_methods_raise_clear_error_without_pandas(seeded_store, sample_station, monkeypatch):
    # None in sys.modules makes `import pandas` raise ImportError even when
    # pandas is installed, exercising the missing-extra error path.
    monkeypatch.setitem(sys.modules, "pandas", None)
    with pytest.raises(ImportError, match=r"community-streamflow-service\[pandas\]"):
        await seeded_store.get_observations_df(sample_station.id)
    with pytest.raises(ImportError, match=r"community-streamflow-service\[pandas\]"):
        await seeded_store.get_stations_df()


# ---------------------------------------------------------------------------
# Direct-fetch helpers (mocked HTTP, hermetic)
# ---------------------------------------------------------------------------

MOCK_DV_RESPONSE = {
    "value": {
        "timeSeries": [{
            "values": [{
                "value": [
                    {
                        "value": "5000",
                        "dateTime": "2024-06-01T00:00:00.000",
                        "qualifiers": ["A"],
                    },
                ]
            }]
        }]
    }
}


@respx.mock
async def test_fetch_observations():
    respx.get("https://waterservices.usgs.gov/nwis/iv/").mock(
        return_value=httpx.Response(200, json=MOCK_DV_RESPONSE)
    )
    chunk = await csfs.fetch_observations(
        "usgs",
        "usgs:01646500",
        start=datetime(2024, 6, 1),
        end=datetime(2024, 6, 2),
    )
    assert isinstance(chunk, TimeSeriesChunk)
    assert chunk.station_id == "usgs:01646500"
    assert len(chunk.observations) == 1
    assert chunk.observations[0].quality is QualityFlag.GOOD


@respx.mock
def test_fetch_observations_sync():
    respx.get("https://waterservices.usgs.gov/nwis/iv/").mock(
        return_value=httpx.Response(200, json=MOCK_DV_RESPONSE)
    )
    chunk = csfs.fetch_observations_sync(
        "usgs",
        "usgs:01646500",
        start=datetime(2024, 6, 1),
        end=datetime(2024, 6, 2),
    )
    assert len(chunk.observations) == 1
    assert chunk.observations[0].discharge_m3s == pytest.approx(5000 * 0.0283168, rel=1e-3)


async def test_fetch_observations_sync_rejects_running_loop():
    with pytest.raises(RuntimeError, match="event loop"):
        csfs.fetch_observations_sync(
            "usgs",
            "usgs:01646500",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )


async def test_fetch_observations_unknown_provider():
    with pytest.raises(KeyError, match="nope"):
        await csfs.fetch_observations(
            "nope",
            "nope:1",
            start=datetime(2024, 6, 1),
            end=datetime(2024, 6, 2),
        )
