"""Tests for core data models."""

from datetime import datetime

from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk


def test_station_id_format(sample_station: Station):
    assert sample_station.id == "usgs:01646500"
    assert sample_station.provider == "usgs"
    assert sample_station.native_id == "01646500"


def test_observation_missing_discharge():
    obs = Observation(
        station_id="test:001",
        timestamp=datetime(2024, 1, 1),
        discharge_m3s=None,
        quality=QualityFlag.MISSING,
    )
    assert obs.discharge_m3s is None
    assert obs.quality == QualityFlag.MISSING


def test_timeseries_chunk_observation_count(sample_chunk: TimeSeriesChunk):
    assert len(sample_chunk.observations) == 2
    assert sample_chunk.provider == "usgs"
