"""Tests for core data models."""

from datetime import datetime

import pytest

from csfs.core.models import (
    VARIABLE_UNITS,
    Observation,
    QualityFlag,
    Resolution,
    Station,
    TimeSeriesChunk,
    Variable,
)


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


def test_observation_defaults_to_discharge_unknown_resolution():
    obs = Observation(station_id="test:001", timestamp=datetime(2024, 1, 1), value=1.5)
    assert obs.variable is Variable.DISCHARGE
    assert obs.resolution is Resolution.UNKNOWN
    assert obs.value == 1.5


def test_observation_discharge_m3s_alias_sets_value_and_variable():
    obs = Observation(
        station_id="test:001", timestamp=datetime(2024, 1, 1), discharge_m3s=3.2
    )
    assert obs.value == 3.2
    assert obs.variable is Variable.DISCHARGE
    assert obs.discharge_m3s == 3.2


def test_observation_discharge_m3s_property_is_none_for_other_variables():
    obs = Observation(
        station_id="test:001",
        timestamp=datetime(2024, 1, 1),
        variable=Variable.STAGE,
        resolution=Resolution.INSTANTANEOUS,
        value=2.1,
    )
    assert obs.discharge_m3s is None
    assert obs.value == 2.1


def test_observation_alias_conflicts_raise():
    with pytest.raises(ValueError, match="not both"):
        Observation(
            station_id="test:001",
            timestamp=datetime(2024, 1, 1),
            value=1.0,
            discharge_m3s=2.0,
        )
    with pytest.raises(ValueError, match="implies variable"):
        Observation(
            station_id="test:001",
            timestamp=datetime(2024, 1, 1),
            variable=Variable.STAGE,
            discharge_m3s=2.0,
        )


def test_observation_dump_uses_canonical_keys():
    obs = Observation(
        station_id="test:001", timestamp=datetime(2024, 1, 1), discharge_m3s=3.2
    )
    dumped = obs.model_dump()
    assert {"variable", "resolution", "value"} <= dumped.keys()
    assert "discharge_m3s" not in dumped


def test_every_variable_has_a_canonical_unit():
    assert set(VARIABLE_UNITS) == set(Variable)


def test_timeseries_chunk_observation_count(sample_chunk: TimeSeriesChunk):
    assert len(sample_chunk.observations) == 2
    assert sample_chunk.provider == "usgs"


def test_registry_get_unknown_raises():
    import pytest

    from csfs.core.registry import get_connector
    with pytest.raises(KeyError, match="No connector registered"):
        get_connector("nonexistent_provider_xyz")


def test_registry_list_providers_returns_sorted():
    from csfs.core.registry import discover, list_providers

    discover()
    providers = list_providers()
    assert providers == sorted(providers)
    assert len(providers) > 0
