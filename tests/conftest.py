"""Shared test fixtures."""

from __future__ import annotations

from datetime import datetime

import pytest

from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk


@pytest.fixture
def sample_station() -> Station:
    return Station(
        id="usgs:01646500",
        provider="usgs",
        native_id="01646500",
        name="Potomac River near Washington DC",
        latitude=38.9497,
        longitude=-77.1278,
        country_code="US",
        river="Potomac",
        catchment_area_km2=29940.0,
    )


@pytest.fixture
def sample_chunk() -> TimeSeriesChunk:
    return TimeSeriesChunk(
        station_id="usgs:01646500",
        provider="usgs",
        observations=[
            Observation(
                station_id="usgs:01646500",
                timestamp=datetime(2024, 6, 1, 0, 0),
                discharge_m3s=150.5,
                quality=QualityFlag.GOOD,
            ),
            Observation(
                station_id="usgs:01646500",
                timestamp=datetime(2024, 6, 2, 0, 0),
                discharge_m3s=145.2,
                quality=QualityFlag.GOOD,
            ),
        ],
        fetched_at=datetime(2024, 6, 2, 12, 0),
    )
