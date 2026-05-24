# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Canonical data models for the CSFS pipeline."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum

import pyarrow as pa
from pydantic import BaseModel, Field


class QualityFlag(StrEnum):
    GOOD = "good"
    SUSPECT = "suspect"
    MISSING = "missing"
    ESTIMATED = "estimated"
    RAW = "raw"


class Station(BaseModel):
    """A gauging station with provider-agnostic metadata."""

    id: str = Field(description="CSFS-internal unique ID: {provider}:{native_id}")
    provider: str = Field(description="Provider slug, e.g. 'usgs', 'uk_ea'")
    native_id: str = Field(description="Station ID as used by the source agency")
    name: str
    latitude: float
    longitude: float
    country_code: str = Field(description="ISO 3166-1 alpha-2")
    river: str | None = None
    catchment_area_km2: float | None = None
    elevation_m: float | None = None
    record_start: datetime | None = None
    record_end: datetime | None = None
    is_active: bool = True


class Observation(BaseModel):
    """A single streamflow observation."""

    station_id: str
    timestamp: datetime
    discharge_m3s: float | None = None
    quality: QualityFlag = QualityFlag.RAW


class TimeSeriesChunk(BaseModel):
    """A batch of observations returned by a connector."""

    station_id: str
    provider: str
    observations: list[Observation]
    fetched_at: datetime


OBSERVATION_SCHEMA = pa.schema([
    pa.field("station_id", pa.string(), nullable=False),
    pa.field("timestamp", pa.timestamp("s", tz="UTC"), nullable=False),
    pa.field("discharge_m3s", pa.float64(), nullable=True),
    pa.field("quality", pa.string(), nullable=False),
])

STATION_SCHEMA = pa.schema([
    pa.field("id", pa.string(), nullable=False),
    pa.field("provider", pa.string(), nullable=False),
    pa.field("native_id", pa.string(), nullable=False),
    pa.field("name", pa.string(), nullable=False),
    pa.field("latitude", pa.float64(), nullable=False),
    pa.field("longitude", pa.float64(), nullable=False),
    pa.field("country_code", pa.string(), nullable=False),
    pa.field("river", pa.string(), nullable=True),
    pa.field("catchment_area_km2", pa.float64(), nullable=True),
    pa.field("elevation_m", pa.float64(), nullable=True),
    pa.field("is_active", pa.bool_(), nullable=False),
])
