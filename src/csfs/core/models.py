# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Canonical data models for the CSFS pipeline."""

from __future__ import annotations

from datetime import datetime
from enum import StrEnum
from typing import TYPE_CHECKING, Any

import pyarrow as pa
from pydantic import BaseModel, Field, model_validator


class QualityFlag(StrEnum):
    GOOD = "good"
    SUSPECT = "suspect"
    MISSING = "missing"
    ESTIMATED = "estimated"
    RAW = "raw"


class Variable(StrEnum):
    """Canonical observed variables. Values are stored in the SI unit
    given by :data:`VARIABLE_UNITS`; there is no per-row unit column."""

    DISCHARGE = "discharge"
    STAGE = "stage"
    WATER_TEMPERATURE = "water_temperature"
    PRECIPITATION = "precipitation"


#: Canonical SI unit implied by each variable.
VARIABLE_UNITS: dict[Variable, str] = {
    Variable.DISCHARGE: "m3/s",
    Variable.STAGE: "m",
    Variable.WATER_TEMPERATURE: "degC",
    Variable.PRECIPITATION: "mm",
}


class Resolution(StrEnum):
    """Temporal resolution + aggregation of an observed value.

    ``UNKNOWN`` covers sources that do not declare their aggregation and
    all rows ingested before the multi-variable schema existed.
    """

    INSTANTANEOUS = "instantaneous"
    HOURLY_MEAN = "hourly_mean"
    DAILY_MEAN = "daily_mean"
    DAILY_MAX = "daily_max"
    DAILY_MIN = "daily_min"
    DAILY_SUM = "daily_sum"
    MONTHLY_MEAN = "monthly_mean"
    UNKNOWN = "unknown"


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
    """A single observation of one variable at one station.

    ``value`` is expressed in the SI unit implied by ``variable``
    (see :data:`VARIABLE_UNITS`). The pre-multi-variable constructor
    kwarg ``discharge_m3s=`` is still accepted as an alias for
    ``value=`` with ``variable='discharge'``.
    """

    station_id: str
    timestamp: datetime
    variable: Variable = Variable.DISCHARGE
    resolution: Resolution = Resolution.UNKNOWN
    value: float | None = None
    quality: QualityFlag = QualityFlag.RAW

    if TYPE_CHECKING:
        # Type-checking-only signature so the transitional discharge_m3s=
        # alias (handled by the before-validator) is a known kwarg.
        def __init__(
            self,
            *,
            station_id: str,
            timestamp: datetime,
            variable: Variable | str = ...,
            resolution: Resolution | str = ...,
            value: float | None = None,
            discharge_m3s: float | None = None,
            quality: QualityFlag | str = ...,
        ) -> None: ...

    @model_validator(mode="before")
    @classmethod
    def _accept_discharge_m3s_alias(cls, data: Any) -> Any:
        if isinstance(data, dict) and "discharge_m3s" in data:
            data = dict(data)
            alias = data.pop("discharge_m3s")
            if data.get("value") is not None:
                raise ValueError("pass either value= or discharge_m3s=, not both")
            variable = data.get("variable", Variable.DISCHARGE)
            if variable not in (Variable.DISCHARGE, Variable.DISCHARGE.value):
                raise ValueError("discharge_m3s= implies variable='discharge'")
            data["value"] = alias
            data["variable"] = Variable.DISCHARGE
        return data

    @property
    def discharge_m3s(self) -> float | None:
        """Compat accessor: the value when this is a discharge observation."""
        return self.value if self.variable is Variable.DISCHARGE else None


class TimeSeriesChunk(BaseModel):
    """A batch of observations returned by a connector."""

    station_id: str
    provider: str
    observations: list[Observation]
    fetched_at: datetime


OBSERVATION_SCHEMA = pa.schema([
    pa.field("station_id", pa.string(), nullable=False),
    pa.field("timestamp", pa.timestamp("s", tz="UTC"), nullable=False),
    pa.field("variable", pa.string(), nullable=False),
    pa.field("resolution", pa.string(), nullable=False),
    pa.field("value", pa.float64(), nullable=True),
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
