# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""VMM Waterinfo connector — Belgian Flanders water data via KiWIS."""

from __future__ import annotations

from csfs.connectors._kiwis import KiWISConnector
from csfs.core.models import QualityFlag
from csfs.core.registry import register

# KiWIS quality code ranges (same convention as BOM)
_QUALITY_GOOD_MAX = 9
_QUALITY_FAIR_MAX = 19
_QUALITY_MISSING = 130
_QUALITY_NOT_AVAILABLE = 255


def _map_quality(code: object) -> QualityFlag:
    """Map a KiWIS quality code to a CSFS QualityFlag."""
    if code is None:
        return QualityFlag.MISSING
    try:
        value = int(str(code))
    except (ValueError, TypeError):
        return QualityFlag.RAW

    if value in (_QUALITY_MISSING, _QUALITY_NOT_AVAILABLE):
        return QualityFlag.MISSING
    if value <= _QUALITY_GOOD_MAX:
        return QualityFlag.GOOD
    if value <= _QUALITY_FAIR_MAX:
        return QualityFlag.GOOD
    return QualityFlag.SUSPECT


@register("belgium_vmm")
class BelgiumVmmConnector(KiWISConnector):
    """Connector for the VMM Waterinfo KiWIS service (Belgium/Flanders).

    VMM hosts ~1,872 stations but only ~195 measure discharge. The canonical
    discharge parameter is ``Q``; each station publishes several cadences and
    we prefer validated real-time 15-minute (``P.15``) over daily mean.
    """

    slug = "belgium_vmm"
    display_name = "VMM Waterinfo (Belgium)"
    base_url = "https://download.waterinfo.be"
    country_codes = ["BE"]

    _KIWIS_PATH = "/tsmdownload/KiWIS/KiWIS"
    _DISCHARGE_PARAM = "Q"
    _country = "BE"
    # Do NOT request river_name/parametertype_name here: it explodes the
    # response to one row per (station x parameter) and floods it with
    # non-discharge series (rainfall, conductivity, drought indices).
    _STATION_FIELDS = "station_no,station_name,station_latitude,station_longitude"
    # P.15 = validated real-time 15-min; DagGem = daily mean; the rest are base
    # / raw cadences.
    _TS_PREFERENCE = ("P.15", "DagGem", "Basis.15", "Basis", "O.15")
    # The unfiltered Q timeseries list is large (~9k rows); the coverage field
    # is omitted (it forces an expensive server-side span computation that
    # times out), so allow a long timeout for this one call.
    _LIST_TIMEOUT = 180.0
    # VMM spells the values request with a lowercase "v".
    _VALUES_REQUEST = "getTimeseriesvalues"
    # ~10 discharge stations have empty-string coordinates; keep them at 0,0.
    _DEFAULT_COORD = 0.0

    _map_quality = staticmethod(_map_quality)
