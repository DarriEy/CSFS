# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""SPW Hydrométrie connector — Wallonia (southern Belgium) via KiWIS.

Complements ``belgium_vmm`` (Flanders) with the French-speaking Walloon region
— the Meuse and Sambre basins. Discharge is the ``Débit`` station parameter.

The SPW host is capacity-limited and returns transient ``503`` errors,
especially for large responses, so we (1) filter ``getTimeseriesList`` to a
single cadence (the hourly mean, available at virtually every discharge
station) to keep responses small, and (2) retry transient 5xx locally (via the
KiWIS base). Of ~330 catalogue rows, ~305 are real discharge stations (the rest
are basin-grouping entries with no coordinates).
"""

from __future__ import annotations

from csfs.connectors._kiwis import KiWISConnector
from csfs.core.models import QualityFlag
from csfs.core.registry import register

# KISTERS quality codes: low = validated, 130/255 = missing, otherwise the
# value is provisional/unvalidated real-time data.
_QUALITY_GOOD_MAX = 40
_QUALITY_MISSING = (130, 255)


def _map_quality(code: object) -> QualityFlag:
    if code is None:
        return QualityFlag.MISSING
    try:
        value = int(str(code))
    except (ValueError, TypeError):
        return QualityFlag.RAW
    if value in _QUALITY_MISSING:
        return QualityFlag.MISSING
    if value <= _QUALITY_GOOD_MAX:
        return QualityFlag.GOOD
    return QualityFlag.RAW


@register("belgium_spw")
class BelgiumSpwConnector(KiWISConnector):
    """Connector for SPW Hydrométrie's KiWIS service (Wallonia, Belgium)."""

    slug = "belgium_spw"
    display_name = "SPW Hydrométrie (Belgium)"
    base_url = "https://hydrometrie.wallonie.be"
    country_codes = ["BE"]

    _KIWIS_PATH = "/services/KiWIS/KiWIS"
    _DISCHARGE_PARAM = "Débit"
    _country = "BE"
    _STATION_FIELDS = (
        "station_no,station_name,station_latitude,station_longitude,river_name"
    )
    # Filter to the hourly-mean cadence (present at ~all discharge stations) so
    # the timeseries-list response stays small enough to dodge 503s.
    _CADENCE = "10-Debit.1h.Moyen"
    _TS_NAME_FILTER = _CADENCE
    _TS_PREFERENCE = (_CADENCE,)
    _TRANSIENT_RETRIES = 6

    _map_quality = staticmethod(_map_quality)
