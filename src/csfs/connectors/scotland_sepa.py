# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""SEPA connector — Scottish Environment Protection Agency via KiWIS.

SEPA covers Scotland (the UK EA connector covers England & Wales only). Its
public KiWIS time-series service needs no authentication. Discharge is the
``Flow`` station parameter; of ~900 stations, ~320 measure flow.
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


@register("scotland_sepa")
class ScotlandSepaConnector(KiWISConnector):
    """Connector for SEPA's KiWIS time-series service (Scotland)."""

    slug = "scotland_sepa"
    display_name = "SEPA (Scotland)"
    base_url = "https://timeseries.sepa.org.uk"
    country_codes = ["GB"]
    # SEPA's KiWIS rate-limits (HTTP 429) under concurrent load; keep a small
    # number of requests in flight so acquisition doesn't get throttled out.
    max_concurrent_requests = 2

    _KIWIS_PATH = "/KiWIS/KiWIS"
    _DISCHARGE_PARAM = "Flow"
    _country = "GB"
    _STATION_FIELDS = (
        "station_no,station_name,station_latitude,station_longitude,river_name"
    )
    # Prefer real-time 15-minute flow, then hourly/daily means.
    _TS_PREFERENCE = ("15minute", "Hour.Mean", "Day.Mean", "Day.Mean.Natural")
    _LIST_TIMEOUT = 120.0

    _map_quality = staticmethod(_map_quality)
