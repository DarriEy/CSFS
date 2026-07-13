# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Tests for the Chile CR2 explorador connector with mocked responses."""

from datetime import UTC, datetime

import httpx
import pytest
import respx

from csfs.connectors.chile_cr2 import ChileCr2Connector
from csfs.core.models import QualityFlag, Resolution, Variable

BASE = "https://explorador.cr2.cl"

# Shape observed live 2026-07: action=["map"] returns map.points with
# per-station metadata fields as single-element string lists.
MOCK_MAP_JSON = {
    "errors": [],
    "map": {
        "meta": {"legend": "<table></table>"},
        "points": [
            {
                "col": "#00f",
                "name": "Rio Caracarani En Humapal... [DGA:01201005]",
                "val": "0.4 m3/s",
                "fields": {
                    "latitud": ["-17.8428"],
                    "longitud": ["-69.6994"],
                    "codigo_estacion": ["01201005"],
                    "nombre": ["Rio Caracarani En Humapalca"],
                    "nombre_cuenca": ["Rio Lluta"],
                    "inicio_observaciones": ["1973-08-15"],
                    "fin_observaciones": ["2020-06-06"],
                    "institucion": ["DGA"],
                    "fuente": ["dga_web"],
                },
                "lat": -17.8428,
                "lon": -69.6994,
                "alt": 3908.0,
                "id": "01201005",
                "valn": 0.4,
            },
            {
                "col": "#0ff",
                "name": "Rio Maule En Forel [DGA:07383001]",
                "val": "533.1 m3/s",
                "fields": {
                    "codigo_estacion": ["07383001"],
                    "nombre": ["Rio Maule En Forel"],
                    "nombre_cuenca": ["Rio Maule"],
                    "inicio_observaciones": ["1985-01-02"],
                    "fin_observaciones": ["2020-06-06"],
                },
                "lat": -35.4069,
                "lon": -72.2083,
                "alt": None,
                "id": "07383001",
                "valn": 533.1,
            },
        ],
    },
}

# Shape observed live 2026-07: the advertised host is a broken
# http://localhost:8080 — the connector must keep only the /tmp/... path
# and download it from the public host.
MOCK_EXPORT_JSON = (
    '{"errors": [], "export": {"series": '
    '{"url": "http://localhost:8080/tmp/map_abc123/EC_series.csv"}}}'
)

# Legacy shape: HTML page embedding an absolute link on the public host.
MOCK_EXPORT_HTML = (
    "<html><body><a href="
    '"https://www.explorador.cr2.cl/tmp/map_leg42/EC_series.csv">CSV</a>'
    "</body></html>"
)

MOCK_CSV = """agno, mes, dia, valor
2009,12,31,8.80000
2010,01,01,1.10000
2010,01,02,-9999.00000
2010,01,03,2.30000
2010,01,05,4.00000
2010,02,01,9.90000
"""

WINDOW_START = datetime(2010, 1, 1, tzinfo=UTC)
WINDOW_END = datetime(2010, 1, 31, tzinfo=UTC)


@pytest.mark.asyncio
@respx.mock
async def test_fetch_stations():
    route = respx.get(f"{BASE}/request.php").mock(
        return_value=httpx.Response(200, json=MOCK_MAP_JSON)
    )

    async with ChileCr2Connector() as conn:
        stations = await conn.fetch_stations()

    assert route.called
    # The station-listing request must ask for the map action on qflxDaily.
    options = route.calls.last.request.url.params["options"]
    assert '"action":["map"]' in options
    assert '"id":"qflxDaily"' in options

    assert len(stations) == 2
    s = stations[0]
    assert s.id == "chile_cr2:01201005"
    assert s.provider == "chile_cr2"
    assert s.native_id == "01201005"
    assert s.name == "Rio Caracarani En Humapalca"
    assert s.latitude == -17.8428
    assert s.longitude == -69.6994
    assert s.country_code == "CL"
    assert s.river == "Rio Lluta"
    assert s.elevation_m == 3908.0
    assert s.record_start == datetime(1973, 8, 15, tzinfo=UTC)
    assert s.record_end == datetime(2020, 6, 6, tzinfo=UTC)
    assert stations[1].native_id == "07383001"
    assert stations[1].elevation_m is None


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_parses_and_slices():
    export_route = respx.get(f"{BASE}/request.php").mock(
        return_value=httpx.Response(200, text=MOCK_EXPORT_JSON)
    )
    csv_route = respx.get(f"{BASE}/tmp/map_abc123/EC_series.csv").mock(
        return_value=httpx.Response(200, text=MOCK_CSV)
    )

    async with ChileCr2Connector(config={"pacing_s": 0}) as conn:
        chunk = await conn.fetch_observations(
            "chile_cr2:01201005", WINDOW_START, WINDOW_END,
        )

    # The export request targets the right gauge; the CSV is downloaded from
    # the public host despite the advertised localhost URL.
    assert '"sites":["01201005"]' in export_route.calls.last.request.url.params["options"]
    assert csv_route.called

    assert chunk.station_id == "chile_cr2:01201005"
    assert chunk.provider == "chile_cr2"
    # 2009-12-31 and 2010-02-01 sliced out; -9999 sentinel dropped.
    assert [obs.value for obs in chunk.observations] == [1.1, 2.3, 4.0]
    assert [obs.timestamp for obs in chunk.observations] == [
        datetime(2010, 1, 1, tzinfo=UTC),
        datetime(2010, 1, 3, tzinfo=UTC),
        datetime(2010, 1, 5, tzinfo=UTC),
    ]
    for obs in chunk.observations:
        assert obs.variable == Variable.DISCHARGE
        assert obs.resolution == Resolution.DAILY_MEAN
        assert obs.quality == QualityFlag.RAW
        assert obs.timestamp.tzinfo == UTC


@pytest.mark.asyncio
@respx.mock
async def test_fetch_observations_legacy_html_response():
    respx.get(f"{BASE}/request.php").mock(
        return_value=httpx.Response(200, text=MOCK_EXPORT_HTML)
    )
    csv_route = respx.get(f"{BASE}/tmp/map_leg42/EC_series.csv").mock(
        return_value=httpx.Response(200, text=MOCK_CSV)
    )

    async with ChileCr2Connector(config={"pacing_s": 0}) as conn:
        chunk = await conn.fetch_observations(
            "chile_cr2:07383001", WINDOW_START, WINDOW_END,
        )

    assert csv_route.called
    assert len(chunk.observations) == 3
    assert chunk.observations[0].discharge_m3s == 1.1
