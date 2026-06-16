# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""Tests for the SYMFLUENCE integration plugin (csfs.integrations.symfluence).

Three layers:

1. Import-safety: the module (and ``import csfs``) must work without
   symfluence installed — verified by hiding symfluence from ``sys.modules``.
2. Pure helpers: station-id parsing, UTC coercion, and the raw/processed CSV
   shaping are framework-free and tested standalone (these keep the module's
   covered-line ratio healthy in the symfluence-less CI).
3. Integration (``pytest.importorskip("symfluence")``): entry-point
   auto-registration, the acquire()/process() happy path with the CSFS fetch
   monkeypatched, store-mode reads, and helpful config errors. These skip in
   CI and run in an environment with symfluence installed.
"""

from __future__ import annotations

import importlib
import logging
import sys
from datetime import UTC, datetime, timedelta
from pathlib import Path

import pytest

import csfs.integrations.symfluence as integration
from csfs.core.models import Observation, QualityFlag, TimeSeriesChunk

logger = logging.getLogger("test_symfluence_integration")

STATION = "usgs:01646500"


def _make_chunk(n: int = 10, start: datetime | None = None, freq_hours: int = 24) -> TimeSeriesChunk:
    start = start or datetime(2020, 1, 1, tzinfo=UTC)
    observations = [
        Observation(
            station_id=STATION,
            timestamp=start + timedelta(hours=freq_hours * i),
            discharge_m3s=10.0 + i,
            quality=QualityFlag.GOOD,
        )
        for i in range(n)
    ]
    return TimeSeriesChunk(
        station_id=STATION,
        provider="usgs",
        observations=observations,
        fetched_at=datetime.now(UTC),
    )


# ---------------------------------------------------------------------------
# 1. Import safety without symfluence
# ---------------------------------------------------------------------------


def test_module_imports_without_symfluence():
    """Hiding symfluence must leave the module importable (base -> object)."""
    hidden = {
        name: sys.modules.pop(name)
        for name in list(sys.modules)
        if name == "symfluence" or name.startswith("symfluence.")
    }
    sys.modules["symfluence"] = None  # any 'import symfluence...' now fails
    try:
        mod = importlib.reload(integration)
        assert mod.HAVE_SYMFLUENCE is False
        assert mod.CSFSStreamflowHandler is not None
        # The degraded class has no framework base.
        assert mod.CSFSStreamflowHandler.__mro__[1] is object
        # register() refuses clearly rather than failing cryptically.
        with pytest.raises(ImportError, match="symfluence is not importable"):
            mod.register()
        # The degraded handler guards its framework-dependent methods.
        handler = mod.CSFSStreamflowHandler()
        with pytest.raises(RuntimeError, match="requires SYMFLUENCE"):
            handler.acquire()
    finally:
        sys.modules.pop("symfluence", None)
        sys.modules.update(hidden)
        importlib.reload(integration)


def test_csfs_package_has_no_symfluence_dependency():
    """import csfs alone must not pull symfluence in."""
    import csfs  # noqa: F401

    # Importing the integration module is also side-effect free in this regard:
    # it only *tries* symfluence; it never requires it.
    assert isinstance(integration.HAVE_SYMFLUENCE, bool)


# ---------------------------------------------------------------------------
# 2. Pure helpers (standalone, no symfluence required)
# ---------------------------------------------------------------------------


def test_parse_station_ids_single():
    assert integration.parse_station_ids("usgs:01646500") == [("usgs", "usgs:01646500")]


def test_parse_station_ids_comma_list_and_whitespace():
    parsed = integration.parse_station_ids(" usgs:01646500 , uk_ea:3400TH ")
    assert parsed == [("usgs", "usgs:01646500"), ("uk_ea", "uk_ea:3400TH")]


def test_parse_station_ids_yaml_list():
    parsed = integration.parse_station_ids(["usgs:01646500", "wsc_rt:05BB001"])
    assert parsed == [("usgs", "usgs:01646500"), ("wsc_rt", "wsc_rt:05BB001")]


def test_parse_station_ids_provider_slug_lowercased():
    assert integration.parse_station_ids("USGS:01646500") == [("usgs", "USGS:01646500")]


@pytest.mark.parametrize("bad", ["01646500", ":01646500", "usgs:", "usgs"])
def test_parse_station_ids_unnamespaced_is_a_helpful_error(bad):
    with pytest.raises(ValueError, match="<provider>:<native_id>"):
        integration.parse_station_ids(bad)


@pytest.mark.parametrize("missing", [None, "", "   ", []])
def test_parse_station_ids_missing_value(missing):
    with pytest.raises(ValueError, match="CSFS_STATION_ID"):
        integration.parse_station_ids(missing)


def test_parse_station_ids_non_string_scalar():
    with pytest.raises(ValueError, match="<provider>:<native_id>"):
        integration.parse_station_ids(1646500)


def test_ensure_utc_naive_is_interpreted_as_utc():
    out = integration.ensure_utc(datetime(2020, 1, 1, 12, 0))
    assert out == datetime(2020, 1, 1, 12, 0, tzinfo=UTC)


def test_ensure_utc_converts_aware_values():
    from datetime import timezone

    plus2 = timezone(timedelta(hours=2))
    out = integration.ensure_utc(datetime(2020, 1, 1, 12, 0, tzinfo=plus2))
    assert out == datetime(2020, 1, 1, 10, 0, tzinfo=UTC)


def test_ensure_utc_accepts_pandas_timestamps():
    pd = pytest.importorskip("pandas")
    out = integration.ensure_utc(pd.Timestamp("2020-01-01 06:00"))
    assert out == datetime(2020, 1, 1, 6, 0, tzinfo=UTC)


def test_ensure_utc_accepts_iso_strings():
    out = integration.ensure_utc("2020-01-01 06:00")
    assert out == datetime(2020, 1, 1, 6, 0, tzinfo=UTC)


def test_ensure_utc_none_raises():
    with pytest.raises(ValueError, match="EXPERIMENT_TIME_START"):
        integration.ensure_utc(None)


def test_observations_to_raw_frame_from_models():
    pytest.importorskip("pandas")
    chunk = _make_chunk(3)
    frame = integration.observations_to_raw_frame(chunk.observations)
    assert list(frame.columns) == integration.RAW_COLUMNS
    assert len(frame) == 3
    assert frame["discharge_m3s"].tolist() == [10.0, 11.0, 12.0]
    assert (frame["quality"] == "good").all()


def test_observations_to_raw_frame_from_store_rows():
    pytest.importorskip("pandas")
    rows = [
        {"station_id": STATION, "timestamp": datetime(2020, 1, 1, tzinfo=UTC), "discharge_m3s": 5.5, "quality": "good"},
        {"station_id": STATION, "timestamp": datetime(2020, 1, 2, tzinfo=UTC), "discharge_m3s": None, "quality": None},
    ]
    frame = integration.observations_to_raw_frame(rows)
    assert list(frame.columns) == integration.RAW_COLUMNS
    assert frame["quality"].tolist() == ["good", ""]
    assert frame["discharge_m3s"].isna().tolist() == [False, True]


def test_observations_to_raw_frame_empty():
    pytest.importorskip("pandas")
    frame = integration.observations_to_raw_frame([])
    assert list(frame.columns) == integration.RAW_COLUMNS
    assert frame.empty


def test_standardize_frame_contract():
    """Raw CSFS frame -> tz-naive UTC 'datetime' index + 'discharge_cms' column."""
    pd = pytest.importorskip("pandas")
    raw = pd.DataFrame(
        {
            "timestamp": [
                "2020-01-02T00:00:00+00:00",
                "2020-01-01T00:00:00+00:00",
                "2020-01-03T00:00:00+00:00",
            ],
            "discharge_m3s": [2.0, 1.0, None],
            "quality": ["good", "good", "missing"],
        }
    )
    df = integration.standardize_frame(raw)
    assert list(df.columns) == ["discharge_cms"]
    assert df.index.name == "datetime"
    assert df.index.tz is None  # tz-naive UTC, matching the USGS/WSC handlers
    assert df.index.is_monotonic_increasing
    assert len(df) == 2  # NaN discharge dropped
    assert df["discharge_cms"].tolist() == [1.0, 2.0]


def test_standardize_frame_converts_to_utc_before_dropping_tz():
    pd = pytest.importorskip("pandas")
    raw = pd.DataFrame({"timestamp": ["2020-01-01T02:00:00+02:00"], "discharge_m3s": [1.0]})
    df = integration.standardize_frame(raw)
    assert df.index[0] == pd.Timestamp("2020-01-01 00:00:00")


def test_standardize_frame_missing_columns():
    pd = pytest.importorskip("pandas")
    with pytest.raises(ValueError, match="discharge_m3s"):
        integration.standardize_frame(pd.DataFrame({"timestamp": []}))


def test_combine_station_frames_single_passthrough():
    pytest.importorskip("pandas")
    chunk = _make_chunk(4)
    df = integration.standardize_frame(integration.observations_to_raw_frame(chunk.observations))
    assert integration.combine_station_frames([df]) is df


def test_combine_station_frames_averages_stations():
    pd = pytest.importorskip("pandas")
    idx = pd.DatetimeIndex(["2020-01-01", "2020-01-02"], name="datetime")
    a = pd.DataFrame({"discharge_cms": [1.0, 3.0]}, index=idx)
    b = pd.DataFrame({"discharge_cms": [3.0, 5.0]}, index=idx)
    combined = integration.combine_station_frames([a, b])
    assert combined["discharge_cms"].tolist() == [2.0, 4.0]
    assert combined.index.name == "datetime"


def test_combine_station_frames_empty_raises():
    pytest.importorskip("pandas")
    with pytest.raises(ValueError, match="combine"):
        integration.combine_station_frames([])


def test_chunk_round_trip_through_csv(tmp_path):
    """TimeSeriesChunk -> raw CSV -> standardized frame keeps values and UTC order."""
    pd = pytest.importorskip("pandas")
    chunk = _make_chunk(5)
    raw_file = tmp_path / "csfs_usgs_01646500_raw.csv"
    integration.observations_to_raw_frame(chunk.observations).to_csv(raw_file, index=False)

    df = integration.standardize_frame(pd.read_csv(raw_file))
    assert df["discharge_cms"].tolist() == [10.0, 11.0, 12.0, 13.0, 14.0]
    assert df.index[0] == pd.Timestamp("2020-01-01 00:00:00")
    assert df.index.tz is None


# ---------------------------------------------------------------------------
# 2b. Drop-in provider backends (pure declarations, no symfluence required)
# ---------------------------------------------------------------------------


def test_provider_backends_cover_the_native_streamflow_providers():
    """Drop-in keys mirror SYMFLUENCE's lowercased STREAMFLOW_DATA_PROVIDER values."""
    assert set(integration.PROVIDER_BACKENDS) == {
        "usgs", "wsc", "smhi", "lamah_ice", "lamah_ce",
        "camels_br", "camels_de", "camels_cl", "camels_ind", "camels_ch",
        "camels_aus", "camels_us", "camels_dk", "camels_gb", "camels_se", "camels_fr",
    }
    slugs = {key: backend.slug for key, backend in integration.PROVIDER_BACKENDS.items()}
    assert slugs == {
        "usgs": "usgs",
        "wsc": "environment_canada",
        "smhi": "sweden_smhi",
        # dataset-artifact providers (not live APIs)
        "lamah_ice": "iceland_lamahice",
        "lamah_ce": "lamah_ce",
        "camels_br": "camels_br",
        "camels_de": "camels_de",
        "camels_cl": "camels_cl",
        "camels_ind": "camels_ind",
        "camels_ch": "camels_ch",
        "camels_aus": "camels_aus",
        "camels_us": "camels_us",
        "camels_dk": "camels_dk",
        "camels_gb": "camels_gb",
        "camels_se": "camels_se",
        "camels_fr": "camels_fr",
    }


def test_smhi_backend_pins_the_15min_product():
    """Native SMHI downloads hydroobs parameter 2 (15-min discharge); parity requires it."""
    assert integration.PROVIDER_BACKENDS["smhi"].connector_defaults == {"resolution": "15min"}
    # USGS/WSC need no connector overrides — the CSFS defaults already match.
    assert integration.PROVIDER_BACKENDS["usgs"].connector_defaults == {}
    assert integration.PROVIDER_BACKENDS["wsc"].connector_defaults == {}


def test_backend_station_key_resolution_order():
    """Key order mirrors the native handlers' own config lookups."""
    assert [k.dict_key for k in integration.PROVIDER_BACKENDS["usgs"].station_keys] == [
        "STATION_ID",
        "USGS_SITE_CODE",
        "STREAMFLOW_STATION_ID",
    ]
    assert [k.dict_key for k in integration.PROVIDER_BACKENDS["wsc"].station_keys] == ["STATION_ID"]
    assert [k.dict_key for k in integration.PROVIDER_BACKENDS["smhi"].station_keys] == ["STATION_ID"]


@pytest.mark.parametrize(
    ("provider", "raw", "expected"),
    [
        ("usgs", "06191500", "usgs:06191500"),
        ("wsc", "05BB001", "environment_canada:05BB001"),
        ("smhi", "2357", "sweden_smhi:2357"),
        ("smhi", 2357, "sweden_smhi:2357"),
    ],
)
def test_resolve_bare_native_station_ids(provider, raw, expected):
    assert integration.resolve_provider_station_id(provider, raw) == expected


@pytest.mark.parametrize("raw", ["6191500", 6191500])
def test_resolve_usgs_zero_pads_short_numeric_ids(raw):
    """YAML int / short codes get the native handler's 8-digit zero-padding."""
    assert integration.resolve_provider_station_id("usgs", raw) == "usgs:06191500"


def test_resolve_usgs_leaves_long_or_alpha_ids_alone():
    assert integration.resolve_provider_station_id("usgs", "06191500") == "usgs:06191500"
    assert integration.resolve_provider_station_id("usgs", "0123456789") == "usgs:0123456789"


@pytest.mark.parametrize(
    ("provider", "raw", "expected"),
    [
        ("usgs", "usgs:06191500", "usgs:06191500"),
        ("wsc", "environment_canada:05BB001", "environment_canada:05BB001"),
        ("wsc", "wsc:05BB001", "environment_canada:05BB001"),
        ("smhi", "sweden_smhi:2357", "sweden_smhi:2357"),
        ("smhi", "SMHI:2357", "sweden_smhi:2357"),
    ],
)
def test_resolve_accepts_namespaced_station_ids(provider, raw, expected):
    """Both the CSFS slug and the SYMFLUENCE provider name work as prefixes."""
    assert integration.resolve_provider_station_id(provider, raw) == expected


@pytest.mark.parametrize(
    ("provider", "raw"),
    [("usgs", "uk_ea:3400TH"), ("wsc", "usgs:06191500"), ("smhi", "wsc:05BB001"), ("usgs", "usgs:")],
)
def test_resolve_rejects_foreign_or_malformed_namespaces(provider, raw):
    with pytest.raises(ValueError, match="ADDITIONAL_OBSERVATIONS: csfs"):
        integration.resolve_provider_station_id(provider, raw)


@pytest.mark.parametrize("raw", [None, "", "   "])
def test_resolve_rejects_empty_station_ids(raw):
    with pytest.raises(ValueError, match="Empty station id"):
        integration.resolve_provider_station_id("usgs", raw)


def test_provider_handler_classes_are_built_for_every_backend():
    assert set(integration.PROVIDER_HANDLERS) == set(integration.PROVIDER_BACKENDS)
    for key, cls in integration.PROVIDER_HANDLERS.items():
        assert issubclass(cls, integration.CSFSStreamflowHandler)
        assert cls.__name__ == f"CSFS{key.upper()}StreamflowHandler"
        assert key == cls.PROVIDER_KEY
        assert cls.BACKEND is integration.PROVIDER_BACKENDS[key]
        assert cls.obs_type == "streamflow"


# ---------------------------------------------------------------------------
# 3. Integration tests (skip when symfluence is not installed)
# ---------------------------------------------------------------------------


def _symfluence_config(tmp_path, **overrides):
    """Minimal flat config accepted by SYMFLUENCE observation handlers."""
    cfg = {
        "SYMFLUENCE_DATA_DIR": str(tmp_path),
        "SYMFLUENCE_CODE_DIR": str(tmp_path / "code"),
        "DOMAIN_NAME": "TestDomain",
        "EXPERIMENT_ID": "exp1",
        "EXPERIMENT_TIME_START": "2020-01-01 00:00",
        "EXPERIMENT_TIME_END": "2020-01-10 00:00",
        "DOMAIN_DEFINITION_METHOD": "lumped",
        "SUB_GRID_DISCRETIZATION": "lumped",
        "FORCING_DATASET": "ERA5",
        "HYDROLOGICAL_MODEL": "SUMMA",
        "FORCING_TIME_STEP_SIZE": 86400,  # daily, so processed output stays daily
        "CSFS_STATION_ID": STATION,
    }
    cfg.update(overrides)
    return cfg


class TestSymfluenceIntegration:
    """End-to-end behaviour inside a real SYMFLUENCE environment."""

    @pytest.fixture(autouse=True)
    def _requires_symfluence(self):
        pytest.importorskip("symfluence")
        pytest.importorskip("pandas")

    def _handler(self, tmp_path, **overrides):
        assert integration.HAVE_SYMFLUENCE
        return integration.CSFSStreamflowHandler(_symfluence_config(tmp_path, **overrides), logger)

    def test_entry_point_auto_registration(self):
        """Plain `import symfluence` discovers and registers the plugin."""
        import symfluence  # noqa: F401  (bootstrap runs plugin discovery)
        from symfluence.core.registries import R

        assert "csfs" in R.observation_handlers
        registered = R.observation_handlers.get("csfs")
        # Compare by identity-of-origin, not object identity: the import-safety
        # test reloads the integration module, which recreates the class object.
        assert registered.__qualname__ == "CSFSStreamflowHandler"
        assert registered.__module__ == "csfs.integrations.symfluence"
        # The same lookup the data manager uses for ADDITIONAL_OBSERVATIONS.
        from symfluence.data.observation.registry import ObservationRegistry

        assert "csfs" in ObservationRegistry.list_observations()

    def test_register_is_idempotent(self):
        from symfluence.core.registries import R

        integration.register()
        integration.register()
        assert R.observation_handlers.get("csfs").__qualname__ == "CSFSStreamflowHandler"

    def test_acquire_and_process_happy_path(self, tmp_path, monkeypatch):
        """Live-fetch path with fetch_observations_sync monkeypatched."""
        import csfs

        calls = {}

        def fake_fetch(provider, station_id, start, end, config=None):
            calls.update(provider=provider, station_id=station_id, start=start, end=end, config=config)
            return _make_chunk(10)

        monkeypatch.setattr(csfs, "fetch_observations_sync", fake_fetch)

        handler = self._handler(tmp_path, CSFS_CONNECTOR_CONFIG={"api_key": "k"})
        raw_path = handler.acquire()

        assert calls["provider"] == "usgs"
        assert calls["station_id"] == STATION
        assert calls["start"] == datetime(2020, 1, 1, tzinfo=UTC)
        assert calls["end"] == datetime(2020, 1, 10, tzinfo=UTC)
        assert calls["config"] == {"api_key": "k"}
        assert raw_path.is_file()
        assert raw_path.name == "csfs_usgs_01646500_raw.csv"
        assert raw_path.parent == handler.project_observations_dir / "streamflow" / "raw_data"

        processed = handler.process(raw_path)

        import pandas as pd

        assert processed.name == "TestDomain_streamflow_processed.csv"
        assert processed.parent == handler.project_observations_dir / "streamflow" / "preprocessed"
        df = pd.read_csv(processed, parse_dates=["datetime"], index_col="datetime")
        assert list(df.columns) == ["discharge_cms"]
        # Experiment window is Jan 1-10 and the chunk is daily from Jan 1.
        assert df["discharge_cms"].iloc[0] == 10.0
        assert df["discharge_cms"].max() <= 19.0
        assert df.index.min() == pd.Timestamp("2020-01-01")

    def test_acquire_skips_existing_raw_file(self, tmp_path, monkeypatch):
        import csfs

        handler = self._handler(tmp_path)
        raw_dir = handler.project_observations_dir / "streamflow" / "raw_data"
        raw_dir.mkdir(parents=True, exist_ok=True)
        existing = raw_dir / "csfs_usgs_01646500_raw.csv"
        integration.observations_to_raw_frame(_make_chunk(2).observations).to_csv(existing, index=False)

        def boom(*args, **kwargs):  # must not be called
            raise AssertionError("fetch_observations_sync should not run when raw data exists")

        monkeypatch.setattr(csfs, "fetch_observations_sync", boom)
        assert handler.acquire() == existing

    def test_acquire_store_mode(self, tmp_path, monkeypatch):
        """CSFS_DB_PATH reads from an existing store instead of fetching live."""
        import csfs

        db_path = tmp_path / "csfs.duckdb"
        db_path.write_bytes(b"")  # existence check only; store is monkeypatched

        rows = [
            {"timestamp": datetime(2020, 1, 1, tzinfo=UTC), "discharge_m3s": 7.0, "quality": "good"},
            {"timestamp": datetime(2020, 1, 2, tzinfo=UTC), "discharge_m3s": 8.0, "quality": "good"},
        ]

        class FakeStore:
            def __init__(self):
                self.queries = []

            async def __aenter__(self):
                return self

            async def __aexit__(self, *exc):
                return False

            async def get_observations(self, station_id, start=None, end=None):
                self.queries.append((station_id, start, end))
                return rows

        fake_store = FakeStore()
        monkeypatch.setattr(csfs, "open_store", lambda path, read_only=True: fake_store)
        monkeypatch.setattr(
            csfs,
            "fetch_observations_sync",
            lambda *a, **k: pytest.fail("store mode must not fetch live"),
        )

        handler = self._handler(tmp_path, CSFS_DB_PATH=str(db_path))
        raw_path = handler.acquire()

        assert fake_store.queries == [(STATION, datetime(2020, 1, 1, tzinfo=UTC), datetime(2020, 1, 10, tzinfo=UTC))]
        import pandas as pd

        raw = pd.read_csv(raw_path)
        assert raw["discharge_m3s"].tolist() == [7.0, 8.0]

    def test_acquire_store_mode_missing_db(self, tmp_path):
        handler = self._handler(tmp_path, CSFS_DB_PATH=str(tmp_path / "nope.duckdb"))
        with pytest.raises(Exception, match="CSFS_DB_PATH"):
            handler.acquire()

    def test_acquire_multiple_stations_returns_raw_dir(self, tmp_path, monkeypatch):
        import csfs

        monkeypatch.setattr(csfs, "fetch_observations_sync", lambda *a, **k: _make_chunk(3))
        handler = self._handler(tmp_path, CSFS_STATION_ID="usgs:01646500, usgs:01638500")
        raw_path = handler.acquire()
        assert raw_path == handler.project_observations_dir / "streamflow" / "raw_data"
        assert len(sorted(raw_path.glob("csfs_*_raw.csv"))) == 2
        # process() averages the stations into the single processed contract file.
        processed = handler.process(raw_path)
        assert processed.name == "TestDomain_streamflow_processed.csv"

    def test_unnamespaced_station_id_is_a_helpful_error(self, tmp_path):
        handler = self._handler(tmp_path, CSFS_STATION_ID="01646500")
        with pytest.raises(Exception, match="<provider>:<native_id>"):
            handler.acquire()

    def test_missing_station_id_is_a_helpful_error(self, tmp_path):
        handler = self._handler(tmp_path, CSFS_STATION_ID="")
        with pytest.raises(Exception, match="CSFS_STATION_ID"):
            handler.acquire()

    def test_falls_back_to_evaluation_station_id(self, tmp_path, monkeypatch):
        """A namespaced evaluation.streamflow.station_id is reused when CSFS_STATION_ID is unset."""
        import csfs

        monkeypatch.setattr(csfs, "fetch_observations_sync", lambda *a, **k: _make_chunk(3))
        handler = self._handler(tmp_path, CSFS_STATION_ID="", STATION_ID=STATION)
        raw_path = handler.acquire()
        assert raw_path.name == "csfs_usgs_01646500_raw.csv"

    def test_connector_config_must_be_a_mapping(self, tmp_path):
        handler = self._handler(tmp_path, CSFS_CONNECTOR_CONFIG="not-a-dict")
        with pytest.raises(Exception, match="CSFS_CONNECTOR_CONFIG"):
            handler.acquire()


class TestDropInProviderHandlers:
    """Drop-in handlers (registry keys usgs/wsc/smhi) inside a real SYMFLUENCE env."""

    @pytest.fixture(autouse=True)
    def _requires_symfluence(self):
        pytest.importorskip("symfluence")
        pytest.importorskip("pandas")

    def _handler(self, provider, tmp_path, **overrides):
        """Provider handler with a native-style config (no CSFS_* keys)."""
        assert integration.HAVE_SYMFLUENCE
        cfg = _symfluence_config(tmp_path, **overrides)
        cfg.pop("CSFS_STATION_ID", None)
        return integration.PROVIDER_HANDLERS[provider](cfg, logger)

    def _capture_fetch(self, monkeypatch, n=10):
        import csfs

        calls = {}

        def fake_fetch(provider, station_id, start, end, config=None):
            calls.update(provider=provider, station_id=station_id, start=start, end=end, config=config)
            return _make_chunk(n)

        monkeypatch.setattr(csfs, "fetch_observations_sync", fake_fetch)
        return calls

    # -- registration ----------------------------------------------------

    def test_entry_point_registers_all_drop_in_keys(self):
        """Plain `import symfluence` exposes csfs + the three provider names."""
        import symfluence  # noqa: F401
        from symfluence.core.registries import R

        for key in ("csfs", "usgs", "wsc", "smhi"):
            assert key in R.observation_handlers, key
        for key in ("usgs", "wsc", "smhi"):
            registered = R.observation_handlers.get(key)
            assert registered.__module__ == "csfs.integrations.symfluence"
            assert registered.__qualname__ == f"CSFS{key.upper()}StreamflowHandler"

    def test_native_handler_registrations_are_untouched(self):
        """No collision: natives live under *_streamflow keys and stay native."""
        import symfluence  # noqa: F401
        from symfluence.core.registries import R

        for key in ("usgs_streamflow", "wsc_streamflow", "smhi_streamflow"):
            assert R.observation_handlers.get(key).__module__.startswith("symfluence.")

    def test_register_is_idempotent_for_provider_keys(self):
        from symfluence.core.registries import R

        integration.register()
        before = {key: R.observation_handlers.get(key) for key in ("usgs", "wsc", "smhi")}
        integration.register()
        for key, cls in before.items():
            assert R.observation_handlers.get(key) is cls

    # -- station-id resolution from the existing config keys --------------

    @pytest.mark.parametrize(
        ("provider", "overrides", "expected_station"),
        [
            ("usgs", {"STATION_ID": "06191500"}, "usgs:06191500"),
            ("usgs", {"USGS_SITE_CODE": "06191500"}, "usgs:06191500"),
            ("usgs", {"STREAMFLOW_STATION_ID": "06191500"}, "usgs:06191500"),
            ("usgs", {"STATION_ID": "usgs:06191500"}, "usgs:06191500"),
            # NOTE: int station ids (YAML `STATION_ID: 6191500`) fail SymfluenceConfig
            # validation and degrade the whole config to a dict (framework behavior,
            # same for the native handlers) — int coercion is covered at the pure
            # resolver level instead (test_resolve_bare_native_station_ids).
            ("usgs", {"STATION_ID": "6191500"}, "usgs:06191500"),
            ("wsc", {"STATION_ID": "05BB001"}, "environment_canada:05BB001"),
            ("wsc", {"STATION_ID": "environment_canada:05BB001"}, "environment_canada:05BB001"),
            ("wsc", {"STATION_ID": "wsc:05BB001"}, "environment_canada:05BB001"),
            ("smhi", {"STATION_ID": "2357"}, "sweden_smhi:2357"),
        ],
    )
    def test_station_id_resolves_from_existing_config_keys(
        self, tmp_path, monkeypatch, provider, overrides, expected_station
    ):
        calls = self._capture_fetch(monkeypatch)
        handler = self._handler(provider, tmp_path, **overrides)
        handler.acquire()
        assert calls["provider"] == integration.PROVIDER_BACKENDS[provider].slug
        assert calls["station_id"] == expected_station

    def test_usgs_station_key_precedence_matches_native(self, tmp_path, monkeypatch):
        """evaluation station id wins over the data.* fallbacks, like handlers/usgs.py."""
        calls = self._capture_fetch(monkeypatch)
        handler = self._handler(
            "usgs", tmp_path, STATION_ID="06191500", USGS_SITE_CODE="01646500"
        )
        handler.acquire()
        assert calls["station_id"] == "usgs:06191500"

    def test_missing_station_id_lists_the_existing_keys(self, tmp_path):
        handler = self._handler("usgs", tmp_path)
        with pytest.raises(Exception, match="STATION_ID, USGS_SITE_CODE, STREAMFLOW_STATION_ID"):
            handler.acquire()

    def test_provider_handlers_do_not_read_csfs_station_id(self, tmp_path):
        """Drop-in handlers use only the native keys; CSFS_STATION_ID is the generic handler's."""
        cfg = _symfluence_config(tmp_path)  # sets CSFS_STATION_ID, no STATION_ID
        handler = integration.PROVIDER_HANDLERS["usgs"](cfg, logger)
        with pytest.raises(Exception, match="No station id configured"):
            handler.acquire()

    # -- connector config --------------------------------------------------

    def test_smhi_requests_the_15min_product(self, tmp_path, monkeypatch):
        """Parity with native SMHI requires hydroobs parameter 2 (15-min discharge)."""
        calls = self._capture_fetch(monkeypatch)
        handler = self._handler("smhi", tmp_path, STATION_ID="2357")
        handler.acquire()
        assert calls["config"] == {"resolution": "15min"}

    def test_usgs_and_wsc_pass_no_connector_config_by_default(self, tmp_path, monkeypatch):
        for provider, station in (("usgs", "06191500"), ("wsc", "05BB001")):
            calls = self._capture_fetch(monkeypatch)
            self._handler(provider, tmp_path / provider, STATION_ID=station).acquire()
            assert calls["config"] is None

    def test_connector_config_merges_user_overrides(self, tmp_path, monkeypatch):
        """CSFS_CONNECTOR_CONFIG wins over backend defaults (documented escape hatch)."""
        calls = self._capture_fetch(monkeypatch)
        handler = self._handler(
            "smhi", tmp_path, STATION_ID="2357",
            CSFS_CONNECTOR_CONFIG={"resolution": "daily", "api_key": "k"},
        )
        handler.acquire()
        assert calls["config"] == {"resolution": "daily", "api_key": "k"}

    # -- acquire + process happy path ---------------------------------------

    @pytest.mark.parametrize(
        ("provider", "station", "raw_name"),
        [
            ("usgs", "06191500", "csfs_usgs_06191500_raw.csv"),
            ("wsc", "05BB001", "csfs_environment_canada_05BB001_raw.csv"),
            ("smhi", "2357", "csfs_sweden_smhi_2357_raw.csv"),
        ],
    )
    def test_acquire_and_process_happy_path(self, tmp_path, monkeypatch, provider, station, raw_name):
        calls = self._capture_fetch(monkeypatch)
        handler = self._handler(provider, tmp_path, STATION_ID=station)

        raw_path = handler.acquire()
        assert calls["start"] == datetime(2020, 1, 1, tzinfo=UTC)
        assert calls["end"] == datetime(2020, 1, 10, tzinfo=UTC)
        assert raw_path.is_file()
        assert raw_path.name == raw_name
        assert raw_path.parent == handler.project_observations_dir / "streamflow" / "raw_data"

        processed = handler.process(raw_path)

        import pandas as pd

        assert processed.name == "TestDomain_streamflow_processed.csv"
        assert processed.parent == handler.project_observations_dir / "streamflow" / "preprocessed"
        df = pd.read_csv(processed, parse_dates=["datetime"], index_col="datetime")
        assert list(df.columns) == ["discharge_cms"]
        assert df["discharge_cms"].iloc[0] == 10.0
        assert df.index.min() == pd.Timestamp("2020-01-01")
        assert df.index.tz is None


# ---------------------------------------------------------------------------
# 4. OBS_CSV_V1 pure helpers (standalone, no symfluence required)
# ---------------------------------------------------------------------------


def test_obs_csv_v1_frame_contract():
    """Raw CSFS frame -> datetime,value,quality_flag (tz-naive UTC, m³/s)."""
    pd = pytest.importorskip("pandas")
    raw = pd.DataFrame(
        {
            "timestamp": ["2020-01-02T00:00:00+00:00", "2020-01-01T06:00:00+02:00"],
            "discharge_m3s": [5.0, 4.0],
            "quality": ["good", "estimated"],
        }
    )
    out = integration.obs_csv_v1_frame(raw)
    assert list(out.columns) == integration.OBS_CSV_V1_COLUMNS
    # Sorted chronologically, tz-naive UTC (the +02:00 row converts to 04:00Z).
    assert out["datetime"].tolist() == [
        pd.Timestamp("2020-01-01 04:00:00"),
        pd.Timestamp("2020-01-02 00:00:00"),
    ]
    assert out["datetime"].dt.tz is None
    assert out["value"].tolist() == [4.0, 5.0]
    assert out["quality_flag"].tolist() == ["estimated", "good"]


def test_obs_csv_v1_frame_window_is_half_open():
    """[start, end): the end-instant boundary bin is trimmed, start is kept."""
    pd = pytest.importorskip("pandas")
    raw = pd.DataFrame(
        {
            "timestamp": pd.date_range("2020-01-01", "2020-01-02", freq="h", tz="UTC"),  # 25 rows incl. end
            "discharge_m3s": range(25),
        }
    )
    out = integration.obs_csv_v1_frame(raw, start="2020-01-01 00:00", end="2020-01-02 00:00")
    assert len(out) == 24
    assert out["datetime"].min() == pd.Timestamp("2020-01-01 00:00:00")
    assert out["datetime"].max() == pd.Timestamp("2020-01-01 23:00:00")


def test_obs_csv_v1_frame_drops_unparseable_values_and_keeps_quality_optional():
    pd = pytest.importorskip("pandas")
    raw = pd.DataFrame(
        {
            "timestamp": ["2020-01-01T00:00:00Z", "2020-01-01T01:00:00Z"],
            "discharge_m3s": [1.5, "not-a-number"],
        }
    )
    out = integration.obs_csv_v1_frame(raw)
    assert len(out) == 1
    assert out["value"].tolist() == [1.5]
    assert out["quality_flag"].tolist() == [""]


def test_obs_csv_v1_frame_missing_columns_is_a_helpful_error():
    pd = pytest.importorskip("pandas")
    with pytest.raises(ValueError, match="missing required column"):
        integration.obs_csv_v1_frame(pd.DataFrame({"timestamp": []}))


def test_observation_capability_table_is_well_formed():
    """Pure capability facts: live drop-ins + LamaH-Ice artifact, parity grammar, csfs ungated."""
    import re

    specs = {spec.provider_id: spec for spec in integration.OBSERVATION_CAPABILITIES}
    assert set(specs) == {"USGS", "WSC", "SMHI", "LAMAH_ICE", "LAMAH_CE", "CAMELS_BR", "CAMELS_DE", "CAMELS_CL", "CAMELS_IND", "CAMELS_CH", "CAMELS_AUS", "CAMELS_US", "CAMELS_DK", "CAMELS_GB", "CAMELS_SE", "CAMELS_FR", "CSFS"}
    grade_re = re.compile(r"^(bit-identical|value-identical:.+)$")
    for spec in specs.values():
        assert spec.kinds == frozenset({"streamflow"})
        assert spec.station_id_scheme
        if spec.parity_grade is not None:
            assert grade_re.match(spec.parity_grade), spec
    assert specs["USGS"].parity_grade == "bit-identical"
    assert specs["WSC"].parity_grade.startswith("value-identical:")
    assert specs["SMHI"].parity_grade == "value-identical:rounding"
    assert specs["CSFS"].parity_grade is None  # ungated by design (parity gate refuses it)


def test_target_interface_version_is_hardcoded_semver():
    """Skew detection relies on a literal target, never the installed framework's."""
    import re

    assert re.match(r"^\d+\.\d+\.\d+$", integration.TARGET_INTERFACE_VERSION)


# ---------------------------------------------------------------------------
# 5. CommunityObservationBackend (requires symfluence; upstream mocked)
# ---------------------------------------------------------------------------


class TestCommunityObservationBackend:
    """The ObservationBackend protocol tier inside a real SYMFLUENCE env."""

    @pytest.fixture(autouse=True)
    def _requires_symfluence(self):
        pytest.importorskip("symfluence")
        pytest.importorskip("pandas")

    def _backend(self, tmp_path, **overrides):
        cfg = _symfluence_config(tmp_path, **overrides)
        cfg.pop("CSFS_STATION_ID", None)
        return integration.CommunityObservationBackend(cfg, logger)

    def _request(self, tmp_path, **overrides):
        from symfluence.data.backends.contract import ObservationRequest

        kwargs = dict(
            provider_id="USGS",
            station_ids=("06191500",),
            kind="streamflow",
            window=("2020-01-01 00:00", "2020-01-10 00:00"),
            target_dir=tmp_path / "obs_delivery",
        )
        kwargs.update(overrides)
        return ObservationRequest(**kwargs)

    def _capture_fetch(self, monkeypatch, n=10, freq_hours=24):
        import csfs

        calls = []

        def fake_fetch(provider, station_id, start, end, config=None):
            calls.append((provider, station_id, start, end))
            return _make_chunk(n, freq_hours=freq_hours)

        monkeypatch.setattr(csfs, "fetch_observations_sync", fake_fetch)
        return calls

    # -- registration ------------------------------------------------------

    def test_register_adds_the_backend_tier(self):
        import symfluence  # noqa: F401
        from symfluence.core.registries import R

        integration.register()  # idempotent
        registered = R.observation_backends.get("community")
        assert registered is not None
        assert registered.__qualname__ == "CommunityObservationBackend"
        assert registered.__module__ == "csfs.integrations.symfluence"
        # Handler-tier registrations stay (the documented fallthrough).
        for key in ("csfs", "usgs", "wsc", "smhi", "lamah_ice", "lamah_ce",
                    "camels_br", "camels_de", "camels_cl", "camels_ind", "camels_ch",
                    "camels_aus", "camels_us", "camels_dk", "camels_gb", "camels_se", "camels_fr"):
            assert key in R.observation_handlers, key

    def test_capabilities_map_the_pure_table(self, tmp_path):
        caps = {cap.provider_id: cap for cap in self._backend(tmp_path).capabilities()}
        assert set(caps) == {"USGS", "WSC", "SMHI", "LAMAH_ICE", "LAMAH_CE", "CAMELS_BR", "CAMELS_DE", "CAMELS_CL", "CAMELS_IND", "CAMELS_CH", "CAMELS_AUS", "CAMELS_US", "CAMELS_DK", "CAMELS_GB", "CAMELS_SE", "CAMELS_FR", "CSFS"}
        assert caps["USGS"].parity_grade == "bit-identical"
        assert caps["CSFS"].parity_grade is None
        for cap in caps.values():
            assert cap.kinds == frozenset({"streamflow"})
            assert cap.auth == frozenset()

    # -- acquire -------------------------------------------------------------

    def test_acquire_serves_the_full_layered_delivery(self, tmp_path, monkeypatch):
        """One call: existing fetch + byte-matched processing + OBS_CSV_V1 + manifest."""
        from symfluence.data.backends.contract import SchemaId, read_manifest

        calls = self._capture_fetch(monkeypatch)
        backend = self._backend(tmp_path)
        request = self._request(tmp_path)

        result = backend.acquire(request)

        # Fetch went through the existing handler internals, station namespaced.
        assert calls == [("usgs", "usgs:06191500", datetime(2020, 1, 1, tzinfo=UTC),
                          datetime(2020, 1, 10, tzinfo=UTC))]

        # Protocol delivery: OBS_CSV_V1 file + sidecar manifest in target_dir.
        assert result.schema is SchemaId.OBS_CSV_V1
        assert result.dataset_id == "USGS"
        assert result.variables_delivered == frozenset({"streamflow"})
        assert [p.name for p in result.paths] == ["csfs_usgs_06191500_obs_v1.csv"]

        import pandas as pd

        obs = pd.read_csv(result.paths[0])
        assert list(obs.columns) == ["datetime", "value", "quality_flag"]
        assert obs["value"].iloc[0] == 10.0

        manifest = read_manifest(request.target_dir)
        assert manifest["schema"] == "obs-csv-v1"
        assert manifest["backend"] == "community"

        # The legacy artifacts are still produced byte-for-byte: raw CSV in the
        # conventional dir and the processed calibration CSV.
        processed = Path(manifest["provenance"]["processed_path"])
        assert processed.name == "TestDomain_streamflow_processed.csv"
        assert processed.exists()
        df = pd.read_csv(processed, parse_dates=["datetime"], index_col="datetime")
        assert list(df.columns) == ["discharge_cms"]
        assert df["discharge_cms"].iloc[0] == 10.0

    def test_delivery_obeys_the_half_open_window(self, tmp_path, monkeypatch):
        """An inclusive-end upstream bin (NWIS endDT) is trimmed from the delivery."""
        pd = pytest.importorskip("pandas")

        # Hourly chunk running PAST the window end (window: Jan 1 - Jan 10).
        self._capture_fetch(monkeypatch, n=24 * 9 + 1, freq_hours=1)
        backend = self._backend(tmp_path)
        result = backend.acquire(self._request(tmp_path))

        times = pd.to_datetime(pd.read_csv(result.paths[0])["datetime"])
        assert times.max() < pd.Timestamp("2020-01-10 00:00:00")
        assert times.min() >= pd.Timestamp("2020-01-01 00:00:00")

    def test_generic_csfs_provider_uses_namespaced_ids(self, tmp_path, monkeypatch):
        calls = self._capture_fetch(monkeypatch)
        backend = self._backend(tmp_path)
        result = backend.acquire(self._request(
            tmp_path, provider_id="CSFS", station_ids=("uk_ea:3400TH",)))

        assert calls[0][0] == "uk_ea"
        assert calls[0][1] == "uk_ea:3400TH"
        assert [p.name for p in result.paths] == ["csfs_uk_ea_3400TH_obs_v1.csv"]

    def test_station_ids_fall_back_to_config_resolution(self, tmp_path, monkeypatch):
        """Empty request.station_ids => the handler's own config chain resolves."""
        calls = self._capture_fetch(monkeypatch)
        backend = self._backend(tmp_path, STATION_ID="06191500")
        backend.acquire(self._request(tmp_path, station_ids=()))
        assert calls[0][1] == "usgs:06191500"

    def test_reacquisition_reuses_the_raw_delivery(self, tmp_path, monkeypatch):
        calls = self._capture_fetch(monkeypatch)
        backend = self._backend(tmp_path)

        backend.acquire(self._request(tmp_path))
        backend.acquire(self._request(tmp_path))
        assert len(calls) == 1, "second acquisition must reuse the existing raw CSV"

    # -- protocol error taxonomy ---------------------------------------------

    def test_unknown_provider_is_dataset_unsupported(self, tmp_path):
        from symfluence.data.backends.errors import DatasetUnsupported

        with pytest.raises(DatasetUnsupported, match="does not serve provider"):
            self._backend(tmp_path).acquire(self._request(tmp_path, provider_id="NOSUCH"))

    def test_unsupported_kind_is_dataset_unsupported(self, tmp_path):
        from symfluence.data.backends.errors import DatasetUnsupported

        with pytest.raises(DatasetUnsupported, match="streamflow"):
            self._backend(tmp_path).acquire(self._request(tmp_path, kind="swe"))

    def test_missing_config_is_an_acquisition_error(self, tmp_path):
        from symfluence.data.backends.errors import AcquisitionError

        backend = integration.CommunityObservationBackend(None, logger)
        with pytest.raises(AcquisitionError, match="requires a framework config"):
            backend.acquire(self._request(tmp_path))

    def test_connector_failures_map_to_upstream_outage(self, tmp_path, monkeypatch):
        from symfluence.data.backends.errors import UpstreamOutage

        import csfs
        from csfs.core.exceptions import ConnectorError

        def boom(*args, **kwargs):
            raise ConnectorError("usgs", "503 from NWIS")

        monkeypatch.setattr(csfs, "fetch_observations_sync", boom)
        with pytest.raises(UpstreamOutage) as excinfo:
            self._backend(tmp_path).acquire(self._request(tmp_path))
        assert excinfo.value.upstream == "usgs"
        assert isinstance(excinfo.value.__cause__, ConnectorError)

    def test_config_errors_map_to_acquisition_error(self, tmp_path):
        from symfluence.data.backends.errors import AcquisitionError

        backend = self._backend(tmp_path)  # no station id anywhere
        with pytest.raises(AcquisitionError, match="No station id configured"):
            backend.acquire(self._request(tmp_path, station_ids=()))
