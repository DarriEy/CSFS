# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""SYMFLUENCE streamflow-observation adapter for CSFS.

This module lets SYMFLUENCE pull calibration/evaluation streamflow from any
of the CSFS provider connectors (or from a pre-built CSFS DuckDB store)
through the standard observation-handler pipeline::

    ADDITIONAL_OBSERVATIONS: csfs
    CSFS_STATION_ID: "usgs:01646500"     # canonical "<provider>:<native_id>"

It is intentionally decoupled:

* The pure helpers (:func:`parse_station_ids`, :func:`ensure_utc`,
  :func:`observations_to_raw_frame`, :func:`standardize_frame`,
  :func:`combine_station_frames`) have no SYMFLUENCE dependency and are
  unit-tested standalone.
* :class:`CSFSStreamflowHandler` only resolves the SYMFLUENCE base class at
  import time; if SYMFLUENCE is absent the class still imports (its base
  degrades to ``object``) so ``import csfs`` never fails.
* :func:`register` is the zero-arg hook referenced by the
  ``symfluence.plugins`` entry point in ``pyproject.toml``; SYMFLUENCE's
  plugin discovery calls it on ``import symfluence``, so no framework-side
  changes are needed.

Configuration keys (flat YAML keys; all CSFS-specific keys are optional
extras carried through SYMFLUENCE's config):

``CSFS_STATION_ID``
    One or more CSFS station ids, as a single string, a comma-separated
    string, or a YAML list. Ids must be namespaced ``"<provider>:<native_id>"``
    (e.g. ``"usgs:01646500"``); the prefix selects the provider connector.
    Falls back to ``STATION_ID`` (``evaluation.streamflow.station_id``) when
    unset — that value must then also be namespaced.
``CSFS_CONNECTOR_CONFIG``
    Optional mapping of provider-specific settings (e.g. API keys) passed to
    the connector for live fetches.
``CSFS_DB_PATH``
    Optional path to an existing CSFS DuckDB store. When set, observations
    are read from the store instead of fetched live from the provider.

CSFS guarantees discharge in m³/s and timestamps in UTC, so processing is a
pure reshape — no unit conversion.
"""

from __future__ import annotations

from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    import pandas as pd

# Resolve the SYMFLUENCE base class defensively so importing this module
# never hard-fails when SYMFLUENCE is not installed.
try:  # pragma: no cover - exercised only with SYMFLUENCE present
    from symfluence.data.observation.base import BaseObservationHandler as _Base

    HAVE_SYMFLUENCE = True
except Exception:  # noqa: BLE001 - any import failure means "not available"
    _Base = object  # type: ignore[assignment, misc]
    HAVE_SYMFLUENCE = False

#: Columns of the raw per-station CSV written into observations/streamflow/raw_data.
RAW_COLUMNS = ["timestamp", "discharge_m3s", "quality"]

_MISSING_STATION_MSG = (
    "CSFS_STATION_ID is required to acquire CSFS streamflow observations. "
    "Set it to a namespaced CSFS station id such as 'usgs:01646500' "
    "(or a comma-separated list / YAML list of them)."
)


# ---------------------------------------------------------------------------
# Pure helpers (no SYMFLUENCE dependency; unit-tested standalone)
# ---------------------------------------------------------------------------


def _require_pandas() -> None:
    """Raise a helpful ImportError when the optional pandas extra is missing."""
    try:
        import pandas  # noqa: F401
    except ImportError as exc:  # pragma: no cover - pandas installed in dev/CI envs
        raise ImportError(
            "The SYMFLUENCE integration requires pandas. Install it with: "
            'pip install "community-streamflow-service[pandas]"'
        ) from exc


def parse_station_ids(raw: Any) -> list[tuple[str, str]]:
    """Parse a ``CSFS_STATION_ID`` config value into ``(provider, station_id)`` pairs.

    Accepts a single id, a comma-separated string, or a list/tuple of ids.
    Every id must be a namespaced CSFS id (``"<provider>:<native_id>"``); the
    provider slug is derived from the prefix.

    Raises:
        ValueError: If the value is missing/empty, or an id has no
            ``provider:`` prefix.
    """
    if raw is None or (isinstance(raw, str) and not raw.strip()):
        raise ValueError(_MISSING_STATION_MSG)

    if isinstance(raw, str):
        ids = [part.strip() for part in raw.split(",") if part.strip()]
    elif isinstance(raw, (list, tuple)):
        ids = [str(part).strip() for part in raw if str(part).strip()]
    else:
        ids = [str(raw).strip()]

    if not ids:
        raise ValueError(_MISSING_STATION_MSG)

    pairs: list[tuple[str, str]] = []
    for sid in ids:
        provider, sep, native = sid.partition(":")
        if not sep or not provider.strip() or not native.strip():
            raise ValueError(
                f"Station id {sid!r} is not a namespaced CSFS id. CSFS station ids "
                "take the form '<provider>:<native_id>' — e.g. 'usgs:01646500' or "
                "'uk_ea:3400TH' — where the prefix selects the provider connector. "
                "Run 'csfs providers' to list available provider slugs."
            )
        pairs.append((provider.strip().lower(), sid))
    return pairs


def ensure_utc(value: Any) -> datetime:
    """Coerce a config/pandas timestamp to a timezone-aware UTC datetime.

    Naive values are interpreted as UTC (CSFS works exclusively in UTC);
    aware values are converted.
    """
    if value is None:
        raise ValueError(
            "Experiment start/end times are required to fetch CSFS observations "
            "(set EXPERIMENT_TIME_START / EXPERIMENT_TIME_END)."
        )
    dt = value.to_pydatetime() if hasattr(value, "to_pydatetime") else value
    if not isinstance(dt, datetime):
        dt = datetime.fromisoformat(str(dt))
    if dt.tzinfo is None:
        return dt.replace(tzinfo=UTC)
    return dt.astimezone(UTC)


def observations_to_raw_frame(observations: Any) -> pd.DataFrame:
    """Convert CSFS observations to the raw CSV frame written under raw_data/.

    Accepts an iterable of :class:`csfs.Observation` models (live-fetch path)
    or of plain dicts (``DuckDBStore.get_observations`` rows). Returns a
    DataFrame with the :data:`RAW_COLUMNS` columns; discharge stays in m³/s
    and timestamps stay UTC.
    """
    _require_pandas()
    import pandas as pd

    rows = []
    for obs in observations:
        if isinstance(obs, dict):
            ts, discharge, quality = obs.get("timestamp"), obs.get("discharge_m3s"), obs.get("quality")
        else:
            ts, discharge, quality = obs.timestamp, obs.discharge_m3s, getattr(obs, "quality", None)
        rows.append(
            {
                "timestamp": ts,
                "discharge_m3s": discharge,
                "quality": "" if quality is None else str(quality),
            }
        )
    return pd.DataFrame(rows, columns=RAW_COLUMNS)


def standardize_frame(raw: pd.DataFrame) -> pd.DataFrame:
    """Shape a raw CSFS frame onto SYMFLUENCE's processed-streamflow contract.

    Returns a DataFrame indexed by tz-naive UTC datetimes (index name
    ``datetime``) with a single ``discharge_cms`` column. CSFS discharge is
    already m³/s, so this is a rename + index reshape, dropping missing
    values and sorting chronologically.
    """
    _require_pandas()
    import pandas as pd

    missing = {"timestamp", "discharge_m3s"} - set(raw.columns)
    if missing:
        raise ValueError(f"Raw CSFS frame is missing required column(s): {sorted(missing)}")

    timestamps = pd.to_datetime(raw["timestamp"], utc=True).dt.tz_localize(None)
    df = pd.DataFrame(
        {"discharge_cms": pd.to_numeric(raw["discharge_m3s"], errors="coerce").to_numpy()},
        index=pd.DatetimeIndex(timestamps),
    )
    df.index.name = "datetime"
    return df.dropna(subset=["discharge_cms"]).sort_index()


def combine_station_frames(frames: list[pd.DataFrame]) -> pd.DataFrame:
    """Combine per-station standardized frames into one ``discharge_cms`` series.

    A single frame passes through unchanged; multiple stations are averaged
    per timestep (mirroring the GRDC multi-station behaviour).
    """
    _require_pandas()
    import pandas as pd

    if not frames:
        raise ValueError("No CSFS station data to combine")
    if len(frames) == 1:
        return frames[0]
    merged = pd.concat([frame["discharge_cms"] for frame in frames], axis=1)
    combined = pd.DataFrame({"discharge_cms": merged.mean(axis=1)})
    combined.index.name = "datetime"
    return combined


# ---------------------------------------------------------------------------
# SYMFLUENCE observation handler
# ---------------------------------------------------------------------------


class CSFSStreamflowHandler(_Base):
    """SYMFLUENCE observation handler backed by the CSFS provider network.

    ``acquire()`` fetches each configured station's series (live via
    :func:`csfs.fetch_observations_sync`, or from a CSFS DuckDB store when
    ``CSFS_DB_PATH`` is set) and writes one raw CSV per station into
    ``observations/streamflow/raw_data/``. ``process()`` standardizes the
    raw CSV(s) to the calibration pipeline's processed contract
    (``{domain}_streamflow_processed.csv`` with a ``datetime`` index and a
    ``discharge_cms`` column, matching the USGS/WSC handlers).
    """

    obs_type = "streamflow"
    source_name = "CSFS"
    SOURCE_INFO = {
        "source": "CSFS (Community Streamflow Service)",
        "url": "https://github.com/DarriEy/CSFS",
    }

    # -- acquisition ---------------------------------------------------

    def acquire(self) -> Path:  # pragma: no cover - exercised by symfluence integration tests
        """Fetch raw CSFS observations and write per-station CSVs.

        Returns the raw CSV path for a single station, or the raw_data
        directory when multiple stations were configured.
        """
        self._require_symfluence()
        stations = parse_station_ids(self._station_id_config())
        connector_config = self._connector_config()
        db_path = self.config_dict.get("CSFS_DB_PATH")
        force_download = bool(
            self._get_config_value(
                lambda: self.config.data.force_download, default=False, dict_key="FORCE_DOWNLOAD"
            )
        )

        start = ensure_utc(self.start_date)
        end = ensure_utc(self.end_date)

        raw_dir = Path(self.project_observations_dir) / "streamflow" / "raw_data"
        raw_dir.mkdir(parents=True, exist_ok=True)

        raw_files: list[Path] = []
        for provider, station_id in stations:
            raw_file = raw_dir / f"csfs_{station_id.replace(':', '_')}_raw.csv"
            if raw_file.exists() and not force_download:
                self.logger.info(f"Using existing CSFS raw data: {raw_file}")
                raw_files.append(raw_file)
                continue

            if db_path:
                frame = self._read_from_store(Path(db_path), station_id, start, end)
            else:
                frame = self._fetch_live(provider, station_id, start, end, connector_config)

            frame.to_csv(raw_file, index=False)
            self.logger.info(f"CSFS raw observations written: {raw_file} ({len(frame)} records)")
            raw_files.append(raw_file)

        return raw_files[0] if len(raw_files) == 1 else raw_dir

    def _station_id_config(self) -> Any:  # pragma: no cover - symfluence-only config access
        """CSFS_STATION_ID, falling back to the shared evaluation station id."""
        raw = self.config_dict.get("CSFS_STATION_ID")
        if raw in (None, "", "default"):
            raw = self._get_config_value(
                lambda: self.config.evaluation.streamflow.station_id,
                default=None,
                dict_key="STATION_ID",
            )
        if isinstance(raw, str) and raw.strip().lower() == "default":
            raw = None
        return raw

    def _connector_config(self) -> dict | None:  # pragma: no cover - symfluence-only config access
        """Optional CSFS_CONNECTOR_CONFIG mapping (API keys etc.)."""
        cfg = self.config_dict.get("CSFS_CONNECTOR_CONFIG")
        if cfg is None:
            return None
        if not isinstance(cfg, dict):
            raise ValueError(
                "CSFS_CONNECTOR_CONFIG must be a mapping of provider-specific "
                f"settings (got {type(cfg).__name__})"
            )
        return cfg

    def _fetch_live(
        self,
        provider: str,
        station_id: str,
        start: datetime,
        end: datetime,
        connector_config: dict | None,
    ) -> pd.DataFrame:  # pragma: no cover - exercised by symfluence integration tests
        """Live one-shot fetch from the provider via the CSFS facade."""
        import csfs

        self.logger.info(
            f"Fetching {station_id} from CSFS provider '{provider}' "
            f"({start:%Y-%m-%d} to {end:%Y-%m-%d})"
        )
        chunk = csfs.fetch_observations_sync(provider, station_id, start=start, end=end, config=connector_config)
        return observations_to_raw_frame(chunk.observations)

    def _read_from_store(
        self,
        db_path: Path,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> pd.DataFrame:  # pragma: no cover - exercised by symfluence integration tests
        """Read observations from an existing CSFS DuckDB store."""
        import asyncio

        import csfs

        if not db_path.exists():
            raise FileNotFoundError(
                f"CSFS_DB_PATH does not exist: {db_path}. Build a store with "
                "'csfs acquire' or unset CSFS_DB_PATH to fetch live."
            )

        async def _query() -> list[dict]:
            async with csfs.open_store(db_path) as store:
                return await store.get_observations(station_id, start=start, end=end)

        self.logger.info(f"Reading {station_id} from CSFS store {db_path}")
        rows = asyncio.run(_query())
        if not rows:
            self.logger.warning(
                f"CSFS store {db_path} holds no observations for {station_id} "
                f"between {start:%Y-%m-%d} and {end:%Y-%m-%d}"
            )
        return observations_to_raw_frame(rows)

    # -- processing ----------------------------------------------------

    def process(self, input_path: Path) -> Path:  # pragma: no cover - exercised by symfluence integration tests
        """Standardize raw CSFS CSV(s) to the processed streamflow contract."""
        self._require_symfluence()
        _require_pandas()
        import pandas as pd

        csv_files = [input_path] if input_path.is_file() else sorted(input_path.glob("csfs_*_raw.csv"))
        if not csv_files:
            raise FileNotFoundError(f"No CSFS raw CSV files found at {input_path}")

        frames: list[pd.DataFrame] = []
        for csv_file in csv_files:
            standardized = standardize_frame(pd.read_csv(csv_file))
            if standardized.empty:
                self.logger.warning(f"No usable records in {csv_file.name}")
                continue
            frames.append(standardized)
        if not frames:
            raise ValueError(f"No CSFS observations could be processed from {input_path}")

        df = combine_station_frames(frames)

        # Filter to the experiment period (timestamps are tz-naive UTC).
        df = df.loc[self.start_date : self.end_date]

        # Resample/align to the configured model timestep, mirroring the
        # USGS/WSC handlers (mean per step, small gaps interpolated).
        resample_freq = self._get_resample_freq()
        resampled = df["discharge_cms"].resample(resample_freq).mean()
        resampled = resampled.interpolate(method="time", limit_direction="both", limit=30)

        output_dir = Path(self.project_observations_dir) / "streamflow" / "preprocessed"
        output_dir.mkdir(parents=True, exist_ok=True)
        output_file = output_dir / f"{self.domain_name}_streamflow_processed.csv"

        resampled.to_csv(output_file, header=True, index_label="datetime")
        self.logger.info(f"CSFS streamflow processing complete: {output_file}")
        return output_file

    # -- guards ----------------------------------------------------------

    def _require_symfluence(self) -> None:
        """Guard for standalone use of the degraded (no-SYMFLUENCE) class."""
        if not HAVE_SYMFLUENCE:
            raise RuntimeError(
                "CSFSStreamflowHandler requires SYMFLUENCE. Install both packages "
                "in the same environment (pip install symfluence "
                "community-streamflow-service) to use this handler."
            )


# ---------------------------------------------------------------------------
# Plugin entry point
# ---------------------------------------------------------------------------


def register() -> None:
    """Register the CSFS observation handler with SYMFLUENCE (idempotent).

    Zero-arg hook referenced by the ``symfluence.plugins`` entry point;
    SYMFLUENCE's bootstrap calls it automatically on ``import symfluence``.
    After registration, ``ADDITIONAL_OBSERVATIONS: csfs`` dispatches to
    :class:`CSFSStreamflowHandler` with no framework changes.
    """
    if not HAVE_SYMFLUENCE:
        raise ImportError(
            "Cannot register the CSFS plugin: symfluence is not importable in this environment."
        )
    from symfluence.core.registries import R  # pragma: no cover - symfluence-only

    if "csfs" not in R.observation_handlers:  # pragma: no cover - symfluence-only
        R.observation_handlers.add("csfs", CSFSStreamflowHandler)


# Self-register when SYMFLUENCE is importable. This complements the entry
# point: if THIS module is imported before symfluence, the defensive import
# above triggers symfluence's bootstrap mid-module, and its plugin discovery
# then sees a partially-initialized module (no `register` yet) and skips the
# csfs entry point. Registering here, at the end of the module body, makes
# the handler available regardless of import order; register() is idempotent
# so the entry-point path stays harmless.
if HAVE_SYMFLUENCE:  # pragma: no cover - exercised only with SYMFLUENCE present
    import contextlib

    # Never let registration break `import csfs.integrations.symfluence`.
    with contextlib.suppress(Exception):
        register()
