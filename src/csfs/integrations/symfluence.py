# SPDX-License-Identifier: GPL-3.0-or-later
# Copyright 2026 Darri Eythorsson <dareyt@gmail.com>
"""SYMFLUENCE streamflow-observation adapter for CSFS.

This module lets SYMFLUENCE pull calibration/evaluation streamflow from any
of the CSFS provider connectors (or from a pre-built CSFS DuckDB store).
Three tiers, layered (highest priority first under ``DATA_ACCESS:
community``):

1. **ObservationBackend tier** (:class:`CommunityObservationBackend`,
   contract 0.2.0): registered under ``R.observation_backends``;
   SYMFLUENCE's observed_processor consults it FIRST under community mode,
   builds an ``ObservationRequest``, and the backend reuses the handler
   classes below as its internals (fetch + byte-matched processing), then
   writes the OBS_CSV_V1 protocol delivery + sidecar manifest.
2. **Registry-handler tier** (the drop-in keys ``usgs``/``wsc``/``smhi``
   plus the generic ``csfs`` key): the pre-protocol integration, KEPT as the
   fallthrough — it still serves when the backend tier declines (e.g. the
   parity gate refuses an ungraded provider), when an older symfluence
   without ``R.observation_backends`` is installed, and it remains the only
   route for ``ADDITIONAL_OBSERVATIONS: csfs``. Redundant under community
   mode but harmless by design.
3. **Legacy tier**: SYMFLUENCE's native provider branches — untouched, and
   the default outside community mode.

The two registration modes of the handler tier:

**Drop-in community backend** (registry keys ``usgs`` / ``wsc`` / ``smhi``):
per-provider handlers that read the *existing* SYMFLUENCE station-id config
keys — no new keys required — so a stock experiment switches its primary
streamflow acquisition to CSFS with a single line::

    DATA_ACCESS: community            # STREAMFLOW_DATA_PROVIDER stays USGS/WSC/SMHI

SYMFLUENCE's registry-first streamflow dispatch resolves the lowercased
provider name in the observation-handler registry and routes to these
handlers when the backend is ``community`` (the default/native path is
untouched). See :data:`PROVIDER_BACKENDS` and :func:`make_provider_handler`.

**Generic handler** (registry key ``csfs``): any of CSFS's 86 providers via
namespaced station ids through the ``ADDITIONAL_OBSERVATIONS`` mechanism::

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

from collections.abc import Callable
from datetime import UTC, datetime
from pathlib import Path
from typing import TYPE_CHECKING, Any, NamedTuple

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


#: Column layout of the contract's OBS_CSV_V1 schema (UTC, SI).
OBS_CSV_V1_COLUMNS = ["datetime", "value", "quality_flag"]


def obs_csv_v1_frame(raw: pd.DataFrame, start: Any = None, end: Any = None) -> pd.DataFrame:
    """Shape a raw CSFS frame onto the contract's OBS_CSV_V1 layout.

    Columns ``datetime,value,quality_flag``: tz-naive UTC timestamps (naive
    == UTC per the contract), discharge in m³/s, provider quality flag
    passed through. When *start*/*end* are given, the frame is trimmed to
    the **half-open UTC window** ``[start, end)`` — the contract's normative
    window rule (the protocol delivery is window-trimmed; the legacy
    processed CSV keeps its historical inclusive-end slice, see
    :class:`CommunityObservationBackend`).
    """
    _require_pandas()
    import pandas as pd

    missing = {"timestamp", "discharge_m3s"} - set(raw.columns)
    if missing:
        raise ValueError(f"Raw CSFS frame is missing required column(s): {sorted(missing)}")

    quality = raw["quality"] if "quality" in raw.columns else ""
    df = pd.DataFrame(
        {
            "datetime": pd.to_datetime(raw["timestamp"], utc=True).dt.tz_localize(None),
            "value": pd.to_numeric(raw["discharge_m3s"], errors="coerce"),
            "quality_flag": quality,
        }
    )
    df = df.dropna(subset=["value"]).sort_values("datetime").reset_index(drop=True)
    if start is not None:
        df = df[df["datetime"] >= ensure_utc(start).replace(tzinfo=None)]
    if end is not None:
        df = df[df["datetime"] < ensure_utc(end).replace(tzinfo=None)]
    return df[OBS_CSV_V1_COLUMNS].reset_index(drop=True)


# ---------------------------------------------------------------------------
# Drop-in provider backends (pure declarations; no SYMFLUENCE dependency)
# ---------------------------------------------------------------------------


class StationKey(NamedTuple):
    """One config location a native SYMFLUENCE handler reads its station id from."""

    accessor: Callable[[Any], Any]  # typed-config accessor, e.g. cfg.data.usgs_site_code
    dict_key: str  # legacy flat YAML key, e.g. "USGS_SITE_CODE"


class ProviderBackend(NamedTuple):
    """How a SYMFLUENCE streamflow provider name maps onto a CSFS connector."""

    slug: str  # CSFS connector slug (``csfs providers``)
    station_keys: tuple[StationKey, ...]  # resolution order = the native handler's
    connector_defaults: dict[str, str]  # fixed connector config (e.g. SMHI 15-min)
    normalize: Callable[[str], str] | None  # native-id normalizer (e.g. USGS zfill)


def _normalize_usgs_site(native_id: str) -> str:
    """Zero-pad short numeric USGS site codes to 8 digits (native-handler parity)."""
    if native_id.isdigit() and len(native_id) < 8:
        return native_id.zfill(8)
    return native_id


_EVAL_STATION_KEY = StationKey(lambda cfg: cfg.evaluation.streamflow.station_id, "STATION_ID")

#: SYMFLUENCE provider name (lowercased ``STREAMFLOW_DATA_PROVIDER``) -> CSFS
#: backend. Station-key order mirrors each native handler's own lookups
#: (``handlers/usgs.py`` / ``wsc.py`` / ``smhi.py``) so the drop-in needs no
#: new config keys. SMHI pins the 15-minute discharge product (hydroobs
#: parameter 2) because that is what the native SMHI handler downloads; the
#: CSFS default would be the daily product (parameter 1).
PROVIDER_BACKENDS: dict[str, ProviderBackend] = {
    "usgs": ProviderBackend(
        slug="usgs",
        station_keys=(
            _EVAL_STATION_KEY,
            StationKey(lambda cfg: cfg.data.usgs_site_code, "USGS_SITE_CODE"),
            StationKey(lambda cfg: cfg.data.streamflow_station_id, "STREAMFLOW_STATION_ID"),
        ),
        connector_defaults={},
        normalize=_normalize_usgs_site,
    ),
    "wsc": ProviderBackend(
        slug="environment_canada",
        station_keys=(_EVAL_STATION_KEY,),
        connector_defaults={},
        normalize=None,
    ),
    "smhi": ProviderBackend(
        slug="sweden_smhi",
        station_keys=(_EVAL_STATION_KEY,),
        connector_defaults={"resolution": "15min"},
        normalize=None,
    ),
    # Dataset-artifact provider (not a live API): LamaH-Ice gauges read from the
    # published HydroShare archive via the iceland_lamahice connector
    # (checksum-verified on download). Same handler machinery as the live
    # drop-ins — the connector abstracts static-archive vs live-API fetch.
    "lamah_ice": ProviderBackend(
        slug="iceland_lamahice",
        station_keys=(
            _EVAL_STATION_KEY,
            StationKey(lambda cfg: cfg.data.streamflow_station_id, "STREAMFLOW_STATION_ID"),
        ),
        connector_defaults={},
        normalize=None,
    ),
    # Dataset-artifact provider: LamaH-CE gauges from the published Zenodo
    # archive via the lamah_ce connector (checksum-verified on download).
    "lamah_ce": ProviderBackend(
        slug="lamah_ce",
        station_keys=(
            _EVAL_STATION_KEY,
            StationKey(lambda cfg: cfg.data.streamflow_station_id, "STREAMFLOW_STATION_ID"),
        ),
        connector_defaults={},
        normalize=None,
    ),
    # Dataset-artifact provider: CAMELS-BR (ANA gauges) from the published
    # Zenodo archive via the camels_br connector (checksum-verified).
    "camels_br": ProviderBackend(
        slug="camels_br",
        station_keys=(
            _EVAL_STATION_KEY,
            StationKey(lambda cfg: cfg.data.streamflow_station_id, "STREAMFLOW_STATION_ID"),
        ),
        connector_defaults={},
        normalize=None,
    ),
    # Dataset-artifact provider: CAMELS-DE (authoritative standalone Zenodo
    # bundle) via the camels_de connector (checksum-verified).
    "camels_de": ProviderBackend(
        slug="camels_de",
        station_keys=(
            _EVAL_STATION_KEY,
            StationKey(lambda cfg: cfg.data.streamflow_station_id, "STREAMFLOW_STATION_ID"),
        ),
        connector_defaults={},
        normalize=None,
    ),
    # Dataset-artifact provider: CAMELS-CL (DGA gauges) from the authoritative
    # PANGAEA archive via the camels_cl connector (checksum-verified).
    "camels_cl": ProviderBackend(
        slug="camels_cl",
        station_keys=(
            _EVAL_STATION_KEY,
            StationKey(lambda cfg: cfg.data.streamflow_station_id, "STREAMFLOW_STATION_ID"),
        ),
        connector_defaults={},
        normalize=None,
    ),
    # Dataset-artifact providers: CAMELS-IND (CWC gauges) and CAMELS-CH (BAFU
    # gauges) from their Zenodo archives via the respective connectors.
    "camels_ind": ProviderBackend(
        slug="camels_ind",
        station_keys=(
            _EVAL_STATION_KEY,
            StationKey(lambda cfg: cfg.data.streamflow_station_id, "STREAMFLOW_STATION_ID"),
        ),
        connector_defaults={},
        normalize=None,
    ),
    "camels_ch": ProviderBackend(
        slug="camels_ch",
        station_keys=(
            _EVAL_STATION_KEY,
            StationKey(lambda cfg: cfg.data.streamflow_station_id, "STREAMFLOW_STATION_ID"),
        ),
        connector_defaults={},
        normalize=None,
    ),
    "camels_aus": ProviderBackend(
        slug="camels_aus",
        station_keys=(
            _EVAL_STATION_KEY,
            StationKey(lambda cfg: cfg.data.streamflow_station_id, "STREAMFLOW_STATION_ID"),
        ),
        connector_defaults={},
        normalize=None,
    ),
    "camels_gb": ProviderBackend(
        slug="camels_gb",
        station_keys=(
            _EVAL_STATION_KEY,
            StationKey(lambda cfg: cfg.data.streamflow_station_id, "STREAMFLOW_STATION_ID"),
        ),
        connector_defaults={},
        normalize=None,
    ),
    "camels_us": ProviderBackend(
        slug="camels_us",
        station_keys=(
            _EVAL_STATION_KEY,
            StationKey(lambda cfg: cfg.data.usgs_site_code, "USGS_SITE_CODE"),
            StationKey(lambda cfg: cfg.data.streamflow_station_id, "STREAMFLOW_STATION_ID"),
        ),
        connector_defaults={},
        normalize=None,  # CAMELS-US is keyed by the exact 8-digit USGS id
    ),
    "camels_se": ProviderBackend(
        slug="camels_se",
        station_keys=(
            _EVAL_STATION_KEY,
            StationKey(lambda cfg: cfg.data.streamflow_station_id, "STREAMFLOW_STATION_ID"),
        ),
        connector_defaults={},
        normalize=None,  # keyed by the SMHI catchment id
    ),
    "camels_fr": ProviderBackend(
        slug="camels_fr",
        station_keys=(
            _EVAL_STATION_KEY,
            StationKey(lambda cfg: cfg.data.streamflow_station_id, "STREAMFLOW_STATION_ID"),
        ),
        connector_defaults={},
        normalize=None,  # keyed by the Hydro3 station code (sta_code_h3)
    ),
    "camels_nz": ProviderBackend(
        slug="camels_nz",
        station_keys=(
            _EVAL_STATION_KEY,
            StationKey(lambda cfg: cfg.data.streamflow_station_id, "STREAMFLOW_STATION_ID"),
        ),
        connector_defaults={},
        normalize=None,  # keyed by the NZ hydrological station id
    ),
    "camels_fi": ProviderBackend(
        slug="camels_fi",
        station_keys=(
            _EVAL_STATION_KEY,
            StationKey(lambda cfg: cfg.data.streamflow_station_id, "STREAMFLOW_STATION_ID"),
        ),
        connector_defaults={},
        normalize=None,  # keyed by the SYKE gauge id (hyphenated for virtual gauges)
    ),
    "camels_lux": ProviderBackend(
        slug="camels_lux",
        station_keys=(
            _EVAL_STATION_KEY,
            StationKey(lambda cfg: cfg.data.streamflow_station_id, "STREAMFLOW_STATION_ID"),
        ),
        connector_defaults={},
        normalize=None,  # keyed by the zero-padded id (ID_01 … ID_56)
    ),
    "camels_dk": ProviderBackend(
        slug="camels_dk",
        station_keys=(
            _EVAL_STATION_KEY,
            StationKey(lambda cfg: cfg.data.streamflow_station_id, "STREAMFLOW_STATION_ID"),
        ),
        connector_defaults={},
        normalize=None,
    ),
}


def resolve_provider_station_id(provider_key: str, raw: Any) -> str:
    """Resolve a native or namespaced station id to the canonical CSFS id.

    The drop-in handlers accept the station id exactly as the native
    SYMFLUENCE handlers do — bare native ids like ``06191500`` / ``05BB001``
    / ``2357`` — plus already-namespaced CSFS ids using either the CSFS
    connector slug (``environment_canada:05BB001``) or the SYMFLUENCE
    provider name (``wsc:05BB001``) as the prefix. Anything namespaced for a
    *different* provider is rejected loudly rather than silently re-routed.

    Returns the canonical ``"<slug>:<native_id>"`` form.
    """
    backend = PROVIDER_BACKENDS[provider_key]
    value = "" if raw is None else str(raw).strip()
    if not value:
        raise ValueError(f"Empty station id for streamflow provider {provider_key!r}")

    prefix, sep, rest = value.partition(":")
    if sep:
        if prefix.strip().lower() not in {provider_key, backend.slug} or not rest.strip():
            raise ValueError(
                f"Station id {value!r} is not a {provider_key.upper()} station id. "
                f"Use the bare native id (e.g. from the agency site) or namespace it "
                f"as '{backend.slug}:<native_id>'. For gauges from other networks, "
                "use the generic handler (ADDITIONAL_OBSERVATIONS: csfs) instead."
            )
        native_id = rest.strip()
    else:
        native_id = value

    if backend.normalize is not None:
        native_id = backend.normalize(native_id)
    return f"{backend.slug}:{native_id}"


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

    #: Set by :class:`CommunityObservationBackend` to serve an explicit
    #: ``ObservationRequest.station_ids`` instead of resolving from config.
    station_ids_override: tuple[str, ...] | None = None

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
        if self.station_ids_override:
            return list(self.station_ids_override)
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
            # round_trip float parsing: re-reading the raw CSV must recover the
            # exact float written, not pandas' default ~1-ULP-lossy parse, so the
            # processed values match the native handler's in-memory result bitwise.
            standardized = standardize_frame(pd.read_csv(csv_file, float_precision="round_trip"))
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
# Drop-in per-provider handlers
# ---------------------------------------------------------------------------


def make_provider_handler(provider_key: str) -> type[CSFSStreamflowHandler]:
    """Build the drop-in handler class for one native SYMFLUENCE provider name.

    The returned class is :class:`CSFSStreamflowHandler` with two overrides:

    * the station id is resolved from the *existing* SYMFLUENCE config keys
      (the same lookups, in the same order, as the native handler for that
      provider — see :data:`PROVIDER_BACKENDS`), then namespaced onto the
      CSFS connector slug via :func:`resolve_provider_station_id`;
    * the backend's fixed connector defaults (SMHI: ``resolution: 15min``)
      are merged under any user-supplied ``CSFS_CONNECTOR_CONFIG``.

    ``acquire()``/``process()`` are inherited unchanged: direct connector
    fetch for the experiment window into the conventional raw dir, then the
    byte-matched processed-CSV transform (tz-naive UTC ``datetime`` index,
    ``discharge_cms`` in m³/s, resample + interpolate identical to native).

    No backend routing happens here. The native handlers register under
    different keys (``usgs_streamflow`` / ``wsc_streamflow`` /
    ``smhi_streamflow``), so registering under the short provider names is
    collision-free, and SYMFLUENCE's registry-first streamflow dispatch only
    routes to these keys when ``DATA_ACCESS`` resolves to ``community`` (or
    the provider is unknown to its legacy branches) — native-mode traffic
    never reaches this class.
    """
    backend = PROVIDER_BACKENDS[provider_key]

    class _ProviderHandler(CSFSStreamflowHandler):
        source_name = f"{provider_key.upper()} (via CSFS)"
        SOURCE_INFO = {
            "source": f"{provider_key.upper()} via CSFS (Community Streamflow Service)",
            "url": "https://github.com/DarriEy/CSFS",
        }
        PROVIDER_KEY = provider_key
        BACKEND = backend

        def _station_id_config(self) -> Any:  # pragma: no cover - symfluence-only config access
            """Native station id from the provider's existing config keys, namespaced."""
            if self.station_ids_override:
                return [
                    resolve_provider_station_id(provider_key, sid)
                    for sid in self.station_ids_override
                ]
            for key in backend.station_keys:
                value = self._get_config_value(
                    lambda key=key: key.accessor(self.config),  # type: ignore[misc]
                    default=None,
                    dict_key=key.dict_key,
                )
                if value in (None, "", "default"):
                    continue
                return resolve_provider_station_id(provider_key, value)
            flat_keys = ", ".join(key.dict_key for key in backend.station_keys)
            raise ValueError(
                f"No station id configured for streamflow provider "
                f"{provider_key.upper()}. Set one of: {flat_keys}."
            )

        def _connector_config(self) -> dict | None:  # pragma: no cover - symfluence-only
            """Backend connector defaults, overridable via CSFS_CONNECTOR_CONFIG."""
            user = super()._connector_config() or {}
            merged = {**backend.connector_defaults, **user}
            return merged or None

    _ProviderHandler.__name__ = f"CSFS{provider_key.upper()}StreamflowHandler"
    _ProviderHandler.__qualname__ = _ProviderHandler.__name__
    return _ProviderHandler


#: Registry key -> drop-in handler class, one per native provider name.
PROVIDER_HANDLERS: dict[str, type[CSFSStreamflowHandler]] = {
    key: make_provider_handler(key) for key in PROVIDER_BACKENDS
}


# ---------------------------------------------------------------------------
# ObservationBackend (SYMFLUENCE acquisition-backend protocol, contract 0.2.0)
# ---------------------------------------------------------------------------

#: The contract version this backend targets. Deliberately hardcoded (not
#: read from the installed framework) so that a contract bump on the
#: SYMFLUENCE side is *detected* as skew by the selection layer instead of
#: silently claimed compatible (pre-1.0, minor bumps are breaking).
#:
#: 0.4.0: declares the source-data license posture (``redistribution`` /
#: ``data_license`` / ``attribution``). 0.5.0: declares the source-kind tier —
#: ``source_kind`` + ``dataset_doi`` / ``dataset_version`` /
#: ``dataset_checksum`` / ``noncommercial`` — so the LamaH-Ice DATASET_ARTIFACT
#: capability is admitted by the framework's provenance gate. These fields exist
#: only on the contract-0.5.0 ObservationCapability, so populating them while
#: targeting an older minor would break against a pre-0.5.0 framework.
TARGET_INTERFACE_VERSION = "0.5.0"


class ObservationCapabilitySpec(NamedTuple):
    """Pure (framework-free) capability facts for one served provider.

    ``redistribution`` is the SOURCE-data posture (contract 0.4.0), as a plain
    string mirroring the framework's ``Redistribution`` enum values
    ("open" | "attribution" | "restricted" | "unknown"); kept framework-free so
    this module still imports without symfluence. ``data_license`` /
    ``attribution`` carry the obligation that must propagate to end users.
    """

    provider_id: str
    kinds: frozenset[str]
    station_id_scheme: str
    parity_grade: str | None
    notes: str
    redistribution: str = "unknown"
    data_license: str = ""
    attribution: str = ""
    # Source-kind tier + provenance (contract 0.5.0). DATASET_ARTIFACT entries
    # are admitted by the framework's provenance gate (DOI + version + checksum
    # + license) instead of the parity gate; PROVIDER_API entries ignore these.
    # ``source_kind`` mirrors the SourceKind enum values ("provider_api" |
    # "dataset_artifact"); ``noncommercial`` flags a CC-BY-NC-style use clause.
    source_kind: str = "provider_api"
    dataset_doi: str = ""
    dataset_version: str = ""
    dataset_checksum: str = ""
    noncommercial: bool = False


#: Providers the community observation backend claims. Parity grades record
#: the validated native-vs-CSFS comparison of the processed streamflow CSV;
#: the generic ``CSFS`` entry is deliberately ungated (parity_grade=None) —
#: SYMFLUENCE's parity-gate policy refuses it unless
#: ``ALLOW_UNGATED_BACKENDS: true``, falling through to the handler tier.
OBSERVATION_CAPABILITIES: tuple[ObservationCapabilitySpec, ...] = (
    ObservationCapabilitySpec(
        provider_id="USGS",
        kinds=frozenset({"streamflow"}),
        station_id_scheme="USGS site number (zero-padded to 8 digits; 'usgs:<id>' also accepted)",
        parity_grade="bit-identical",
        notes="USGS NWIS via the CSFS usgs connector; processed CSV bit-identical "
              "to the native handler per the parity work.",
        # USGS/NWIS data are produced by a US federal agency and are public
        # domain (17 U.S.C. §105) — free to mirror; courtesy citation only.
        redistribution="open",
        data_license="public-domain",
        attribution="U.S. Geological Survey, National Water Information System (NWIS)",
    ),
    ObservationCapabilitySpec(
        provider_id="WSC",
        kinds=frozenset({"streamflow"}),
        station_id_scheme="WSC station number (e.g. 05BB001; 'environment_canada:<id>' also accepted)",
        parity_grade="value-identical:float-repr",
        notes="Environment Canada real-time/historical via CSFS; values identical, "
              "byte differences limited to float representation.",
        # Environment and Climate Change Canada data: Open Government Licence –
        # Canada. Redistributable with attribution.
        redistribution="attribution",
        data_license="OGL-Canada-2.0",
        attribution="Environment and Climate Change Canada, Water Survey of Canada",
    ),
    ObservationCapabilitySpec(
        provider_id="SMHI",
        kinds=frozenset({"streamflow"}),
        station_id_scheme="SMHI hydroobs station number (e.g. 2357; 'sweden_smhi:<id>' also accepted)",
        parity_grade="value-identical:rounding",
        notes="SMHI 15-minute discharge (hydroobs parameter 2) pinned to match the "
              "native handler; values identical up to provider rounding.",
        # SMHI open data: Creative Commons Attribution 4.0. Redistributable
        # with attribution.
        redistribution="attribution",
        data_license="CC-BY-4.0",
        attribution="Swedish Meteorological and Hydrological Institute (SMHI)",
    ),
    ObservationCapabilitySpec(
        provider_id="LAMAH_ICE",
        kinds=frozenset({"streamflow"}),
        station_id_scheme="LamaH-Ice gauge id (e.g. 1; 'iceland_lamahice:<id>' also accepted)",
        parity_grade=None,  # dataset artifact: admitted by the provenance gate, not parity
        notes="LamaH-Ice daily discharge from the published HydroShare archive via the "
              "CSFS iceland_lamahice connector. A static, DOI-pinned dataset artifact — "
              "admitted by the framework's provenance gate (DOI + version + checksum), "
              "not the parity gate. The archive is checksum-verified on download.",
        # Streamflow is CC-BY-NC-4.0 (attributes are CC-BY-4.0). Redistributable
        # WITH attribution, but NOT for commercial use — hence noncommercial=True.
        redistribution="attribution",
        data_license="CC-BY-NC-4.0",
        attribution="Helgason & Nijssen (2024), LamaH-Ice (HydroShare); "
                    "streamflow data CC-BY-NC-4.0",
        source_kind="dataset_artifact",
        dataset_doi="10.4211/hs.86117a5f36cc4b7c90a5d54e18161c91",
        dataset_version="daily; HydroShare snapshot 2024-05-30",
        dataset_checksum="md5:6246f7300c77ead2c9f097ad5da89ba9",
        noncommercial=True,
    ),
    ObservationCapabilitySpec(
        provider_id="LAMAH_CE",
        kinds=frozenset({"streamflow"}),
        station_id_scheme="LamaH-CE gauge id (e.g. 1; 'lamah_ce:<id>' also accepted)",
        parity_grade=None,  # dataset artifact: provenance-gated, not parity
        notes="LamaH-CE daily discharge from the published Zenodo archive via the CSFS "
              "lamah_ce connector. A static, DOI-pinned dataset artifact — admitted by "
              "the provenance gate (DOI + version + checksum), archive checksum-verified "
              "on download.",
        # LamaH-CE is CC-BY-4.0 throughout (no NonCommercial clause).
        redistribution="attribution",
        data_license="CC-BY-4.0",
        attribution="Klingler, Schulz & Herrnegger (2021), LamaH-CE (Zenodo)",
        source_kind="dataset_artifact",
        dataset_doi="10.5281/zenodo.5153305",
        dataset_version="1.0; daily",
        dataset_checksum="md5:69fd2733e969513403f923ecc5eaa3dc",
        noncommercial=False,
    ),
    ObservationCapabilitySpec(
        provider_id="CAMELS_BR",
        kinds=frozenset({"streamflow"}),
        station_id_scheme="ANA gauge code (e.g. 10100000; 'camels_br:<id>' also accepted)",
        parity_grade=None,  # dataset artifact: provenance-gated, not parity
        notes="CAMELS-BR daily discharge from the published Zenodo archive via the CSFS "
              "camels_br connector. A static, DOI-pinned dataset artifact — admitted by "
              "the provenance gate; the streamflow archive is checksum-verified on "
              "download (the attributes archive supplies gauge coordinates).",
        # CAMELS-BR is CC-BY-4.0 throughout.
        redistribution="attribution",
        data_license="CC-BY-4.0",
        attribution="Chagas et al. (2020), CAMELS-BR (Zenodo)",
        source_kind="dataset_artifact",
        dataset_doi="10.5281/zenodo.3964745",
        dataset_version="1.1; daily",
        dataset_checksum="md5:599b96f48ec78e25751cf1cc691a22bb",
        noncommercial=False,
    ),
    ObservationCapabilitySpec(
        provider_id="CAMELS_DE",
        kinds=frozenset({"streamflow"}),
        station_id_scheme="CAMELS-DE gauge id (e.g. DE210480; 'camels_de:<id>' also accepted)",
        parity_grade=None,  # dataset artifact: provenance-gated, not parity
        notes="CAMELS-DE daily observed discharge (discharge_vol_obs) from the published "
              "Zenodo archive via the CSFS camels_de connector. A static, DOI-pinned "
              "dataset artifact — admitted by the provenance gate; the bundle is "
              "checksum-verified on download.",
        # CAMELS-DE is CC-BY-4.0 throughout.
        redistribution="attribution",
        data_license="CC-BY-4.0",
        attribution="Loritz et al. (2024), CAMELS-DE (Zenodo)",
        source_kind="dataset_artifact",
        dataset_doi="10.5281/zenodo.16755906",
        dataset_version="1.1.0; daily",
        dataset_checksum="md5:5ee2f89f6204e8eafdbc11b491d34afb",
        noncommercial=False,
    ),
    ObservationCapabilitySpec(
        provider_id="CAMELS_CL",
        kinds=frozenset({"streamflow"}),
        station_id_scheme="DGA gauge code (e.g. 1001001; 'camels_cl:<id>' also accepted)",
        parity_grade=None,  # dataset artifact: provenance-gated, not parity
        notes="CAMELS-CL daily discharge from the published PANGAEA archive via the CSFS "
              "camels_cl connector. A static, DOI-pinned dataset artifact served from the "
              "authoritative store.pangaea.de zips — admitted by the provenance gate; the "
              "streamflow matrix is checksum-verified on download.",
        # CAMELS-CL (PANGAEA) is CC-BY.
        redistribution="attribution",
        data_license="CC-BY-4.0",
        attribution="Alvarez-Garreton et al. (2018), CAMELS-CL (PANGAEA)",
        source_kind="dataset_artifact",
        dataset_doi="10.1594/PANGAEA.894885",
        dataset_version="2018; daily",
        dataset_checksum="md5:3457bc87e444e1e7d84a1b703965708d",
        noncommercial=False,
    ),
    ObservationCapabilitySpec(
        provider_id="CAMELS_IND",
        kinds=frozenset({"streamflow"}),
        station_id_scheme="CWC gauge code (e.g. 3002; 'camels_ind:<id>' also accepted)",
        parity_grade=None,
        notes="CAMELS-IND v2.2 daily observed discharge (wide-matrix streamflow_observed.csv) "
              "from the published Zenodo archive via the CSFS camels_ind connector — a "
              "DOI-pinned dataset artifact, checksum-verified on download. Authoritative "
              "standalone (distinct from the Caravan-derived camels_in alias).",
        redistribution="attribution",
        data_license="CC-BY-4.0",
        attribution="Mangukiya et al. (2025), CAMELS-IND (Zenodo)",
        source_kind="dataset_artifact",
        dataset_doi="10.5281/zenodo.14999580",
        dataset_version="2.2; daily",
        dataset_checksum="md5:3993c25ba7d7b86df0541de91e094f39",
        noncommercial=False,
    ),
    ObservationCapabilitySpec(
        provider_id="CAMELS_CH",
        kinds=frozenset({"streamflow"}),
        station_id_scheme="BAFU gauge id (e.g. 2004; 'camels_ch:<id>' also accepted)",
        parity_grade=None,
        notes="CAMELS-CH daily observation-based discharge (discharge_vol(m3/s)) from the "
              "published Zenodo archive via the CSFS camels_ch connector — a DOI-pinned "
              "dataset artifact, checksum-verified on download.",
        redistribution="attribution",
        data_license="CC-BY-4.0",
        attribution="Höge et al. (2023), CAMELS-CH (Zenodo)",
        source_kind="dataset_artifact",
        dataset_doi="10.5281/zenodo.15025258",
        dataset_version="daily",
        dataset_checksum="md5:04f909d9904375647d030c4ab8ddfdbe",
        noncommercial=False,
    ),
    ObservationCapabilitySpec(
        provider_id="CAMELS_DK",
        kinds=frozenset({"streamflow"}),
        station_id_scheme="CAMELS-DK catchment id (e.g. 12410011; 'camels_dk:<id>' also accepted)",
        parity_grade=None,
        notes="CAMELS-DK daily observed discharge (Qobs) from the published GEUS Dataverse "
              "archive via the CSFS camels_dk connector — a DOI-pinned dataset artifact, "
              "checksum-verified on download; outlet coords (EPSG:25832) reprojected to WGS84.",
        # CAMELS-DK is CC0 1.0 (public-domain dedication) — no restriction.
        redistribution="open",
        data_license="CC0-1.0",
        attribution="Liu et al. (2024), CAMELS-DK (GEUS Dataverse)",
        source_kind="dataset_artifact",
        dataset_doi="10.22008/FK2/AZXSYP",
        dataset_version="daily",
        dataset_checksum="md5:50b6d3957e6abf0017973ac872aea67f",
        noncommercial=False,
    ),
    ObservationCapabilitySpec(
        provider_id="CAMELS_US",
        kinds=frozenset({"streamflow"}),
        station_id_scheme="8-digit USGS gauge id (e.g. 01013500; 'camels_us:<id>' also accepted)",
        parity_grade=None,
        notes="CAMELS-US daily USGS discharge (cfs converted to m3/s) from the published "
              "bundle via the CSFS camels_us connector — a DOI-pinned dataset artifact, "
              "checksum-verified on download; coords from the bundled gauge_information.txt.",
        redistribution="attribution",
        data_license="CC-BY-4.0",
        attribution="Newman et al. (2015) / Addor et al. (2017), CAMELS-US (NCAR/UCAR)",
        source_kind="dataset_artifact",
        dataset_doi="10.5065/D6MW2F4D",
        dataset_version="1.2; daily",
        dataset_checksum="md5:8e9a466710e8270b58f01d332a87184f",
        noncommercial=False,
    ),
    ObservationCapabilitySpec(
        provider_id="CAMELS_GB",
        kinds=frozenset({"streamflow"}),
        station_id_scheme="NRFA gauge id (e.g. 41004; 'camels_gb:<id>' also accepted)",
        parity_grade=None,
        notes="CAMELS-GB daily observed discharge (discharge_vol, m3/s) from the published "
              "CEH archive via the CSFS camels_gb connector — a dataset artifact. CEH "
              "regenerates the zip per request, so integrity is a CONTENT checksum over the "
              "extracted data (verified on download).",
        # CAMELS-GB is Open Government Licence (UK), redistributable with attribution.
        redistribution="attribution",
        data_license="OGL-UK-3.0",
        attribution="Coxon et al. (2020), CAMELS-GB (NERC EDS / CEH EIDC)",
        source_kind="dataset_artifact",
        dataset_doi="10.5285/8344e4f3-d2ea-44f5-8afa-86d2987543a9",
        dataset_version="daily",
        dataset_checksum="content-sha256:de33e2731d7285423801db723acbd0c8d97c1505b3d184830032c755a341742c",
        noncommercial=False,
    ),
    ObservationCapabilitySpec(
        provider_id="CAMELS_NZ",
        kinds=frozenset({"streamflow"}),
        station_id_scheme="NZ hydrological station id (e.g. 29605; 'camels_nz:<id>' also accepted)",
        parity_grade=None,
        notes="CAMELS-NZ daily observed streamflow (flow, m3/s) from the published University "
              "of Canterbury (figshare) archive via the CSFS camels_nz connector — a dataset "
              "artifact, checksum-verified on download (gauge coordinates already WGS84). 14 of "
              "369 stations are permission-gated by the data owner and ship empty (all-NA) files.",
        redistribution="attribution",
        data_license="CC-BY-4.0",
        attribution="Bushra et al. (2025), CAMELS-NZ (University of Canterbury)",
        source_kind="dataset_artifact",
        dataset_doi="10.26021/canterburynz.28827644",
        dataset_version="daily",
        dataset_checksum="md5:089757d4b019487fefd8f20d7099403d",
        noncommercial=False,
    ),
    ObservationCapabilitySpec(
        provider_id="CAMELS_FI",
        kinds=frozenset({"streamflow"}),
        station_id_scheme="SYKE gauge id (e.g. 896; hyphenated for virtual gauges; 'camels_fi:<id>' also accepted)",
        parity_grade=None,
        notes="CAMELS-FI daily observed discharge (discharge_vol, m3/s) from the published "
              "Zenodo archive via the CSFS camels_fi connector — a dataset artifact, checksum-"
              "verified on download. Gauge coordinates reprojected from EPSG:3067 (the lat/lon "
              "columns have a documented description swap). ESSD preprint under review.",
        redistribution="attribution",
        data_license="CC-BY-4.0",
        attribution="Seppä et al. (2025), CAMELS-FI (Zenodo)",
        source_kind="dataset_artifact",
        dataset_doi="10.5281/zenodo.15853357",
        dataset_version="1.2.0; daily",
        dataset_checksum="md5:f50bf2d972f42b6fc4db690ce201482f",
        noncommercial=False,
    ),
    ObservationCapabilitySpec(
        provider_id="CAMELS_LUX",
        kinds=frozenset({"streamflow"}),
        station_id_scheme="zero-padded id (e.g. ID_01; 'camels_lux:<id>' also accepted)",
        parity_grade=None,
        notes="CAMELS-LUX daily streamflow (Q, m3/s) from the published Zenodo archive via the "
              "CSFS camels_lux connector — a dataset artifact, checksum-verified on download. "
              "Gap-filled values (Qflag != 0) are surfaced with an 'estimated' quality flag; "
              "gauge coordinates from the bundled WGS84 shapefile. ESSD preprint under review.",
        redistribution="attribution",
        data_license="CC-BY-4.0",
        attribution="Nijzink et al. (2025), CAMELS-LUX (Zenodo)",
        source_kind="dataset_artifact",
        dataset_doi="10.5281/zenodo.13846619",
        dataset_version="2.1; daily",
        dataset_checksum="md5:6c4a14a0feed08382a6b565a798d8fdc",
        noncommercial=False,
    ),
    ObservationCapabilitySpec(
        provider_id="CAMELS_FR",
        kinds=frozenset({"streamflow"}),
        station_id_scheme="Hydro3 station code (e.g. A105003001; 'camels_fr:<code>' also accepted)",
        parity_grade=None,
        notes="CAMELS-FR daily observed streamflow (tsd_q_l, L/s converted to m3/s on read) "
              "from the published Recherche Data Gouv archive via the CSFS camels_fr "
              "connector — a dataset artifact, checksum-verified on download. Gauge outlet "
              "coordinates from the bundled GeoPackage (EPSG:27572 reprojected to WGS84).",
        redistribution="attribution",
        data_license="CC-BY-4.0",
        attribution="Delaigue et al. (2024), CAMELS-FR (INRAE / Recherche Data Gouv)",
        source_kind="dataset_artifact",
        dataset_doi="10.57745/WH7FJR",
        dataset_version="3.2; daily",
        dataset_checksum="md5:dd48efe7cca89e86d8435a9888ebcdca",
        noncommercial=False,
    ),
    ObservationCapabilitySpec(
        provider_id="CAMELS_SE",
        kinds=frozenset({"streamflow"}),
        station_id_scheme="SMHI catchment id (e.g. 1069; 'camels_se:<id>' also accepted)",
        parity_grade=None,
        notes="CAMELS-SE daily observed discharge (Qobs_m3s) from the published SND archive "
              "via the CSFS camels_se connector — a dataset artifact, checksum-verified on "
              "download (gauge coordinates from the bundled WGS84 station shapefile). SND "
              "publishes no archive checksum; the pinned md5 is self-computed.",
        redistribution="attribution",
        data_license="CC-BY-4.0",
        attribution="Teutschbein et al. (2024), CAMELS-SE (SND 2023-173)",
        source_kind="dataset_artifact",
        dataset_doi="10.57804/t3rm-v029",
        dataset_version="v1; daily",
        dataset_checksum="md5:5e6972cf29c9220e547bc00dddd7b03a",
        noncommercial=False,
    ),
    ObservationCapabilitySpec(
        provider_id="CAMELS_AUS",
        kinds=frozenset({"streamflow"}),
        station_id_scheme="AWRC station id (e.g. 912101A; 'camels_aus:<id>' also accepted)",
        parity_grade=None,
        notes="CAMELS-AUS v2 daily streamflow (wide matrix, ML/day converted to m3/s on "
              "read) from the published Zenodo archive via the CSFS camels_aus connector — "
              "a dataset artifact, checksum-verified on download (outlet coordinates from "
              "the bare attributes master-table CSV).",
        redistribution="attribution",
        data_license="CC-BY-4.0",
        attribution="Fowler et al. (2024), CAMELS-AUS v2 (Zenodo)",
        source_kind="dataset_artifact",
        dataset_doi="10.5281/zenodo.13350616",
        dataset_version="2.02; daily",
        dataset_checksum="md5:28113b991387796fe374aa0d1f4d4a4f",
        noncommercial=False,
    ),
    ObservationCapabilitySpec(
        provider_id="CSFS",
        kinds=frozenset({"streamflow"}),
        station_id_scheme="namespaced CSFS id '<provider>:<native_id>' (run 'csfs providers')",
        parity_grade=None,
        notes="Ungated generic access to every CSFS provider connector; refused by "
              "the parity gate unless ALLOW_UNGATED_BACKENDS: true (the registry-"
              "handler tier and ADDITIONAL_OBSERVATIONS: csfs remain available).",
        # Generic entry spans 80+ providers with heterogeneous terms (some
        # restricted, e.g. GRDC), so the posture cannot be asserted per-provider
        # here. Left 'unknown' — already refused by the parity gate
        # (parity_grade=None); a future per-provider capability split can
        # declare each source's true posture.
        redistribution="unknown",
        data_license="",
        attribution="",
    ),
)


def _backend_contract() -> Any:  # pragma: no cover - symfluence-only import
    from symfluence.data.backends import contract

    return contract


def _backend_errors() -> Any:  # pragma: no cover - symfluence-only import
    from symfluence.data.backends import errors

    return errors


class CommunityObservationBackend:
    """CSFS exposed through SYMFLUENCE's ObservationBackend protocol (0.2.0).

    A thin wrapper over the handler classes above (factored, not duplicated):
    ``acquire()`` runs the existing fetch (``handler.acquire()`` — skip-if-
    exists raw CSVs, store or live) and the existing byte-matched processing
    (``handler.process()`` — the parity-gated ``{domain}_streamflow_processed
    .csv``), then derives the *protocol delivery*: one OBS_CSV_V1 file per
    station plus the sidecar ``acquisition_manifest.json``.

    Window semantics: the OBS_CSV_V1 delivery is trimmed to the contract's
    half-open UTC ``[start, end)`` window. The processed CSV deliberately
    reproduces the legacy pipeline byte-for-byte — including its historical
    inclusive-end slice — so the parity grades above keep holding; the
    boundary bin can therefore appear in the processed artifact but never in
    the protocol delivery.

    Instantiated by SYMFLUENCE's selection layer with ``(config, logger)``,
    exactly like the framework's NativeBackend (handlers are config-driven).
    """

    name = "community"
    interface_version = TARGET_INTERFACE_VERSION

    def __init__(self, config: Any = None, logger: Any = None) -> None:
        self.config = config
        self.logger = logger or _integration_logger()

    # -- protocol surface ---------------------------------------------------

    def capabilities(self) -> tuple[Any, ...]:  # pragma: no cover - symfluence-only
        """Observation providers servable, as contract ObservationCapability."""
        contract = _backend_contract()
        return tuple(
            contract.ObservationCapability(
                provider_id=spec.provider_id,
                kinds=spec.kinds,
                station_id_scheme=spec.station_id_scheme,
                temporal=None,
                auth=frozenset(),
                parity_grade=spec.parity_grade,
                notes=spec.notes,
                data_license=spec.data_license,
                attribution=spec.attribution,
                redistribution=contract.Redistribution(spec.redistribution),
                source_kind=contract.SourceKind(spec.source_kind),
                dataset_doi=spec.dataset_doi,
                dataset_version=spec.dataset_version,
                dataset_checksum=spec.dataset_checksum,
                noncommercial=spec.noncommercial,
            )
            for spec in OBSERVATION_CAPABILITIES
        )

    def acquire(self, request: Any) -> Any:  # pragma: no cover - exercised by integration tests
        """Serve an ``ObservationRequest`` via the existing handler internals."""
        contract = _backend_contract()
        errors = _backend_errors()

        provider_key = str(request.provider_id).strip().lower()
        if provider_key == "csfs":
            handler_cls: type[CSFSStreamflowHandler] = CSFSStreamflowHandler
        else:
            maybe = PROVIDER_HANDLERS.get(provider_key)
            if maybe is None:
                served = sorted(spec.provider_id for spec in OBSERVATION_CAPABILITIES)
                raise errors.DatasetUnsupported(
                    f"The community observation backend does not serve provider "
                    f"'{request.provider_id}' (served: {served})",
                    dataset_id=request.provider_id,
                    backend=self.name,
                )
            handler_cls = maybe
        if request.kind != "streamflow":
            raise errors.DatasetUnsupported(
                f"The community observation backend serves kind 'streamflow', "
                f"not {request.kind!r}",
                dataset_id=request.provider_id,
                backend=self.name,
            )

        if self.config is None:
            raise errors.AcquisitionError(
                "CommunityObservationBackend.acquire() requires a framework config "
                "(the CSFS handlers are configuration-driven)"
            )

        handler = handler_cls(self.config, self.logger)
        if request.station_ids:
            handler.station_ids_override = tuple(request.station_ids)

        from csfs.core.exceptions import ConnectorError

        try:
            raw = handler.acquire()
            processed = handler.process(raw)
        except ConnectorError as exc:
            raise errors.UpstreamOutage(
                f"CSFS connector failure while acquiring '{request.provider_id}' "
                f"observations: {exc}",
                upstream=provider_key,
            ) from exc
        except FileNotFoundError as exc:
            raise errors.AcquisitionError(
                f"CSFS observation acquisition for '{request.provider_id}' failed: {exc}"
            ) from exc
        except (ValueError, KeyError, TypeError, RuntimeError, OSError) as exc:
            raise errors.AcquisitionError(
                f"CSFS observation acquisition for '{request.provider_id}' failed: {exc}"
            ) from exc

        # Protocol delivery: OBS_CSV_V1 per station, derived from the raw
        # CSVs the handler wrote (window-trimmed to UTC [start, end)).
        _require_pandas()
        import pandas as pd

        target_dir = Path(request.target_dir)
        target_dir.mkdir(parents=True, exist_ok=True)
        raw_path = Path(raw)
        raw_files = [raw_path] if raw_path.is_file() else sorted(raw_path.glob("csfs_*_raw.csv"))
        if not raw_files:
            raise errors.IntegrityError(
                f"CSFS handler reported success but delivered no raw station files "
                f"under {raw_path}"
            )

        start = request.window[0] if request.window else None
        end = request.window[1] if request.window else None
        paths: list[Path] = []
        for raw_file in raw_files:
            # round_trip float parsing (see process()): exact recovery of the
            # written floats keeps the OBS_CSV_V1 delivery bitwise-faithful.
            obs = obs_csv_v1_frame(pd.read_csv(raw_file, float_precision="round_trip"), start=start, end=end)
            out = target_dir / f"{raw_file.stem.removesuffix('_raw')}_obs_v1.csv"
            obs.to_csv(out, index=False)
            paths.append(out)

        import csfs

        # Propagate the SOURCE-data license posture into the delivery so the
        # obligation survives into the manifest and downstream provenance.
        spec = next(
            (s for s in OBSERVATION_CAPABILITIES
             if s.provider_id.lower() == str(request.provider_id).lower()),
            None,
        )
        provenance = {
            "integration": f"{__name__}.CommunityObservationBackend",
            "csfs_version": getattr(csfs, "__version__", "unknown"),
            "provider_id": str(request.provider_id),
            "stations": ",".join(request.station_ids),
            "processed_path": str(processed),
            "acquired_at": datetime.now(UTC).isoformat(),
        }
        # Dataset-artifact provenance: record the DOI/version/verified checksum
        # in the manifest so the delivery is traceable to the published archive.
        if spec and spec.source_kind == "dataset_artifact":
            provenance.update(
                source_kind=spec.source_kind,
                dataset_doi=spec.dataset_doi,
                dataset_version=spec.dataset_version,
                dataset_checksum=spec.dataset_checksum,
                noncommercial=str(spec.noncommercial).lower(),
            )
        result = contract.AcquisitionResult(
            paths=tuple(paths),
            schema=contract.SchemaId.OBS_CSV_V1,
            dataset_id=request.provider_id,
            backend=self.name,
            provenance=provenance,
            variables_delivered=frozenset({"streamflow"}),
            data_license=spec.data_license if spec else "",
            attribution=spec.attribution if spec else "",
            redistribution=contract.Redistribution(spec.redistribution if spec else "unknown"),
        )
        contract.write_manifest(result, target_dir)
        return result


def _integration_logger() -> Any:
    import logging

    return logging.getLogger("csfs.integrations.symfluence")


# ---------------------------------------------------------------------------
# Plugin entry point
# ---------------------------------------------------------------------------


def register() -> None:
    """Register the CSFS observation tiers with SYMFLUENCE (idempotent).

    Zero-arg hook referenced by the ``symfluence.plugins`` entry point;
    SYMFLUENCE's bootstrap calls it automatically on ``import symfluence``.
    After registration, layered as in the module docstring:

    * :class:`CommunityObservationBackend` joins ``R.observation_backends``
      (the protocol tier, consulted FIRST under ``DATA_ACCESS: community``;
      skipped when the installed symfluence predates contract 0.2.0 — the
      handler tier then still serves community mode);
    * ``ADDITIONAL_OBSERVATIONS: csfs`` dispatches to
      :class:`CSFSStreamflowHandler` (any CSFS provider, namespaced ids);
    * the drop-in keys ``usgs`` / ``wsc`` / ``smhi`` stay registered for
      SYMFLUENCE's registry-first primary-streamflow dispatch — the
      fallthrough tier below the backend, now redundant under community
      mode but harmless and required for ADDITIONAL_OBSERVATIONS and for
      frameworks without the backend registry. The native handlers live
      under ``usgs_streamflow``-style keys, so these registrations are
      additive and inert until community mode is enabled.
    """
    if not HAVE_SYMFLUENCE:
        raise ImportError(
            "Cannot register the CSFS plugin: symfluence is not importable in this environment."
        )
    from symfluence.core.registries import R  # pragma: no cover - symfluence-only

    if "csfs" not in R.observation_handlers:  # pragma: no cover - symfluence-only
        R.observation_handlers.add("csfs", CSFSStreamflowHandler)
    for key, handler_cls in PROVIDER_HANDLERS.items():  # pragma: no cover - symfluence-only
        if key not in R.observation_handlers:
            R.observation_handlers.add(key, handler_cls)

    # Protocol tier (contract 0.2.0). Registered as a CLASS: SYMFLUENCE's
    # selection layer instantiates it with (config, logger). Older
    # frameworks without the registry simply skip this tier.
    backends = getattr(R, "observation_backends", None)  # pragma: no cover - symfluence-only
    if backends is not None and "community" not in backends:  # pragma: no cover - symfluence-only
        backends.add("community", CommunityObservationBackend)


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
