"""Iceland LamaH-Ice connector — local large-sample hydrology dataset.

LamaH-Ice (LArge-SaMple DAta for Hydrology and Environmental Sciences for
Iceland) provides daily hydro-meteorological time series — including observed
streamflow — and catchment attributes for 107 Icelandic river basins. It is
published on CUAHSI HydroShare
(DOI 10.4211/hs.86117a5f36cc4b7c90a5d54e18161c91).

There is **no public real-time discharge API** from the Icelandic
Meteorological Office, so this connector is local-file based, mirroring
``lamah_ce``:

* **Stations** are read from the dataset's gauge-attributes table (real
  gauge IDs and coordinates) — not a hand-maintained seed list.
* **Observations** are read from the per-gauge daily discharge CSVs
  (``ID_<id>.csv`` under a ``daily`` timeseries folder).

The daily archive (``lamah_ice.zip``, ~636 MB) is auto-downloaded and cached
on first use via :func:`csfs.core.downloads.ensure_dataset`. Set
``config['data_dir']`` to point at a pre-downloaded copy, or
``config['auto_download'] = False`` to disable the download.
"""

from __future__ import annotations

import csv
import io
from datetime import UTC, datetime
from functools import lru_cache
from pathlib import Path

import structlog
from pyproj import Transformer

from csfs.connectors.base import BaseConnector
from csfs.core.downloads import ensure_dataset
from csfs.core.exceptions import ConnectorError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

_HYDROSHARE_URL = (
    "https://www.hydroshare.org/resource/"
    "86117a5f36cc4b7c90a5d54e18161c91/"
)

# LamaH-Ice gauge coordinates are stored in ISN93 / Lambert (EPSG:3057, metres),
# not WGS84 — the ``lat`` column is the northing and ``lon`` the easting.
_ISN93_EPSG = 3057

# Column-name fragments used to locate the discharge value / date columns in
# the (semicolon-delimited) timeseries CSVs. Matched case-insensitively.
_QOBS_HINTS = ("qobs", "discharge", "streamflow", "flow", "q_")
_DATE_HINTS = ("date", "datum", "yyyy-mm-dd", "time")


@lru_cache(maxsize=1)
def _isn93_to_wgs84() -> Transformer:
    """Cached EPSG:3057 (easting, northing) -> WGS84 (lon, lat) transformer."""
    return Transformer.from_crs(_ISN93_EPSG, 4326, always_xy=True)


@register("iceland_lamahice")
class IcelandLamahIceConnector(BaseConnector):
    """Connector for LamaH-Ice (Iceland) — local file-based.

    Configuration options (via ``config`` dict):
        data_dir : str | Path
            Directory containing a pre-downloaded LamaH-Ice dataset. If unset,
            the dataset is auto-downloaded to a managed cache.
        auto_download : bool
            If True (default), download the dataset on first use.
        datasets_dir : str | Path
            Base directory for auto-downloaded datasets (default
            ``data/datasets``).
    """

    slug = "iceland_lamahice"
    display_name = "LamaH-Ice (Iceland)"
    base_url = _HYDROSHARE_URL
    country_codes = ["IS"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return Icelandic gauging stations from the LamaH-Ice attributes table.

        Triggers the dataset download (cached) if needed, then parses the
        gauge-attributes CSV for real IDs and coordinates. Returns an empty
        list if the dataset is unavailable (no fabricated stations).
        """
        data_dir = await ensure_dataset(self.slug, self.config)
        if data_dir is None:
            logger.info(
                "iceland_lamahice_no_data",
                hint=(
                    "LamaH-Ice data unavailable (auto-download disabled or "
                    f"failed). Download from {_HYDROSHARE_URL}"
                ),
            )
            return []

        attr_file = self._find_attributes_file(Path(data_dir))
        if attr_file is None:
            logger.warning(
                "iceland_lamahice_attributes_not_found",
                data_dir=str(data_dir),
            )
            return []

        stations = self._parse_attributes(attr_file)
        logger.info(
            "stations_fetched",
            provider=self.slug,
            count=len(stations),
            source="lamah_ice_attributes",
        )
        return stations

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Read daily discharge observations from the LamaH-Ice CSV for a gauge."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        data_dir = await ensure_dataset(self.slug, self.config)

        if data_dir is None:
            logger.info(
                "iceland_lamahice_no_data",
                station=native_id,
                hint=(
                    "LamaH-Ice data unavailable (auto-download disabled or "
                    f"failed). Download from {_HYDROSHARE_URL}"
                ),
            )
            return self._empty_chunk(station_id)

        file_path = self._find_data_file(Path(data_dir), native_id)
        if file_path is None:
            logger.info(
                "iceland_lamahice_file_not_found",
                station=native_id,
                data_dir=str(data_dir),
            )
            return self._empty_chunk(station_id)

        start_aware = start if start.tzinfo else start.replace(tzinfo=UTC)
        end_aware = end if end.tzinfo else end.replace(tzinfo=UTC)

        observations = self._parse_timeseries_csv(
            file_path, station_id, start_aware, end_aware,
        )
        logger.info(
            "iceland_lamahice_observations_loaded",
            station=native_id,
            count=len(observations),
            file=str(file_path),
        )
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # ------------------------------------------------------------------
    # File discovery
    # ------------------------------------------------------------------

    def _find_attributes_file(self, data_dir: Path) -> Path | None:
        """Locate the gauge-attributes CSV (``D_gauges/.../Gauge_attributes.csv``)."""
        gauge_attr = [
            p for p in data_dir.rglob("Gauge_attributes.csv") if p.is_file()
        ]
        if gauge_attr:
            return gauge_attr[0]
        # Fallback: any *attributes*.csv (excluding derived hydro-index tables).
        matches = [
            p for p in data_dir.rglob("*ttributes*.csv")
            if p.is_file() and "indices" not in p.name.lower()
        ]
        return matches[0] if matches else None

    def _find_data_file(self, data_dir: Path, gauge_id: str) -> Path | None:
        """Locate the daily *discharge* CSV for a gauge.

        LamaH-Ice has ``ID_<id>.csv`` files in many timeseries folders (land
        cover, meteorology, snow cover, ...). The observed discharge lives in
        ``D_gauges/2_timeseries/daily/ID_<id>.csv``, so matches are ranked to
        prefer that path and avoid the look-alikes.
        """
        matches = [
            p for p in data_dir.rglob(f"ID_{gauge_id}.csv") if p.is_file()
        ]
        if not matches:
            return None

        def score(p: Path) -> int:
            parts = [x.lower() for x in p.parts]
            s = 0
            if "d_gauges" in parts:
                s += 4
            if p.parent.name.lower() == "daily":
                s += 2
            elif p.parent.name.lower() == "daily_filtered":
                s += 1
            return s

        best = max(matches, key=score)
        return best if score(best) > 0 else None

    # ------------------------------------------------------------------
    # Parsing
    # ------------------------------------------------------------------

    def _parse_attributes(self, file_path: Path) -> list[Station]:
        """Parse ``Gauge_attributes.csv`` into Station objects.

        Columns: ``id;V_no;name;river;...;elevation;lat;lon;geometry`` where
        ``lat``/``lon`` are ISN93 northing/easting (metres) — converted to
        WGS84 here.
        """
        rows, fields = self._read_delimited(file_path)
        lower = {f.lower(): f for f in fields}
        id_col = lower.get("id")
        lat_col = lower.get("lat")  # northing (EPSG:3057)
        lon_col = lower.get("lon")  # easting (EPSG:3057)
        name_col = lower.get("name")
        river_col = lower.get("river")
        elev_col = lower.get("elevation")

        if not (id_col and lat_col and lon_col):
            logger.warning(
                "iceland_lamahice_attribute_columns_missing", fields=fields,
            )
            return []

        transformer = _isn93_to_wgs84()
        stations: list[Station] = []
        for row in rows:
            native_id = (row.get(id_col) or "").strip()
            northing = _safe_float(row.get(lat_col))
            easting = _safe_float(row.get(lon_col))
            if not native_id or northing is None or easting is None:
                continue
            lon_deg, lat_deg = transformer.transform(easting, northing)
            stations.append(Station(
                id=self._station_id(native_id),
                provider=self.slug,
                native_id=native_id,
                name=((row.get(name_col) or "").strip() if name_col else "")
                or f"LamaH-Ice gauge {native_id}",
                latitude=lat_deg,
                longitude=lon_deg,
                country_code="IS",
                river=((row.get(river_col) or "").strip() or None) if river_col else None,
                elevation_m=_safe_float(row.get(elev_col)) if elev_col else None,
            ))
        return stations

    def _parse_timeseries_csv(
        self,
        file_path: Path,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> list[Observation]:
        """Parse a LamaH-Ice daily discharge CSV into Observations.

        Handles both a single ``date`` column and separate ``YYYY``/``MM``/
        ``DD`` columns, with the discharge value under a qobs/discharge-style
        column.
        """
        rows, fields = self._read_delimited(file_path)
        if not fields:
            return []

        lower = {f.lower(): f for f in fields}
        qobs_col = self._match_column(fields, _QOBS_HINTS)
        date_col = self._match_column(fields, _DATE_HINTS)
        y_col = lower.get("yyyy") or lower.get("year")
        m_col = lower.get("mm") or lower.get("month")
        d_col = lower.get("dd") or lower.get("day")

        if qobs_col is None:
            logger.warning(
                "iceland_lamahice_qobs_column_missing", fields=fields,
            )
            return []

        observations: list[Observation] = []
        for row in rows:
            ts = self._row_timestamp(row, date_col, y_col, m_col, d_col)
            if ts is None or ts < start or ts > end:
                continue
            value_str = (row.get(qobs_col) or "").strip()
            discharge = _safe_float(value_str)
            # LamaH uses the -999 family as a missing-value sentinel.
            if discharge is not None and discharge <= -990:
                discharge = None
            quality = (
                QualityFlag.RAW if discharge is not None else QualityFlag.MISSING
            )
            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=discharge,
                quality=quality,
            ))
        return observations

    @staticmethod
    def _row_timestamp(
        row: dict[str, str],
        date_col: str | None,
        y_col: str | None,
        m_col: str | None,
        d_col: str | None,
    ) -> datetime | None:
        """Build a UTC timestamp from a single date column or Y/M/D columns."""
        if date_col:
            raw = (row.get(date_col) or "").strip()
            for fmt in ("%Y-%m-%d", "%Y%m%d", "%d.%m.%Y", "%Y-%m-%dT%H:%M:%S"):
                try:
                    return datetime.strptime(raw, fmt).replace(tzinfo=UTC)
                except ValueError:
                    continue
        if y_col and m_col and d_col:
            try:
                return datetime(
                    int(float(row[y_col])),
                    int(float(row[m_col])),
                    int(float(row[d_col])),
                    tzinfo=UTC,
                )
            except (ValueError, KeyError, TypeError):
                return None
        return None

    # ------------------------------------------------------------------
    # Low-level helpers
    # ------------------------------------------------------------------

    def _read_delimited(
        self, file_path: Path,
    ) -> tuple[list[dict[str, str]], list[str]]:
        """Read a CSV that may be semicolon- or comma-delimited."""
        try:
            text = file_path.read_text(encoding="utf-8", errors="replace")
        except OSError as exc:
            raise ConnectorError(
                self.slug, f"Cannot read LamaH-Ice file {file_path}: {exc}",
            ) from exc

        first = text.splitlines()[0] if text else ""
        delimiter = ";" if ";" in first else ","
        reader = csv.DictReader(io.StringIO(text), delimiter=delimiter)
        fields = [f.strip() for f in (reader.fieldnames or [])]
        rows = [
            {(k.strip() if k else k): v for k, v in row.items()}
            for row in reader
        ]
        return rows, fields

    @staticmethod
    def _match_column(
        fields: list[str],
        hints: tuple[str, ...],
        exact_first: tuple[str, ...] = (),
    ) -> str | None:
        """Return the first field matching an exact name, then any hint substring."""
        lower = {f.lower(): f for f in fields}
        for name in exact_first:
            if name in lower:
                return lower[name]
        for hint in hints:
            for low, original in lower.items():
                if hint in low:
                    return original
        return None

    def _empty_chunk(self, station_id: str) -> TimeSeriesChunk:
        """Return an empty TimeSeriesChunk for a station."""
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )


def _safe_float(value: object) -> float | None:
    """Safely convert a value to float, mapping NA-style sentinels to None."""
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.lower() in ("na", "nan", "-", "n/a"):
        return None
    try:
        return float(text)
    except (ValueError, TypeError):
        return None
