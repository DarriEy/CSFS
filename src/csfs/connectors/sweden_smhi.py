"""SMHI connector — Swedish Meteorological and Hydrological Institute hydrology data."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Observation, QualityFlag, Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# Discharge products offered by the SMHI hydroobs API, keyed by the
# connector-level ``resolution`` config option
# (``SwedenSMHIConnector(config={"resolution": "15min"})``).
_PARAMETER_BY_RESOLUTION = {
    "daily": 1,  # "Vattenföring (Dygn)" — daily mean discharge
    "15min": 2,  # "Vattenföring (15 min)" — 15-minute discharge
}

# A station's full 15-min corrected archive is served as one large JSON
# document (~73 MB for long-record stations); allow extra download time.
_ARCHIVE_TIMEOUT_15MIN_S = 300.0


def _quality_from_smhi(raw: str) -> QualityFlag:
    """Map SMHI quality codes to CSFS quality flags.

    SMHI's documented codes (the legend SMHI ships with every hydroobs
    data file):

        "G" (green)  -> "Kontrollerade och godkända värden"
                        (checked and approved)      -> GOOD
        "Y" (yellow) -> "Grovt kontrollerade värden"
                        (roughly checked)           -> SUSPECT
        "O" (orange) -> "Okontrollerade värden"
                        (unchecked, e.g. recent realtime data) -> RAW

    "Controlled" is kept for payloads that spell the green flag out.
    SMHI also mentions a red code for QC-rejected values, but rejected
    data is not served; any unknown/future code deliberately falls
    through to RAW (treated as unchecked).
    """
    code = raw.strip()
    if code in ("G", "Controlled"):
        return QualityFlag.GOOD
    if code == "Y":
        return QualityFlag.SUSPECT
    if code == "O":
        return QualityFlag.RAW
    # Deliberate fallthrough: treat unknown codes as unchecked data.
    return QualityFlag.RAW


@register("sweden_smhi")
class SwedenSMHIConnector(BaseConnector):
    slug = "sweden_smhi"
    display_name = "SMHI Hydrology (Sweden)"
    base_url = "https://opendata-download-hydroobs.smhi.se/api"
    country_codes = ["SE"]

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        resolution = str(self.config.get("resolution", "daily"))
        if resolution not in _PARAMETER_BY_RESOLUTION:
            raise ConnectorError(
                self.slug,
                f"Unknown resolution {resolution!r}; "
                f"expected one of {sorted(_PARAMETER_BY_RESOLUTION)}",
            )
        self.resolution = resolution
        self._parameter = _PARAMETER_BY_RESOLUTION[resolution]

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return all stations reporting the configured discharge product.

        Parameter 1 (daily discharge) by default; parameter 2 (15-minute
        discharge) when constructed with ``config={"resolution": "15min"}``.
        """
        resp = await self._get(f"/version/latest/parameter/{self._parameter}.json")
        data = resp.json()
        return self._parse_stations(data)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge observations for a station, filtered to [start, end].

        The hydroobs data endpoint accepts no date parameters, so the
        whole period file is downloaded and date-range filtering is done
        client-side. The period (``corrected-archive`` vs ``latest-day``)
        is chosen per :meth:`_select_period`.
        """
        # Normalize naive datetimes to UTC up front so both period
        # selection and filtering see timezone-aware bounds.
        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)
        if end.tzinfo is None:
            end = end.replace(tzinfo=UTC)

        native_id = station_id.removeprefix(f"{self.slug}:")
        period = self._select_period(start)
        timeout = (
            _ARCHIVE_TIMEOUT_15MIN_S
            if self.resolution == "15min" and period == "corrected-archive"
            else None
        )

        resp = await self._get(
            f"/version/latest/parameter/{self._parameter}/station/{native_id}"
            f"/period/{period}/data.json",
            timeout=timeout,
        )
        data = resp.json()
        return self._parse_observations(data, station_id, start, end)

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Fetch the most recent observations (last 24 h)."""
        now = datetime.now(UTC)
        return await self.fetch_observations(
            station_id,
            start=now - timedelta(hours=24),
            end=now,
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _select_period(self, start: datetime) -> str:
        """Pick the cheapest API period that covers the requested window.

        The hydroobs data endpoint only offers ``latest-hour``,
        ``latest-day`` and ``corrected-archive`` (there is no
        ``latest-months`` for this API, unlike SMHI metobs, and no date
        subsetting). The daily product's corrected archive is small, so
        it is always used — preserving long-standing behavior. The
        15-minute corrected archive is one ~73 MB JSON document per
        station, so when the requested window starts within the last
        24 hours the much smaller ``latest-day`` file (observed to span
        roughly the last two days) is fetched instead.
        """
        # One minute of slack so fetch_latest's "now - 24 h" window (computed
        # microseconds before this check) still takes the cheap path; the
        # latest-day file comfortably covers it.
        cutoff = datetime.now(UTC) - timedelta(hours=24, minutes=1)
        if self.resolution == "15min" and start >= cutoff:
            return "latest-day"
        return "corrected-archive"

    def _parse_stations(self, data: dict) -> list[Station]:
        """Parse the station listing JSON from the SMHI parameter endpoint."""
        stations: list[Station] = []
        for entry in data.get("station", []):
            native_id = str(entry.get("key", ""))
            if not native_id:
                continue

            is_active = entry.get("active", False)

            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=entry.get("name", ""),
                    latitude=float(entry.get("latitude", 0.0)),
                    longitude=float(entry.get("longitude", 0.0)),
                    country_code="SE",
                    is_active=is_active,
                ))
            except (ValueError, KeyError) as exc:
                logger.warning(
                    "station_parse_failed",
                    provider=self.slug,
                    station=native_id,
                    error=str(exc),
                )
                continue
        return stations

    def _parse_observations(
        self,
        data: dict,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Parse the observation JSON and filter to [start, end].

        Identical for both products: timestamps are epoch milliseconds
        (UTC) and values are m³/s (no conversion needed).
        """
        # Ensure start/end are timezone-aware (UTC) for comparison
        if start.tzinfo is None:
            start = start.replace(tzinfo=UTC)
        if end.tzinfo is None:
            end = end.replace(tzinfo=UTC)

        observations: list[Observation] = []
        for entry in data.get("value", []):
            date_ms = entry.get("date")
            if date_ms is None:
                continue

            try:
                ts = datetime.fromtimestamp(date_ms / 1000.0, tz=UTC)
            except (OSError, ValueError, OverflowError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid epoch timestamp in observation: {exc}",
                ) from exc

            # Client-side date range filter
            if ts < start or ts > end:
                continue

            raw_value = entry.get("value")
            discharge = float(raw_value) if raw_value is not None else None
            quality_code = entry.get("quality", "")
            quality = QualityFlag.MISSING if discharge is None else _quality_from_smhi(quality_code)

            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                discharge_m3s=discharge,
                quality=quality,
            ))

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )
