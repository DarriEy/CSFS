"""EStreams connector -- European streamflow dataset (Zenodo).

EStreams covers 17,130 catchments across 41 European countries with up to
120 years of data (Serrano-Notivoli et al., 2024).  This connector serves
countries that lack a dedicated national connector in CSFS: Luxembourg,
Albania, Montenegro, and North Macedonia.

The primary value is the **station catalogue** -- metadata telling users
where to find streamflow data for these countries.  EStreams distributes
pre-processed streamflow indices rather than raw daily discharge, so
``fetch_observations`` returns an empty ``TimeSeriesChunk`` with a
diagnostic log message.

References
----------
- DOI: 10.5281/zenodo.13154470
- Paper: https://doi.org/10.1038/s41597-024-03706-1
"""

from __future__ import annotations

import csv
import io
from datetime import UTC, datetime

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import ConnectorError, DataFormatError
from csfs.core.models import Station, TimeSeriesChunk
from csfs.core.registry import register

logger = structlog.get_logger()

# Zenodo record for EStreams v1.0
_ZENODO_RECORD_ID = "13154470"

# Countries served by this connector (ISO 3166-1 alpha-2)
_TARGET_COUNTRIES = {"LU", "AL", "ME", "MK"}

# Substring used to identify the streamflow catalogue CSV among Zenodo files
_CATALOGUE_FILENAME_HINT = "estreams_gauging_stations"


def _safe_float(value: object) -> float | None:
    """Safely convert a value to float."""
    if value is None:
        return None
    try:
        return float(str(value))
    except (ValueError, TypeError):
        return None


@register("estreams")
class EStreamsConnector(BaseConnector):
    """Connector for EStreams European streamflow catalogue on Zenodo."""

    slug = "estreams"
    display_name = "EStreams (European Streamflow Dataset)"
    base_url = "https://zenodo.org/api"
    country_codes = ["LU", "AL", "ME", "MK"]

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    async def fetch_stations(self) -> list[Station]:
        """Return stations for target countries from the EStreams catalogue.

        1. Fetch Zenodo record metadata to discover file URLs.
        2. Find the streamflow catalogue CSV.
        3. Download, parse, and filter to target countries.
        """
        catalogue_url = await self._resolve_catalogue_url()
        csv_text = await self._download_file(catalogue_url)
        return self._parse_catalogue_csv(csv_text)

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Return an empty chunk -- EStreams provides indices, not raw Q.

        EStreams distributes pre-processed streamflow indices rather than
        raw daily discharge time series.  Users should consult the
        original national data providers for raw observations.
        """
        logger.info(
            "estreams_no_raw_observations",
            provider=self.slug,
            station=station_id,
            detail=(
                "EStreams provides streamflow indices, not raw daily "
                "discharge. Consult the national provider for raw data."
            ),
        )
        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=[],
            fetched_at=datetime.now(UTC),
        )

    async def fetch_latest(self, station_id: str) -> TimeSeriesChunk:
        """Return an empty chunk (no real-time data available)."""
        return await self.fetch_observations(
            station_id,
            start=datetime.now(UTC),
            end=datetime.now(UTC),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    async def _resolve_catalogue_url(self) -> str:
        """Query the Zenodo API for the record and find the catalogue CSV URL."""
        try:
            resp = await self._get(f"/records/{_ZENODO_RECORD_ID}")
            data = resp.json()
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to fetch Zenodo record {_ZENODO_RECORD_ID}: {exc}",
            ) from exc

        files = data.get("files", [])
        if not files:
            raise DataFormatError(
                self.slug,
                f"Zenodo record {_ZENODO_RECORD_ID} has no files",
            )

        for file_entry in files:
            key = file_entry.get("key", "")
            if (
                _CATALOGUE_FILENAME_HINT in key.lower()
                and key.lower().endswith(".csv")
            ):
                link = (
                    file_entry.get("links", {}).get("self")
                    or file_entry.get("links", {}).get("download")
                )
                if link:
                    return str(link)

        # Fallback: pick the first CSV file
        for file_entry in files:
            key = file_entry.get("key", "")
            if key.lower().endswith(".csv"):
                link = (
                    file_entry.get("links", {}).get("self")
                    or file_entry.get("links", {}).get("download")
                )
                if link:
                    logger.warning(
                        "catalogue_csv_heuristic_fallback",
                        provider=self.slug,
                        filename=key,
                    )
                    return str(link)

        raise DataFormatError(
            self.slug,
            "No CSV file found in Zenodo record files list",
        )

    async def _download_file(self, url: str) -> str:
        """Download a file by absolute URL and return its text content."""
        try:
            resp = await self.client.get(url, follow_redirects=True)
            if resp.status_code not in (200, 206):
                resp.raise_for_status()
            return resp.text
        except Exception as exc:
            raise ConnectorError(
                self.slug,
                f"Failed to download catalogue CSV: {exc}",
            ) from exc

    def _parse_catalogue_csv(self, csv_text: str) -> list[Station]:
        """Parse EStreams catalogue CSV and return stations for target countries.

        Expected columns (may vary by release):
        - provider_id / provider -- source agency identifier
        - code_basins -- basin/station code (used as native_id)
        - provider_country -- ISO-2 country code
        - provider_name -- station name
        - lat / latitude -- latitude
        - lon / longitude -- longitude
        - river_name / river -- river name
        - catchment_area / area_km2 -- catchment area in km2
        """
        stations: list[Station] = []
        reader = csv.DictReader(io.StringIO(csv_text))

        if reader.fieldnames is None:
            raise DataFormatError(
                self.slug,
                "Catalogue CSV has no header row",
            )

        # Normalise column names to lowercase for resilient matching
        field_map = {f.lower().strip(): f for f in reader.fieldnames}

        for row in reader:
            try:
                station = self._parse_catalogue_row(row, field_map)
                if station is not None:
                    stations.append(station)
            except (ValueError, KeyError, TypeError) as exc:
                logger.debug(
                    "catalogue_row_skipped",
                    provider=self.slug,
                    error=str(exc),
                )
                continue

        logger.info(
            "estreams_stations_loaded",
            provider=self.slug,
            count=len(stations),
            countries=sorted(_TARGET_COUNTRIES),
        )
        return stations

    def _parse_catalogue_row(
        self,
        row: dict[str, str],
        field_map: dict[str, str],
    ) -> Station | None:
        """Parse one CSV row into a Station, or return None if not a target country."""
        # Build a lowercase-keyed copy for resilient access
        lrow = {k.lower().strip(): v for k, v in row.items()}

        country = (
            lrow.get("provider_country")
            or lrow.get("country")
            or lrow.get("country_code")
            or ""
        ).strip().upper()

        if country not in _TARGET_COUNTRIES:
            return None

        native_id = (
            lrow.get("code_basins")
            or lrow.get("code")
            or lrow.get("station_id")
            or lrow.get("id")
            or ""
        ).strip()

        if not native_id:
            return None

        name = (
            lrow.get("provider_name")
            or lrow.get("station_name")
            or lrow.get("name")
            or ""
        ).strip()

        lat = _safe_float(
            lrow.get("lat")
            or lrow.get("latitude")
        )
        lon = _safe_float(
            lrow.get("lon")
            or lrow.get("longitude")
        )

        if lat is None or lon is None:
            return None

        river = (
            lrow.get("river_name")
            or lrow.get("river")
            or None
        )
        if river is not None:
            river = river.strip() or None

        catchment = _safe_float(
            lrow.get("catchment_area")
            or lrow.get("area_km2")
            or lrow.get("catchment_area_km2")
        )

        return Station(
            id=self._station_id(native_id),
            provider=self.slug,
            native_id=native_id,
            name=name or native_id,
            latitude=lat,
            longitude=lon,
            country_code=country,
            river=river,
            catchment_area_km2=catchment,
        )
