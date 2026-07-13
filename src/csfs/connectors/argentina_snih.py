"""Argentina INA (Instituto Nacional del Agua) connector — Alerta Hidrologica."""

from __future__ import annotations

from datetime import UTC, datetime

import structlog

from csfs.connectors.base import BaseConnector
from csfs.core.exceptions import DataFormatError
from csfs.core.models import (
    Observation,
    QualityFlag,
    Resolution,
    Station,
    TimeSeriesChunk,
    Variable,
)
from csfs.core.registry import register

logger = structlog.get_logger()

# Variable IDs used by INA's Alerta system.
_DISCHARGE_VAR_NAMES = {"caudal"}
_WATER_LEVEL_VAR_ID = 2  # Altura hidrometrica (stage, served in metres)


@register("argentina_snih")
class ArgentinaSnihConnector(BaseConnector):
    slug = "argentina_snih"
    display_name = "SNIH Argentina (INA)"
    base_url = "https://alerta.ina.gob.ar/a5"
    country_codes = ["AR"]
    supported_variables = ("discharge", "stage")

    def __init__(self, config: dict | None = None) -> None:
        super().__init__(config)
        # Caches: native station id -> discharge / stage series_id, plus the
        # resolution declared by each selected series' var_nombre.
        self._station_to_series: dict[str, int] = {}
        self._station_to_stage_series: dict[str, int] = {}
        self._series_resolution: dict[int, Resolution] = {}

    async def fetch_stations(self) -> list[Station]:
        """Return INA Alerta stations that carry discharge or stage data.

        The raw ``/obs/puntual/estaciones`` catalogue lists ~4700 stations,
        but ~76% of them have no discharge or stage series at all (they are
        precipitation/meteo points or dataless placeholders) — fetching
        observations for those can only fail. Filter the roster down to
        stations with at least one populated discharge or stage series
        (live-measured 2026-07: 4664 raw -> 892 usable stations).
        """
        resp = await self._get("/obs/puntual/estaciones")
        stations = self._parse_stations(resp.json())

        try:
            if (
                not self._station_to_series
                and not self._station_to_stage_series
            ):
                await self._build_series_cache()
        except Exception as exc:  # noqa: BLE001 - degrade to unfiltered
            logger.warning(
                "series_catalogue_unavailable",
                provider=self.slug,
                error=str(exc)[:120],
            )
            return stations

        usable = (
            self._station_to_series.keys()
            | self._station_to_stage_series.keys()
        )
        filtered = [s for s in stations if s.native_id in usable]
        logger.info(
            "stations_filtered_to_series",
            provider=self.slug,
            raw=len(stations),
            usable=len(filtered),
        )
        return filtered

    async def fetch_observations(
        self,
        station_id: str,
        start: datetime,
        end: datetime,
    ) -> TimeSeriesChunk:
        """Fetch discharge and stage observations for a station over a time range."""
        native_id = station_id.removeprefix(f"{self.slug}:")
        series = await self._resolve_series(native_id)

        observations: list[Observation] = []
        for series_id, variable in series:
            resp = await self._get(
                f"/obs/puntual/series/{series_id}/observaciones",
                params={
                    "timestart": start.strftime("%Y-%m-%dT%H:%M:%SZ"),
                    "timeend": end.strftime("%Y-%m-%dT%H:%M:%SZ"),
                },
            )
            observations.extend(self._parse_observations(
                resp.json(),
                station_id,
                variable,
                self._series_resolution.get(series_id, Resolution.UNKNOWN),
            ))

        return TimeSeriesChunk(
            station_id=station_id,
            provider=self.slug,
            observations=observations,
            fetched_at=datetime.now(UTC),
        )

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _parse_stations(self, data: list[dict]) -> list[Station]:
        """Parse the station list JSON from /obs/puntual/estaciones."""
        stations: list[Station] = []
        for entry in data:
            native_id = entry.get("id")
            if native_id is None:
                continue
            native_id = str(native_id)

            geom = entry.get("geom") or {}
            coords = geom.get("coordinates", [])
            if len(coords) < 2:
                continue

            try:
                lon = float(str(coords[0]))
                lat = float(str(coords[1]))
            except (ValueError, TypeError):
                continue

            try:
                stations.append(Station(
                    id=self._station_id(native_id),
                    provider=self.slug,
                    native_id=native_id,
                    name=entry.get("nombre") or native_id,
                    latitude=lat,
                    longitude=lon,
                    country_code="AR",
                    river=entry.get("rio"),
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
        data: list[dict],
        station_id: str,
        variable: Variable,
        resolution: Resolution,
    ) -> list[Observation]:
        """Parse the observations JSON array into ``Observation`` models.

        Values are already in canonical units: caudal in m3/s, altura
        hidrometrica in metres.
        """
        observations: list[Observation] = []
        for entry in data:
            try:
                ts = datetime.fromisoformat(entry["timestart"])
            except (KeyError, ValueError) as exc:
                raise DataFormatError(
                    self.slug,
                    f"Invalid timestamp in observation: {exc}",
                ) from exc

            value_raw = entry.get("valor")
            value = (
                float(str(value_raw)) if value_raw is not None else None
            )

            observations.append(Observation(
                station_id=station_id,
                timestamp=ts,
                variable=variable,
                resolution=resolution,
                value=value,
                quality=(
                    QualityFlag.RAW
                    if value is not None
                    else QualityFlag.MISSING
                ),
            ))

        return observations

    async def _build_series_cache(self) -> None:
        """Fetch the (paginated) series metadata and cache station -> series.

        Two things the SNIH catalogue forces:
          * It is PAGINATED (~5000 features/page, 10k+ total). Fetching only
            page 1 misses every discharge series on later pages.
          * A station usually has several 'caudal' (discharge) variants, and
            the plain "Caudal" series is frequently empty while a daily-mean
            ("Caudal medio diario") variant carries the data -- so prefer the
            daily-mean variant rather than the first one seen.

        'Altura hidrometrica' (stage, var id 2, metres) series are cached the
        same way. Each selected series' resolution is remembered: daily-mean
        ("... medio diario") variants are DAILY_MEAN, monthly-mean
        ("... medio mensual") variants are MONTHLY_MEAN, the rest declare no
        aggregation and stay UNKNOWN.

        Series that have never held data (no ``count`` and no ``timeend`` --
        ~2000 placeholder series in the live catalogue) are skipped entirely,
        and on equal variant priority the series with the most recent
        ``timeend`` wins, so a live series is never shadowed by a stale one.
        """
        # estacion_id -> (priority, timeend, series_id); higher wins.
        best_discharge: dict[str, tuple[int, str, int]] = {}
        best_stage: dict[str, tuple[int, str, int]] = {}

        def _priority(var_name: str) -> int:
            v = var_name.lower()
            if "diario" in v or "diaria" in v:  # daily mean -- most reliable
                return 3
            if "medio" in v or "media" in v:
                return 2
            return 1

        def _resolution(v: str) -> Resolution:
            if "diario" in v or "diaria" in v:
                return Resolution.DAILY_MEAN
            if "mensual" in v:
                return Resolution.MONTHLY_MEAN
            return Resolution.UNKNOWN

        next_url: str | None = "/obs/puntual/series"
        params: dict | None = {"format": "geojson"}
        while next_url:
            resp = await self._get(next_url, params=params)
            data = resp.json()
            for feat in data.get("features", []):
                props = feat.get("properties", {})
                var_name = props.get("var_nombre") or ""
                v = var_name.lower()
                if any(kw in v for kw in _DISCHARGE_VAR_NAMES):
                    best = best_discharge
                elif props.get("var_id") == _WATER_LEVEL_VAR_ID or "altura" in v:
                    best = best_stage
                else:
                    continue
                estacion_id = props.get("estacion_id")
                series_id = props.get("id")
                if estacion_id is None or series_id is None:
                    continue
                timeend = props.get("timeend") or ""
                if not props.get("count") and not timeend:
                    # Placeholder series that has never held an observation.
                    continue
                key = str(estacion_id)
                candidate = (_priority(var_name), timeend, int(series_id))
                if key not in best or candidate[:2] > best[key][:2]:
                    best[key] = candidate
                self._series_resolution[int(series_id)] = _resolution(v)
            if data.get("is_last_page"):
                break
            # next_page_url already carries its own query string.
            next_url = data.get("next_page_url") or None
            params = None

        for key, (_prio, _te, series_id) in best_discharge.items():
            self._station_to_series[key] = series_id
        for key, (_prio, _te, series_id) in best_stage.items():
            self._station_to_stage_series[key] = series_id

    async def _resolve_series(
        self, native_id: str
    ) -> list[tuple[int, Variable]]:
        """Return the (series_id, variable) pairs to fetch for a station.

        Uses the caches first; falls back to fetching the series metadata if
        the station is not mapped yet. Raises if the station has neither a
        discharge nor a stage series.
        """
        if (
            native_id not in self._station_to_series
            and native_id not in self._station_to_stage_series
        ):
            await self._build_series_cache()

        series: list[tuple[int, Variable]] = []
        discharge_id = self._station_to_series.get(native_id)
        if discharge_id is not None:
            series.append((discharge_id, Variable.DISCHARGE))
        stage_id = self._station_to_stage_series.get(native_id)
        if stage_id is not None:
            series.append((stage_id, Variable.STAGE))

        if not series:
            raise DataFormatError(
                self.slug,
                f"No discharge or stage series found for station '{native_id}'",
            )
        return series
