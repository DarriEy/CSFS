"""Coordinate reprojection helper for dataset connectors.

Several CAMELS-family datasets publish gauge/outlet coordinates in a national
projected CRS (e.g. CAMELS-DK in ETRS89 / UTM 32N = EPSG:25832, CAMELS-FR in
NTF Lambert II = EPSG:27572) rather than WGS84. Connectors call :func:`to_wgs84`
to obtain the ``(latitude, longitude)`` the :class:`~csfs.core.models.Station`
model requires.
"""

from __future__ import annotations

from functools import lru_cache


@lru_cache(maxsize=64)
def _transformer(epsg_source: int):  # noqa: ANN202 — pyproj.Transformer
    from pyproj import Transformer

    # always_xy=True → transform takes/returns (x=lon/easting, y=lat/northing).
    return Transformer.from_crs(f"EPSG:{epsg_source}", "EPSG:4326", always_xy=True)


def to_wgs84(x: float, y: float, epsg_source: int) -> tuple[float, float]:
    """Project ``(x, y)`` in ``EPSG:<epsg_source>`` to ``(latitude, longitude)`` WGS84.

    ``x`` is easting/longitude and ``y`` is northing/latitude in the source CRS.
    """
    lon, lat = _transformer(epsg_source).transform(x, y)
    return lat, lon
