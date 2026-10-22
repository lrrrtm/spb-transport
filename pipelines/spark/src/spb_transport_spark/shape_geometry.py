"""Pure-Python projection of vehicle positions onto static GTFS shapes.

The module has no Spark dependency: Spark jobs and research scripts share the same geometry.
Distances use a local equirectangular projection around the shape, which is accurate to well
under one metre at city scale.
"""

from __future__ import annotations

import math
from collections import defaultdict
from collections.abc import Sequence
from dataclasses import dataclass

EARTH_RADIUS_M = 6_371_008.8


@dataclass(frozen=True)
class ShapeProjection:
    shape_id: str
    segment_index: int
    distance_along_m: float
    offset_m: float
    segment_bearing_deg: float


def haversine_m(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
    """Great-circle distance between two WGS84 points, in metres."""
    phi1, phi2 = math.radians(lat1), math.radians(lat2)
    half_dphi = (phi2 - phi1) / 2
    half_dlambda = math.radians(lon2 - lon1) / 2
    a = math.sin(half_dphi) ** 2 + math.cos(phi1) * math.cos(phi2) * math.sin(half_dlambda) ** 2
    return 2 * EARTH_RADIUS_M * math.asin(min(1.0, math.sqrt(a)))


def bearing_difference_deg(first: float, second: float) -> float:
    """Smallest absolute angle between two compass bearings, in [0, 180]."""
    difference = abs(first - second) % 360.0
    return 360.0 - difference if difference > 180.0 else difference


class ShapeGeometry:
    """Polyline of one GTFS shape with a uniform grid index over its segments."""

    def __init__(
        self, shape_id: str, points: Sequence[tuple[float, float]], *, cell_size_m: float
    ) -> None:
        if len(points) < 2:
            raise ValueError(f"shape {shape_id} must contain at least two points")
        if cell_size_m <= 0:
            raise ValueError("cell_size_m must be positive")
        self.shape_id = shape_id
        self._cell_size_m = cell_size_m
        self._cos_lat = math.cos(math.radians(sum(lat for lat, _ in points) / len(points)))
        self._xy = [self._to_xy(lat, lon) for lat, lon in points]
        self._start_m: list[float] = []
        self._bearing: list[float] = []
        self._cells: dict[tuple[int, int], list[int]] = defaultdict(list)
        travelled = 0.0
        for index, ((x1, y1), (x2, y2)) in enumerate(zip(self._xy, self._xy[1:], strict=False)):
            self._start_m.append(travelled)
            travelled += math.hypot(x2 - x1, y2 - y1)
            self._bearing.append(math.degrees(math.atan2(x2 - x1, y2 - y1)) % 360.0)
            for cell in self._covered_cells(min(x1, x2), min(y1, y2), max(x1, x2), max(y1, y2)):
                self._cells[cell].append(index)
        self.length_m = travelled

    def _to_xy(self, lat: float, lon: float) -> tuple[float, float]:
        return (
            math.radians(lon) * EARTH_RADIUS_M * self._cos_lat,
            math.radians(lat) * EARTH_RADIUS_M,
        )

    def _covered_cells(
        self, min_x: float, min_y: float, max_x: float, max_y: float
    ) -> list[tuple[int, int]]:
        size = self._cell_size_m
        return [
            (cx, cy)
            for cx in range(math.floor(min_x / size), math.floor(max_x / size) + 1)
            for cy in range(math.floor(min_y / size), math.floor(max_y / size) + 1)
        ]

    def _project_on_segment(self, index: int, x: float, y: float) -> ShapeProjection:
        (x1, y1), (x2, y2) = self._xy[index], self._xy[index + 1]
        dx, dy = x2 - x1, y2 - y1
        squared = dx * dx + dy * dy
        ratio = (
            0.0 if squared == 0 else max(0.0, min(1.0, ((x - x1) * dx + (y - y1) * dy) / squared))
        )
        px, py = x1 + ratio * dx, y1 + ratio * dy
        return ShapeProjection(
            shape_id=self.shape_id,
            segment_index=index,
            distance_along_m=self._start_m[index] + ratio * math.sqrt(squared),
            offset_m=math.hypot(x - px, y - py),
            segment_bearing_deg=self._bearing[index],
        )

    def candidates(self, lat: float, lon: float, max_offset_m: float) -> list[ShapeProjection]:
        """Projections within `max_offset_m`, one per pass of the shape near the point.

        A shape can pass the same place more than once (loops, out-and-back variants). Each run
        of consecutive nearby segments is a separate pass and yields its closest projection.
        """
        x, y = self._to_xy(lat, lon)
        segments = sorted(
            {
                index
                for cell in self._covered_cells(
                    x - max_offset_m, y - max_offset_m, x + max_offset_m, y + max_offset_m
                )
                for index in self._cells.get(cell, ())
            }
        )
        passes: list[ShapeProjection] = []
        previous_index = -2
        for index in segments:
            projection = self._project_on_segment(index, x, y)
            if projection.offset_m > max_offset_m:
                continue
            if passes and index == previous_index + 1:
                if projection.offset_m < passes[-1].offset_m:
                    passes[-1] = projection
            else:
                passes.append(projection)
            previous_index = index
        return passes
