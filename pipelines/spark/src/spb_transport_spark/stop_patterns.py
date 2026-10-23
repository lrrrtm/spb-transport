"""Static stop sequences of GTFS shapes, projected onto shape geometry.

GTFS links stops to trips, and trips to shapes. Several trips can share a shape with different
stop patterns (short turns, skipped stops), so one dominant pattern is chosen per shape: the one
served by most trips. Its stops are projected onto the shape in order, so that the distance along
the shape never decreases; this places stops correctly on loops and out-and-back shapes that pass
the same place twice. The module has no Spark dependency.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass

from spb_transport_spark.route_matching import stable_id
from spb_transport_spark.shape_geometry import ShapeGeometry


@dataclass(frozen=True)
class PatternStop:
    stop_id: str
    stop_index: int
    distance_along_m: float
    offset_m: float


@dataclass(frozen=True)
class StopPattern:
    pattern_id: str
    shape_id: str
    trip_count: int
    trip_share: float
    stops: tuple[PatternStop, ...]
    unprojected_stop_ids: tuple[str, ...]


@dataclass(frozen=True)
class DominantPattern:
    stop_ids: tuple[str, ...]
    trip_count: int
    trip_share: float


def dominant_patterns(
    pattern_counts: Iterable[tuple[str, Sequence[str], int]],
) -> dict[str, DominantPattern]:
    """Choose the pattern served by most trips of each shape; ties prefer more stops.

    Input rows are `(shape_id, ordered stop_ids, number of trips)`.
    """
    counts: dict[str, Counter[tuple[str, ...]]] = defaultdict(Counter)
    for shape_id, stop_ids, trip_count in pattern_counts:
        if len(stop_ids) >= 2 and trip_count > 0:
            counts[shape_id][tuple(stop_ids)] += trip_count
    chosen: dict[str, DominantPattern] = {}
    for shape_id, patterns in counts.items():
        stop_ids, count = min(patterns.items(), key=lambda item: (-item[1], -len(item[0]), item[0]))
        chosen[shape_id] = DominantPattern(stop_ids, count, count / sum(patterns.values()))
    return chosen


def project_pattern(
    shape: ShapeGeometry,
    pattern: DominantPattern,
    stop_coordinates: Mapping[str, tuple[float, float]],
    max_offset_m: float,
) -> StopPattern:
    """Place pattern stops on the shape with non-decreasing distance along it.

    Each stop takes the earliest pass of the shape near it that is not behind the previous stop.
    Stops without coordinates or farther than `max_offset_m` from any such pass are reported as
    unprojected and do not produce events.
    """
    projected: list[PatternStop] = []
    unprojected: list[str] = []
    previous = 0.0
    for index, stop_id in enumerate(pattern.stop_ids):
        coordinates = stop_coordinates.get(stop_id)
        feasible = (
            []
            if coordinates is None
            else [
                candidate
                for candidate in shape.candidates(*coordinates, max_offset_m=max_offset_m)
                if candidate.distance_along_m >= previous
            ]
        )
        if not feasible:
            unprojected.append(stop_id)
            continue
        best = min(feasible, key=lambda candidate: candidate.distance_along_m)
        previous = best.distance_along_m
        projected.append(
            PatternStop(
                stop_id=stop_id,
                stop_index=index,
                distance_along_m=round(best.distance_along_m, 2),
                offset_m=round(best.offset_m, 2),
            )
        )
    return StopPattern(
        pattern_id=stable_id("pattern", shape.shape_id, *pattern.stop_ids),
        shape_id=shape.shape_id,
        trip_count=pattern.trip_count,
        trip_share=round(pattern.trip_share, 6),
        stops=tuple(projected),
        unprojected_stop_ids=tuple(unprojected),
    )


def build_stop_patterns(
    shapes: Mapping[str, ShapeGeometry],
    pattern_counts: Iterable[tuple[str, Sequence[str], int]],
    stop_coordinates: Mapping[str, tuple[float, float]],
    max_offset_m: float,
) -> dict[str, StopPattern]:
    """Dominant projected stop pattern for every shape with known geometry."""
    return {
        shape_id: project_pattern(shapes[shape_id], pattern, stop_coordinates, max_offset_m)
        for shape_id, pattern in dominant_patterns(pattern_counts).items()
        if shape_id in shapes
    }
