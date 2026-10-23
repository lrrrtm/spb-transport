import math

import pytest
from spb_transport_spark.shape_geometry import ShapeGeometry
from spb_transport_spark.stop_patterns import (
    DominantPattern,
    build_stop_patterns,
    dominant_patterns,
    project_pattern,
)

LAT0, LON0 = 59.93, 30.31
M_PER_DEG_LAT = 111_195.0
M_PER_DEG_LON = M_PER_DEG_LAT * math.cos(math.radians(LAT0))


def at(north_m: float, east_m: float) -> tuple[float, float]:
    return LAT0 + north_m / M_PER_DEG_LAT, LON0 + east_m / M_PER_DEG_LON


def shape(shape_id: str, *points_m: tuple[float, float]) -> ShapeGeometry:
    return ShapeGeometry(shape_id, [at(n, e) for n, e in points_m], cell_size_m=100)


def test_dominant_pattern_is_served_by_most_trips() -> None:
    chosen = dominant_patterns(
        [
            ("s1", ["a", "b", "c"], 10),
            ("s1", ["a", "b"], 3),
            ("s1", ["a", "b", "c"], 2),
            ("s2", ["x"], 50),
            ("s3", ["p", "q"], 1),
            ("s3", ["p", "r", "q"], 1),
        ]
    )

    assert chosen["s1"] == DominantPattern(("a", "b", "c"), 12, 12 / 15)
    assert "s2" not in chosen  # one stop is not a pattern
    assert chosen["s3"].stop_ids == ("p", "r", "q")  # tie → more stops


def test_stops_are_projected_in_order_on_out_and_back_shape() -> None:
    geometry = shape("loop", (0, 0), (1000, 0), (1000, 20), (0, 20))
    stops = {"a": at(0, 10), "b": at(500, -5), "c": at(990, 10), "d": at(500, 25)}

    pattern = project_pattern(
        geometry, DominantPattern(("a", "b", "c", "d"), 1, 1.0), stops, max_offset_m=40
    )

    distances = [stop.distance_along_m for stop in pattern.stops]
    assert [stop.stop_id for stop in pattern.stops] == ["a", "b", "c", "d"]
    assert distances == sorted(distances)
    assert distances[1] == pytest.approx(500, abs=1)
    assert distances[3] == pytest.approx(1520, abs=1)  # second pass, not the first one
    assert pattern.unprojected_stop_ids == ()


def test_far_or_unknown_stops_are_reported_unprojected() -> None:
    geometry = shape("s", (0, 0), (1000, 0))
    stops = {"a": at(0, 0), "far": at(500, 300), "c": at(1000, 0)}

    pattern = project_pattern(
        geometry, DominantPattern(("a", "far", "missing", "c"), 1, 1.0), stops, max_offset_m=40
    )

    assert [stop.stop_index for stop in pattern.stops] == [0, 3]
    assert pattern.unprojected_stop_ids == ("far", "missing")


def test_pattern_ids_are_deterministic_and_skip_unknown_shapes() -> None:
    shapes = {"s": shape("s", (0, 0), (1000, 0))}
    rows = [("s", ["a", "b"], 1), ("unknown", ["a", "b"], 1)]
    stops = {"a": at(0, 0), "b": at(1000, 0)}

    first = build_stop_patterns(shapes, rows, stops, max_offset_m=40)
    second = build_stop_patterns(shapes, rows, stops, max_offset_m=40)

    assert list(first) == ["s"]
    assert first == second
    assert len(first["s"].pattern_id) == 64
