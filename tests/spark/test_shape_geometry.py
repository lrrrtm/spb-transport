import math

import pytest
from spb_transport_spark.shape_geometry import ShapeGeometry, bearing_difference_deg

LAT0, LON0 = 59.93, 30.31
M_PER_DEG_LAT = 111_195.0
M_PER_DEG_LON = M_PER_DEG_LAT * math.cos(math.radians(LAT0))


def at(north_m: float, east_m: float) -> tuple[float, float]:
    return LAT0 + north_m / M_PER_DEG_LAT, LON0 + east_m / M_PER_DEG_LON


def shape(*points_m: tuple[float, float], shape_id: str = "s1") -> ShapeGeometry:
    return ShapeGeometry(shape_id, [at(n, e) for n, e in points_m], cell_size_m=100)


def test_projects_point_on_straight_northbound_shape() -> None:
    geometry = shape((0, 0), (1000, 0))

    [projection] = geometry.candidates(*at(400, 20), max_offset_m=50)

    assert geometry.length_m == pytest.approx(1000, abs=1)
    assert projection.distance_along_m == pytest.approx(400, abs=1)
    assert projection.offset_m == pytest.approx(20, abs=0.5)
    assert projection.segment_bearing_deg == pytest.approx(0, abs=0.5)


def test_distance_along_accumulates_over_segments() -> None:
    geometry = shape((0, 0), (500, 0), (500, 300))

    [projection] = geometry.candidates(*at(510, 100), max_offset_m=50)

    assert projection.segment_index == 1
    assert projection.distance_along_m == pytest.approx(600, abs=1)
    assert projection.segment_bearing_deg == pytest.approx(90, abs=0.5)


def test_point_beyond_offset_has_no_candidates() -> None:
    assert shape((0, 0), (1000, 0)).candidates(*at(400, 80), max_offset_m=50) == []


def test_out_and_back_shape_yields_one_candidate_per_pass() -> None:
    geometry = shape((0, 0), (1000, 0), (1000, 15), (0, 15))

    passes = geometry.candidates(*at(300, 7), max_offset_m=50)

    assert [round(p.distance_along_m) for p in passes] == [300, 1715]
    assert [round(p.segment_bearing_deg) for p in passes] == [0, 180]


def test_long_segment_is_found_far_from_its_endpoints() -> None:
    geometry = shape((0, 0), (5000, 0))

    assert len(geometry.candidates(*at(2500, 10), max_offset_m=30)) == 1


def test_shape_requires_two_points() -> None:
    with pytest.raises(ValueError, match="two points"):
        ShapeGeometry("s", [at(0, 0)], cell_size_m=100)


@pytest.mark.parametrize(
    ("first", "second", "expected"),
    [(0, 0, 0), (10, 350, 20), (90, 270, 180), (359, 1, 2), (180, 90, 90)],
)
def test_bearing_difference(first: float, second: float, expected: float) -> None:
    assert bearing_difference_deg(first, second) == pytest.approx(expected)
