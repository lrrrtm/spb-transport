import math
from collections.abc import Sequence

import pytest
from spb_transport_spark.route_matching import (
    MatchingParameters,
    MatchStatus,
    Observation,
    RouteMatch,
    RouteMatcher,
    split_tracks,
)
from spb_transport_spark.shape_geometry import ShapeGeometry

LAT0, LON0 = 59.93, 30.31
M_PER_DEG_LAT = 111_195.0
M_PER_DEG_LON = M_PER_DEG_LAT * math.cos(math.radians(LAT0))

PARAMETERS = MatchingParameters(
    max_offset_m=75,
    gps_sigma_m=15,
    outlier_offset_m=50,
    bearing_weight=2,
    bearing_min_speed=1,
    max_backtrack_m=30,
    max_speed_mps=25,
    transition_beta_m=50,
    switch_cost=8,
    terminal_restart_cost=2,
    off_route_transition_cost=3,
    terminal_zone_m=300,
    max_gap_seconds=300,
    min_confidence=0.8,
)


def at(north_m: float, east_m: float) -> tuple[float, float]:
    return LAT0 + north_m / M_PER_DEG_LAT, LON0 + east_m / M_PER_DEG_LON


def shape(shape_id: str, *points_m: tuple[float, float]) -> ShapeGeometry:
    return ShapeGeometry(shape_id, [at(n, e) for n, e in points_m], cell_size_m=100)


def track(
    points_m: Sequence[tuple[float, float]],
    *,
    route_id: str = "r1",
    step_s: float = 10,
    bearings: Sequence[float | None] | None = None,
    speed: float | None = 5.0,
    vehicle_id: str = "v1",
    start_s: float = 1_790_000_000,
) -> list[Observation]:
    observations = []
    for index, (north, east) in enumerate(points_m):
        lat, lon = at(north, east)
        observations.append(
            Observation(
                event_id=f"{vehicle_id}-{index:05d}",
                vehicle_id=vehicle_id,
                route_id=route_id,
                timestamp=start_s + index * step_s,
                latitude=lat,
                longitude=lon,
                bearing=None if bearings is None else bearings[index],
                speed=speed,
            )
        )
    return observations


def by_status(matches: Sequence[RouteMatch]) -> list[str]:
    return [match.status.value for match in matches]


def test_vehicle_moving_along_single_shape_is_matched_with_monotone_progress() -> None:
    matcher = RouteMatcher({"r1": [shape("north", (0, 0), (2000, 0))]}, PARAMETERS)
    points = [(100 * i, 8 if i % 2 else -6) for i in range(15)]

    matches = matcher.match_vehicle(track(points, bearings=[0.0] * 15))

    assert set(by_status(matches)) == {"matched"}
    assert all(m.confidence > 0.99 for m in matches)
    assert len({m.run_id for m in matches}) == 1
    progress = [m.distance_along_m for m in matches]
    assert progress == sorted(progress)  # type: ignore[type-var]
    assert matches[3].distance_along_m == pytest.approx(300, abs=1)


def test_out_and_back_variants_are_separated_by_bearing_and_turnaround() -> None:
    north = shape("north", (0, 0), (1000, 0))
    south = shape("south", (1000, 12), (0, 12))
    matcher = RouteMatcher({"r1": [north, south]}, PARAMETERS)
    points = [(100.0 * i, 3) for i in range(11)] + [(1000 - 100.0 * i, 9) for i in range(1, 11)]
    bearings: list[float | None] = [*([0.0] * 11), *([180.0] * 10)]

    matches = matcher.match_vehicle(track(points, bearings=bearings))

    assert [m.shape_id for m in matches] == ["north"] * 11 + ["south"] * 10
    assert set(by_status(matches)) == {"matched"}
    assert len({m.run_id for m in matches[:11]}) == 1
    assert len({m.run_id for m in matches[11:]}) == 1
    assert matches[0].run_id != matches[-1].run_id


def test_future_divergence_resolves_overlapping_variants() -> None:
    east = shape("east", (0, 0), (1000, 0), (1000, 1000))
    west = shape("west", (0, 0), (1000, 0), (1000, -1000))
    matcher = RouteMatcher({"r1": [east, west]}, PARAMETERS)
    points = [(100.0 * i, 0) for i in range(11)] + [(1000, -100.0 * i) for i in range(1, 8)]

    matches = matcher.match_vehicle(track(points))

    assert {m.shape_id for m in matches} == {"west"}
    assert matches[0].status == MatchStatus.MATCHED


def test_overlap_without_distinguishing_evidence_is_low_confidence() -> None:
    east = shape("east", (0, 0), (1000, 0), (1000, 1000))
    west = shape("west", (0, 0), (1000, 0), (1000, -1000))
    matcher = RouteMatcher({"r1": [east, west]}, PARAMETERS)

    matches = matcher.match_vehicle(track([(100.0 * i, 0) for i in range(6)]))

    assert set(by_status(matches)) == {"low_confidence"}
    assert all(m.confidence == pytest.approx(0.5, abs=0.01) for m in matches)


def test_gps_outlier_is_off_route_and_does_not_break_the_run() -> None:
    matcher = RouteMatcher({"r1": [shape("north", (0, 0), (2000, 0))]}, PARAMETERS)
    points = [(100.0 * i, 0) for i in range(10)]
    points[5] = (500, 400)

    matches = matcher.match_vehicle(track(points))

    assert matches[5].status == MatchStatus.OFF_ROUTE
    assert matches[5].shape_id is None and matches[5].run_id is None
    assert by_status(matches[:5] + matches[6:]) == ["matched"] * 9
    assert len({m.run_id for m in matches if m.run_id}) == 1


def test_gps_jitter_while_stationary_stays_on_the_same_run() -> None:
    matcher = RouteMatcher({"r1": [shape("north", (0, 0), (2000, 0))]}, PARAMETERS)
    points = [(500 + (-8 if i % 2 else 8), 4) for i in range(12)]

    matches = matcher.match_vehicle(track(points, speed=0.0, bearings=[0.0] * 12))

    assert set(by_status(matches)) == {"matched"}
    assert len({m.run_id for m in matches}) == 1


def test_loop_route_restart_starts_a_new_run() -> None:
    loop = shape("loop", (0, 0), (500, 0), (500, 500), (0, 500), (0, 20))
    matcher = RouteMatcher({"r1": [loop]}, PARAMETERS)
    lap = [(0, 0), (250, 0), (500, 0), (500, 250), (500, 500), (250, 500), (0, 500), (0, 250)]
    points = lap + lap

    matches = matcher.match_vehicle(track(points, step_s=20))

    run_ids = [m.run_id for m in matches]
    assert len(set(run_ids)) == 2
    assert run_ids[: len(lap)] == [run_ids[0]] * len(lap)


def test_route_without_static_shape() -> None:
    matcher = RouteMatcher({}, PARAMETERS)

    matches = matcher.match_vehicle(track([(0, 0), (100, 0)], route_id="unknown"))

    assert by_status(matches) == ["no_static_shape", "no_static_shape"]
    assert all(m.run_id is None for m in matches)


def test_far_from_every_shape_is_off_route() -> None:
    matcher = RouteMatcher({"r1": [shape("north", (0, 0), (2000, 0))]}, PARAMETERS)

    matches = matcher.match_vehicle(track([(100.0 * i, 900) for i in range(4)]))

    assert by_status(matches) == ["off_route"] * 4


def test_ids_are_deterministic() -> None:
    matcher = RouteMatcher({"r1": [shape("north", (0, 0), (2000, 0))]}, PARAMETERS)
    observations = track([(100.0 * i, 0) for i in range(5)])

    assert matcher.match_vehicle(observations) == matcher.match_vehicle(reversed(observations))


def test_tracks_split_on_route_change_and_long_gap() -> None:
    first = track([(0, 0), (100, 0)], route_id="r1")
    second = track([(200, 0)], route_id="r2", start_s=first[-1].timestamp + 10)
    third = track([(300, 0)], route_id="r2", start_s=second[-1].timestamp + 301)

    tracks = split_tracks([*third, *second, *first], max_gap_seconds=300)

    assert [len(t) for t in tracks] == [2, 1, 1]


def test_parameters_are_validated() -> None:
    with pytest.raises(ValueError, match="min_confidence"):
        MatchingParameters(**{**PARAMETERS.__dict__, "min_confidence": 0})
    with pytest.raises(ValueError, match="gps_sigma_m"):
        MatchingParameters(**{**PARAMETERS.__dict__, "gps_sigma_m": 0})
