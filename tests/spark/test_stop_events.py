from collections.abc import Sequence

import pytest
from spb_transport_spark.stop_events import RunPoint, StopEventParameters, stop_events
from spb_transport_spark.stop_patterns import PatternStop, StopPattern

PARAMETERS = StopEventParameters(
    zone_before_m=30, zone_after_m=30, max_interpolation_gap_s=120, stopped_max_speed_mps=0.5
)
PATTERN = StopPattern(
    pattern_id="p" * 64,
    shape_id="s",
    trip_count=1,
    trip_share=1.0,
    stops=(
        PatternStop("A", 0, 0.0, 3.0),
        PatternStop("B", 1, 500.0, 3.0),
        PatternStop("C", 2, 1000.0, 3.0),
    ),
    unprojected_stop_ids=(),
)


def run(
    samples: Sequence[tuple[float, float]],
    confidence: float = 0.99,
    speeds: Sequence[float | None] | None = None,
) -> list[RunPoint]:
    return [
        RunPoint(
            f"e{index}",
            timestamp,
            distance,
            confidence,
            5.0 if speeds is None else speeds[index],
        )
        for index, (timestamp, distance) in enumerate(samples)
    ]


def test_constant_speed_pass_produces_interpolated_events_and_traversals() -> None:
    points = run([(10.0 * i, 50.0 * i - 25) for i in range(23)])  # 5 m/s from -25 m

    arrivals, traversals = stop_events("r", PATTERN, points, PARAMETERS)

    assert [a.stop_id for a in arrivals] == ["A", "B", "C"]
    b = arrivals[1]
    assert b.passage_time == pytest.approx(105)
    assert b.arrival_time == pytest.approx(99)
    assert b.departure_time == pytest.approx(111)
    assert b.dwell_s == pytest.approx(12)
    assert b.stopped is False  # driving through the zone at 5 m/s is not a stop
    assert [(t.from_stop_id, t.to_stop_id) for t in traversals] == [("A", "B"), ("B", "C")]
    assert traversals[0].travel_time_s == pytest.approx(100)
    assert traversals[0].speed_mps == pytest.approx(5)
    assert traversals[0].running_time_s == pytest.approx(88)
    assert traversals[0].max_gap_s == pytest.approx(10)


def test_dwell_at_stop_is_measured_and_jitter_does_not_repeat_the_event() -> None:
    samples = [(0.0, 400.0), (10.0, 480.0)]
    samples += [(10.0 + 10 * i, 500.0 + (5 if i % 2 else -5)) for i in range(1, 7)]
    samples += [(80.0, 560.0), (90.0, 640.0)]

    speeds = [5.0, 5.0, *([0.0] * 6), 5.0, 5.0]
    arrivals, _ = stop_events("r", PATTERN, run(samples, speeds=speeds), PARAMETERS)

    [b] = arrivals
    assert b.stop_id == "B"
    assert b.observations_in_zone == 7
    assert b.method == "observed"
    assert b.dwell_s is not None and b.dwell_s > 50
    assert b.stopped is True


def test_run_starting_inside_terminal_zone_has_departure_only_first_stop() -> None:
    points = run([(0.0, 0.0), (30.0, 0.0), (60.0, 100.0), (120.0, 520.0)])

    arrivals, traversals = stop_events("r", PATTERN, points, PARAMETERS)

    a, b = arrivals
    assert (a.stop_id, a.method) == ("A", "departure_only")
    assert a.passage_time is None and a.arrival_time is None
    assert a.departure_time == pytest.approx(39)
    assert b.stop_id == "B" and b.passage_time is not None
    [segment] = traversals
    assert segment.travel_time_s is None and segment.start_time is None
    assert segment.running_time_s == pytest.approx(b.arrival_time - 39)  # type: ignore[operator]


def test_run_starting_beyond_a_stop_has_no_event_for_it() -> None:
    points = run([(0.0, 100.0), (60.0, 520.0)])

    arrivals, traversals = stop_events("r", PATTERN, points, PARAMETERS)

    assert [a.stop_id for a in arrivals] == ["B"]
    assert traversals == []


def test_crossing_is_not_interpolated_over_a_long_gap() -> None:
    points = run([(0.0, 400.0), (300.0, 900.0), (310.0, 1100.0)])

    arrivals, traversals = stop_events("r", PATTERN, points, PARAMETERS)

    assert [a.stop_id for a in arrivals] == ["C"]
    assert arrivals[0].method == "interpolated"
    assert traversals == []


def test_run_ending_inside_zone_has_no_departure() -> None:
    arrivals, _ = stop_events("r", PATTERN, run([(0.0, 400.0), (20.0, 505.0)]), PARAMETERS)

    [b] = arrivals
    assert b.departure_time is None and b.dwell_s is None and b.stopped is None


def test_stop_is_unknown_without_observations_with_speed_in_zone() -> None:
    arrivals, _ = stop_events("r", PATTERN, run([(0.0, 400.0), (10.0, 600.0)]), PARAMETERS)
    [b] = arrivals
    assert b.dwell_s == pytest.approx(3)
    assert b.stopped is None

    samples = [(0.0, 400.0), (10.0, 495.0), (20.0, 600.0)]
    arrivals, _ = stop_events("r", PATTERN, run(samples, speeds=[5.0, None, 5.0]), PARAMETERS)
    assert arrivals[0].stopped is None


def test_event_confidence_is_the_weakest_bracketing_observation() -> None:
    points = [RunPoint("a", 0, 400, 0.95), RunPoint("b", 10, 600, 0.85)]

    arrivals, _ = stop_events("r", PATTERN, points, PARAMETERS)

    assert arrivals[0].confidence == pytest.approx(0.85)


def test_event_ids_are_deterministic() -> None:
    points = run([(10.0 * i, 50.0 * i - 25) for i in range(23)])

    first = stop_events("r", PATTERN, points, PARAMETERS)
    second = stop_events("r", PATTERN, list(reversed(points)), PARAMETERS)

    assert first == second
    assert len({a.event_id for a in first[0]}) == 3


def test_parameters_are_validated() -> None:
    with pytest.raises(ValueError, match="max_interpolation_gap_s"):
        StopEventParameters(
            zone_before_m=1, zone_after_m=1, max_interpolation_gap_s=0, stopped_max_speed_mps=0
        )
    with pytest.raises(ValueError, match="must not be negative"):
        StopEventParameters(
            zone_before_m=-1, zone_after_m=1, max_interpolation_gap_s=1, stopped_max_speed_mps=0
        )


def test_stationary_progress_proves_a_stop_when_reported_speed_lags() -> None:
    samples = [(0.0, 400.0), (10.0, 490.0), (20.0, 491.0), (30.0, 491.0), (40.0, 600.0)]

    arrivals, _ = stop_events("r", PATTERN, run(samples, speeds=[5.0] * 5), PARAMETERS)

    assert arrivals[0].stopped is True
