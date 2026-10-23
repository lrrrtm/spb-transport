"""Stop-arrival and segment-traversal events from matched runs (T10, ADR 0006).

Input is one run of the route matcher: a monotone pass of a vehicle along one shape, with only
`matched` (high-confidence) observations. The shape fixes the direction of travel and the order
of stops, so an event is never produced for a vehicle going the opposite way.

Each stop of the shape pattern has a geofence along the shape: `[d - zone_before, d + zone_after]`
around the stop's distance `d`. Times are interpolated linearly between the two observations
that bracket a threshold:

- `arrival_time`  — progress first reaches `d - zone_before` (zone entry);
- `passage_time`  — progress first reaches `d` (the stop line);
- `departure_time` — progress first exceeds `d + zone_after` (zone exit).

Repeated crossing is impossible by construction: thresholds are applied to the running maximum
of progress, so GPS jitter backwards and forwards around a stop cannot create a second event. A
crossing is not interpolated across a gap longer than `max_interpolation_gap_s`; a stop whose line
the run starts beyond (for example the first stop while the vehicle waits at the terminal) has no
event. Dwell is `departure_time - arrival_time`; `stopped` is dwell ≥ `min_dwell_s`.

A segment traversal connects the passage times of two consecutive pattern stops in the same run.
The module has no Spark dependency.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass

from spb_transport_spark.route_matching import stable_id
from spb_transport_spark.stop_patterns import PatternStop, StopPattern

ALGORITHM_VERSION = "stop-events-v1"


@dataclass(frozen=True)
class StopEventParameters:
    zone_before_m: float
    zone_after_m: float
    max_interpolation_gap_s: float
    min_dwell_s: float

    def __post_init__(self) -> None:
        if self.zone_before_m < 0 or self.zone_after_m < 0 or self.min_dwell_s < 0:
            raise ValueError("stop zone sizes and min_dwell_s must not be negative")
        if self.max_interpolation_gap_s <= 0:
            raise ValueError("max_interpolation_gap_s must be positive")


@dataclass(frozen=True)
class RunPoint:
    event_id: str
    timestamp: float
    distance_along_m: float
    confidence: float


@dataclass(frozen=True)
class Crossing:
    time: float
    gap_s: float
    confidence: float


@dataclass(frozen=True)
class StopArrival:
    event_id: str
    run_id: str
    pattern_id: str
    stop_id: str
    stop_index: int
    stop_distance_m: float
    arrival_time: float | None
    passage_time: float | None
    departure_time: float | None
    dwell_s: float | None
    stopped: bool | None
    observations_in_zone: int
    method: str
    crossing_gap_s: float
    confidence: float


@dataclass(frozen=True)
class SegmentTraversal:
    event_id: str
    run_id: str
    pattern_id: str
    from_stop_id: str
    to_stop_id: str
    from_stop_index: int
    to_stop_index: int
    segment_length_m: float
    start_time: float | None
    end_time: float | None
    departure_time: float | None
    arrival_time: float | None
    travel_time_s: float | None
    running_time_s: float | None
    speed_mps: float | None
    observations: int
    max_gap_s: float
    confidence: float


class _Progress:
    """Running maximum of progress along the shape over a time-ordered run."""

    def __init__(self, points: Sequence[RunPoint]) -> None:
        self.points = sorted(points, key=lambda point: point.timestamp)
        self.envelope: list[float] = []
        top = -float("inf")
        for point in self.points:
            top = max(top, point.distance_along_m)
            self.envelope.append(top)

    def crossing(self, threshold: float, max_gap_s: float) -> Crossing | None:
        """Interpolated time when progress first reaches `threshold`, if it is observable."""
        for index, value in enumerate(self.envelope):
            if value < threshold:
                continue
            if index == 0:
                return None
            before, after = self.points[index - 1], self.points[index]
            gap = after.timestamp - before.timestamp
            if gap > max_gap_s:
                return None
            low = self.envelope[index - 1]
            ratio = (threshold - low) / (value - low) if value > low else 1.0
            return Crossing(
                time=before.timestamp + ratio * gap,
                gap_s=gap,
                confidence=min(before.confidence, after.confidence),
            )
        return None


def _round(value: float | None, digits: int = 3) -> float | None:
    return None if value is None else round(value, digits)


def stop_events(
    run_id: str,
    pattern: StopPattern,
    points: Sequence[RunPoint],
    parameters: StopEventParameters,
) -> tuple[list[StopArrival], list[SegmentTraversal]]:
    if len(points) < 2:
        return [], []
    progress = _Progress(points)
    gap = parameters.max_interpolation_gap_s
    arrivals: list[StopArrival] = []
    first_progress = progress.envelope[0]
    for stop in pattern.stops:
        zone_start = stop.distance_along_m - parameters.zone_before_m
        zone_end = stop.distance_along_m + parameters.zone_after_m
        passage = progress.crossing(stop.distance_along_m, gap)
        entry = progress.crossing(zone_start, gap)
        exit_ = progress.crossing(zone_end + 1e-6, gap)
        if passage is not None:
            method_crossing = passage
        elif exit_ is not None and zone_start <= first_progress <= zone_end:
            # The run starts inside the zone (typically at a terminal): only departure is known.
            method_crossing = exit_
        else:
            continue
        dwell = None if entry is None or exit_ is None else exit_.time - entry.time
        in_zone = sum(zone_start <= value <= zone_end for value in progress.envelope)
        observed = "observed" if in_zone else "interpolated"
        method = "departure_only" if passage is None else observed
        arrivals.append(
            StopArrival(
                event_id=stable_id(
                    "stop-arrival", run_id, pattern.pattern_id, str(stop.stop_index)
                ),
                run_id=run_id,
                pattern_id=pattern.pattern_id,
                stop_id=stop.stop_id,
                stop_index=stop.stop_index,
                stop_distance_m=stop.distance_along_m,
                arrival_time=_round(None if entry is None else entry.time),
                passage_time=_round(None if passage is None else passage.time),
                departure_time=_round(None if exit_ is None else exit_.time),
                dwell_s=_round(dwell),
                stopped=None if dwell is None else dwell >= parameters.min_dwell_s,
                observations_in_zone=in_zone,
                method=method,
                crossing_gap_s=round(method_crossing.gap_s, 3),
                confidence=method_crossing.confidence,
            )
        )
    return arrivals, segment_traversals(run_id, pattern, progress, arrivals)


def _first(*values: float | None) -> float | None:
    return next((value for value in values if value is not None), None)


def segment_traversals(
    run_id: str,
    pattern: StopPattern,
    progress: _Progress,
    arrivals: Sequence[StopArrival],
) -> list[SegmentTraversal]:
    """Traversals between consecutive pattern stops that both have events in the run.

    `travel_time_s` is stop line to stop line (includes dwell at the origin after the line);
    `running_time_s` is zone exit to next zone entry (excludes dwell). Either may be unknown.
    """
    by_index: dict[int, StopArrival] = {arrival.stop_index: arrival for arrival in arrivals}
    stops: dict[int, PatternStop] = {stop.stop_index: stop for stop in pattern.stops}
    ordered = [stop.stop_index for stop in pattern.stops]
    times = [point.timestamp for point in progress.points]
    traversals: list[SegmentTraversal] = []
    for from_index, to_index in zip(ordered, ordered[1:], strict=False):
        start, end = by_index.get(from_index), by_index.get(to_index)
        if start is None or end is None:
            continue
        travel = (
            None
            if start.passage_time is None or end.passage_time is None
            else end.passage_time - start.passage_time
        )
        running = (
            None
            if start.departure_time is None or end.arrival_time is None
            else end.arrival_time - start.departure_time
        )
        if running is not None and running < 0:
            running = None  # overlapping zones of close stops
        if travel is None and running is None:
            continue
        begin = _first(start.passage_time, start.departure_time)
        finish = _first(end.passage_time, end.arrival_time)
        assert begin is not None and finish is not None
        first = max((i for i, t in enumerate(times) if t <= begin), default=0)
        last = min((i for i, t in enumerate(times) if t >= finish), default=len(times) - 1)
        covering = progress.points[first : last + 1]
        length = stops[to_index].distance_along_m - stops[from_index].distance_along_m
        traversals.append(
            SegmentTraversal(
                event_id=stable_id(
                    "segment-traversal", run_id, pattern.pattern_id, str(from_index)
                ),
                run_id=run_id,
                pattern_id=pattern.pattern_id,
                from_stop_id=start.stop_id,
                to_stop_id=end.stop_id,
                from_stop_index=from_index,
                to_stop_index=to_index,
                segment_length_m=round(length, 2),
                start_time=start.passage_time,
                end_time=end.passage_time,
                departure_time=start.departure_time,
                arrival_time=end.arrival_time,
                travel_time_s=_round(travel),
                running_time_s=_round(running),
                speed_mps=round(length / travel, 3) if travel else None,
                observations=sum(begin <= t <= finish for t in times),
                max_gap_s=round(
                    max(
                        (
                            b.timestamp - a.timestamp
                            for a, b in zip(covering, covering[1:], strict=False)
                        ),
                        default=0.0,
                    ),
                    3,
                ),
                confidence=min(point.confidence for point in covering),
            )
        )
    return traversals
