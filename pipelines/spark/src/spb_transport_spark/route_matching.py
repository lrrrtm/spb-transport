"""Route/shape matching of vehicle tracks without realtime trip_id (ADR 0004).

A vehicle track is a time-ordered series of positions with one `route_id`. The static shapes of
that route are the candidate paths. Matching is a hidden Markov model in the spirit of
Newson & Krumm (2009):

- hidden state of an observation: one projection onto a shape pass (`ShapeGeometry.candidates`)
  or the explicit `off_route` state;
- emission: Gaussian GPS offset plus agreement of the reported bearing with the shape direction
  while the vehicle moves;
- transition on the same shape: progress along the shape must be monotone up to GPS noise and
  physically reachable; its likelihood decays with the difference between progress along the
  shape and the straight-line displacement;
- transition to another shape (or back to the start of the same one) is a variant switch. It is
  cheap only from the end of one shape to the start of the next one (terminal turnaround).

Viterbi gives the most likely path, forward-backward gives the posterior probability of every
chosen state. That posterior is the `confidence`: it is low when variants overlap and the track
contains no evidence that separates them. The module has no Spark dependency.
"""

from __future__ import annotations

import hashlib
import math
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass
from enum import StrEnum

from spb_transport_spark.shape_geometry import (
    ShapeGeometry,
    ShapeProjection,
    bearing_difference_deg,
    haversine_m,
)

ALGORITHM_VERSION = "route-match-hmm-v1"
NEG_INF = -math.inf


class MatchStatus(StrEnum):
    MATCHED = "matched"
    LOW_CONFIDENCE = "low_confidence"
    OFF_ROUTE = "off_route"
    NO_STATIC_SHAPE = "no_static_shape"


class _Transition(StrEnum):
    CONTINUE = "continue"
    RESTART = "restart"
    SWITCH = "switch"
    OFF_ROUTE = "off_route"


@dataclass(frozen=True)
class MatchingParameters:
    """Matcher thresholds. Units: metres, seconds, natural-log costs; speed in source units."""

    max_offset_m: float
    gps_sigma_m: float
    outlier_offset_m: float
    bearing_weight: float
    bearing_min_speed: float
    max_backtrack_m: float
    max_speed_mps: float
    transition_beta_m: float
    switch_cost: float
    terminal_restart_cost: float
    off_route_transition_cost: float
    terminal_zone_m: float
    max_gap_seconds: float
    min_confidence: float

    def __post_init__(self) -> None:
        for name in (
            "max_offset_m",
            "gps_sigma_m",
            "outlier_offset_m",
            "max_speed_mps",
            "transition_beta_m",
            "max_gap_seconds",
        ):
            if getattr(self, name) <= 0:
                raise ValueError(f"{name} must be positive")
        for name in (
            "bearing_weight",
            "bearing_min_speed",
            "max_backtrack_m",
            "switch_cost",
            "terminal_restart_cost",
            "off_route_transition_cost",
            "terminal_zone_m",
        ):
            if getattr(self, name) < 0:
                raise ValueError(f"{name} must not be negative")
        if not 0 < self.min_confidence <= 1:
            raise ValueError("min_confidence must be in (0, 1]")


@dataclass(frozen=True)
class Observation:
    event_id: str
    vehicle_id: str
    route_id: str
    timestamp: float
    latitude: float
    longitude: float
    bearing: float | None
    speed: float | None


@dataclass(frozen=True)
class RouteMatch:
    event_id: str
    track_id: str
    run_id: str | None
    status: MatchStatus
    confidence: float
    shape_id: str | None = None
    segment_index: int | None = None
    distance_along_m: float | None = None
    shape_length_m: float | None = None
    offset_m: float | None = None
    bearing_difference_deg: float | None = None


def stable_id(*parts: str) -> str:
    return hashlib.sha256("\x1f".join(parts).encode("utf-8")).hexdigest()


def split_tracks(
    observations: Iterable[Observation], max_gap_seconds: float
) -> list[list[Observation]]:
    """Split one vehicle's observations on a route_id change or a gap longer than allowed."""
    tracks: list[list[Observation]] = []
    previous: Observation | None = None
    for observation in sorted(observations, key=lambda o: o.timestamp):
        if (
            previous is None
            or observation.route_id != previous.route_id
            or observation.timestamp - previous.timestamp > max_gap_seconds
        ):
            tracks.append([])
        tracks[-1].append(observation)
        previous = observation
    return tracks


def _logsumexp(values: Sequence[float]) -> float:
    top = max(values)
    if top == NEG_INF:
        return NEG_INF
    return top + math.log(sum(math.exp(v - top) for v in values))


class RouteMatcher:
    def __init__(
        self, shapes_by_route: Mapping[str, Sequence[ShapeGeometry]], parameters: MatchingParameters
    ) -> None:
        self._shapes_by_route = shapes_by_route
        self._p = parameters
        self._lengths = {
            shape.shape_id: shape.length_m
            for shapes in shapes_by_route.values()
            for shape in shapes
        }

    def match_vehicle(self, observations: Iterable[Observation]) -> list[RouteMatch]:
        matches: list[RouteMatch] = []
        for track in split_tracks(observations, self._p.max_gap_seconds):
            matches += self.match_track(track)
        return matches

    def _candidates(self, observation: Observation) -> list[ShapeProjection | None]:
        states: list[ShapeProjection | None] = []
        for shape in self._shapes_by_route.get(observation.route_id, ()):
            states += shape.candidates(
                observation.latitude, observation.longitude, self._p.max_offset_m
            )
        states.append(None)
        return states

    def _moving_bearing(self, observation: Observation) -> float | None:
        if observation.bearing is None or observation.speed is None:
            return None
        if observation.speed < self._p.bearing_min_speed or observation.speed == 0:
            return None
        return observation.bearing

    def _emission(self, state: ShapeProjection | None, bearing: float | None) -> float:
        p = self._p
        if state is None:
            return -0.5 * (p.outlier_offset_m / p.gps_sigma_m) ** 2
        value = -0.5 * (state.offset_m / p.gps_sigma_m) ** 2
        if bearing is not None:
            difference = math.radians(bearing_difference_deg(bearing, state.segment_bearing_deg))
            value += p.bearing_weight * (math.cos(difference) - 1.0)
        return value

    def _transition(
        self,
        previous: ShapeProjection | None,
        current: ShapeProjection | None,
        elapsed_s: float,
        straight_m: float,
    ) -> tuple[_Transition, float]:
        p = self._p
        if previous is None and current is None:
            return _Transition.OFF_ROUTE, 0.0
        if previous is None or current is None:
            return _Transition.OFF_ROUTE, -p.off_route_transition_cost
        if previous.shape_id == current.shape_id:
            progress = current.distance_along_m - previous.distance_along_m
            reachable = p.max_speed_mps * max(elapsed_s, 1.0) + p.max_backtrack_m
            if -p.max_backtrack_m <= progress <= reachable:
                return _Transition.CONTINUE, -abs(progress - straight_m) / p.transition_beta_m
        at_end = previous.distance_along_m >= self._lengths[previous.shape_id] - p.terminal_zone_m
        at_start = current.distance_along_m <= p.terminal_zone_m
        if at_end and at_start:
            return _Transition.RESTART, -p.terminal_restart_cost
        if previous.shape_id != current.shape_id:
            return _Transition.SWITCH, -p.switch_cost
        return _Transition.CONTINUE, NEG_INF

    def _continues(
        self,
        last: tuple[Observation, ShapeProjection] | None,
        observation: Observation,
        state: ShapeProjection,
    ) -> bool:
        if last is None:
            return False
        before, previous = last
        kind, log_probability = self._transition(
            previous,
            state,
            observation.timestamp - before.timestamp,
            haversine_m(
                before.latitude, before.longitude, observation.latitude, observation.longitude
            ),
        )
        return kind == _Transition.CONTINUE and log_probability > NEG_INF

    def match_track(self, track: Sequence[Observation]) -> list[RouteMatch]:
        if not track:
            return []
        track_id = stable_id("track", track[0].vehicle_id, track[0].event_id)
        if not self._shapes_by_route.get(track[0].route_id):
            return [
                RouteMatch(o.event_id, track_id, None, MatchStatus.NO_STATIC_SHAPE, 1.0)
                for o in track
            ]

        states = [self._candidates(o) for o in track]
        emissions = [
            [self._emission(s, self._moving_bearing(o)) for s in candidates]
            for o, candidates in zip(track, states, strict=True)
        ]
        transitions: list[list[list[tuple[_Transition, float]]]] = [[]]
        for t in range(1, len(track)):
            before, now = track[t - 1], track[t]
            elapsed = now.timestamp - before.timestamp
            straight = haversine_m(before.latitude, before.longitude, now.latitude, now.longitude)
            transitions.append(
                [
                    [self._transition(a, b, elapsed, straight) for b in states[t]]
                    for a in states[t - 1]
                ]
            )

        # Viterbi and forward pass.
        best = list(emissions[0])
        alphas = [list(emissions[0])]
        pointers: list[list[int]] = [[-1] * len(states[0])]
        for t in range(1, len(track)):
            matrix = transitions[t]
            previous_alpha = alphas[-1]
            new_best: list[float] = []
            alpha: list[float] = []
            back: list[int] = []
            for j, emission in enumerate(emissions[t]):
                scores = [best[i] + matrix[i][j][1] for i in range(len(best))]
                winner = max(range(len(scores)), key=scores.__getitem__)
                new_best.append(scores[winner] + emission)
                back.append(winner)
                alpha.append(
                    _logsumexp([a + matrix[i][j][1] for i, a in enumerate(previous_alpha)])
                    + emission
                )
            best = new_best
            alphas.append(alpha)
            pointers.append(back)

        # Backward pass.
        betas: list[list[float]] = [[0.0] * len(states[-1])]
        for t in range(len(track) - 2, -1, -1):
            matrix = transitions[t + 1]
            following = betas[-1]
            betas.append(
                [
                    _logsumexp(
                        [
                            matrix[i][j][1] + emissions[t + 1][j] + following[j]
                            for j in range(len(states[t + 1]))
                        ]
                    )
                    for i in range(len(states[t]))
                ]
            )
        betas.reverse()
        evidence = _logsumexp(alphas[-1])

        path = [max(range(len(best)), key=best.__getitem__)]
        for t in range(len(track) - 1, 0, -1):
            path.append(pointers[t][path[-1]])
        path.reverse()

        matches: list[RouteMatch] = []
        run_id: str | None = None
        last_on_route: tuple[Observation, ShapeProjection] | None = None
        for t, (observation, index) in enumerate(zip(track, path, strict=True)):
            state = states[t][index]
            posterior = math.exp(alphas[t][index] + betas[t][index] - evidence)
            confidence = round(min(1.0, max(0.0, posterior)), 6)
            if state is None:
                matches.append(
                    RouteMatch(
                        observation.event_id, track_id, None, MatchStatus.OFF_ROUTE, confidence
                    )
                )
                continue
            # A run is one monotone pass along one shape. Off-route outliers inside the pass do
            # not break it: continuity is checked against the last on-route state.
            if run_id is None or not self._continues(last_on_route, observation, state):
                run_id = stable_id("run", track_id, observation.event_id)
            last_on_route = (observation, state)
            bearing = self._moving_bearing(observation)
            matches.append(
                RouteMatch(
                    event_id=observation.event_id,
                    track_id=track_id,
                    run_id=run_id,
                    status=(
                        MatchStatus.MATCHED
                        if confidence >= self._p.min_confidence
                        else MatchStatus.LOW_CONFIDENCE
                    ),
                    confidence=confidence,
                    shape_id=state.shape_id,
                    segment_index=state.segment_index,
                    distance_along_m=round(state.distance_along_m, 2),
                    shape_length_m=round(self._lengths[state.shape_id], 2),
                    offset_m=round(state.offset_m, 2),
                    bearing_difference_deg=(
                        None
                        if bearing is None
                        else round(bearing_difference_deg(bearing, state.segment_bearing_deg), 2)
                    ),
                )
            )
        return matches
