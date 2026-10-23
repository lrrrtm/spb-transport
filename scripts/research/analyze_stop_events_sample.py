"""Measure the inputs of T10 stop-arrival events on a collected research sample.

Reads a directory written by `collect_route_matching_sample.py`, matches it with the T09 matcher
(ROUTE_MATCH_* parameters from the environment) and runs stop-events with STOP_EVENT_* parameters.
Prints a Markdown report: stop projection onto shapes, inter-stop distances, where stationary
vehicles stand relative to stops, event methods, dwell, stop detection and segment traversals.
The report is the evidence for the thresholds in `docs/research/stop-arrivals.md`.

Usage:
    set -a; source <(grep -E '^(ROUTE_MATCH|STOP_EVENT)_' .env); set +a
    uv run python scripts/research/analyze_stop_events_sample.py \
        data/research/route-matching/<run>
"""

from __future__ import annotations

import argparse
import bisect
import csv
import io
import sys
import zipfile
from collections import Counter, defaultdict
from collections.abc import Iterator
from pathlib import Path

from spb_transport_spark.config import STOP_EVENT_PARAMETER_ENV, matching_parameters_from_env
from spb_transport_spark.config import required_finite_float as env_float
from spb_transport_spark.route_matching import Observation, RouteMatch, RouteMatcher
from spb_transport_spark.shape_geometry import ShapeGeometry
from spb_transport_spark.stop_events import (
    ALGORITHM_VERSION,
    RunPoint,
    SegmentTraversal,
    StopArrival,
    StopEventParameters,
    stop_events,
)
from spb_transport_spark.stop_patterns import StopPattern, build_stop_patterns

sys.path.insert(0, str(Path(__file__).parent))
from analyze_route_matching_sample import (  # noqa: E402
    CELL_SIZE_M,
    describe,
    load_observations,
    load_static,
    share,
)

STATIONARY_WINDOW_M = 150
STATIONARY_BIN_M = 10


def _read_csv(archive: zipfile.ZipFile, name: str) -> Iterator[dict[str, str]]:
    with archive.open(name) as raw:
        yield from csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig"))


def load_stop_static(
    feed: Path,
) -> tuple[Counter[tuple[str, tuple[str, ...]]], dict[str, tuple[float, float]]]:
    """Trip counts per (shape_id, ordered stop_ids) and stop coordinates."""
    with zipfile.ZipFile(feed) as archive:
        trip_shapes = {
            row["trip_id"]: row["shape_id"]
            for row in _read_csv(archive, "trips.txt")
            if row.get("shape_id")
        }
        sequences: dict[str, list[tuple[int, str]]] = defaultdict(list)
        for row in _read_csv(archive, "stop_times.txt"):
            sequences[row["trip_id"]].append((int(row["stop_sequence"]), row["stop_id"]))
        stops = {
            row["stop_id"]: (float(row["stop_lat"]), float(row["stop_lon"]))
            for row in _read_csv(archive, "stops.txt")
        }
    counts: Counter[tuple[str, tuple[str, ...]]] = Counter()
    for trip_id, items in sequences.items():
        if trip_id in trip_shapes:
            counts[(trip_shapes[trip_id], tuple(stop for _, stop in sorted(items)))] += 1
    return counts, stops


def analyze(sample: Path, stop_max_offset_m: float, parameters: StopEventParameters) -> str:
    _, shapes_by_route, shape_points = load_static(sample / "feed.zip")
    _, _, observations = load_observations(sample / "vehicle")
    counts, stops = load_stop_static(sample / "feed.zip")
    geometries = {
        shape_id: ShapeGeometry(shape_id, points, cell_size_m=CELL_SIZE_M)
        for shape_id, points in shape_points.items()
        if len(points) >= 2
    }
    pattern_counts = [(shape_id, ids, count) for (shape_id, ids), count in counts.items()]
    wide = build_stop_patterns(geometries, pattern_counts, stops, 150.0)
    patterns = build_stop_patterns(geometries, pattern_counts, stops, stop_max_offset_m)
    patterns_per_shape = Counter(Counter(shape_id for shape_id, _ in counts).values())
    total_stops = sum(len(p.stops) + len(p.unprojected_stop_ids) for p in patterns.values())
    offsets = [stop.offset_m for p in wide.values() for stop in p.stops]
    gaps = [
        b.distance_along_m - a.distance_along_m
        for p in patterns.values()
        for a, b in zip(p.stops, p.stops[1:], strict=False)
    ]
    zone = parameters.zone_before_m + parameters.zone_after_m

    matcher = RouteMatcher(
        {
            route_id: [geometries[s] for s in sorted(shape_ids) if s in geometries]
            for route_id, shape_ids in shapes_by_route.items()
        },
        matching_parameters_from_env(),
    )
    by_vehicle: dict[str, list[Observation]] = defaultdict(list)
    for sampled in observations:
        by_vehicle[sampled.vehicle_id].append(
            Observation(
                f"{sampled.vehicle_id}@{sampled.source_timestamp}",
                sampled.vehicle_id,
                sampled.route_id,
                sampled.source_timestamp,
                sampled.latitude,
                sampled.longitude,
                sampled.bearing,
                sampled.speed,
            )
        )
    runs: dict[str, list[tuple[Observation, RouteMatch]]] = defaultdict(list)
    for track in by_vehicle.values():
        by_event = {o.event_id: o for o in track}
        for match in matcher.match_vehicle(track):
            if match.status.value == "matched" and match.run_id is not None:
                runs[match.run_id].append((by_event[match.event_id], match))

    stationary: Counter[int] = Counter()
    positions: Counter[int] = Counter()
    arrivals: list[StopArrival] = []
    traversals: list[SegmentTraversal] = []
    for run_id, items in runs.items():
        pattern: StopPattern | None = patterns.get(items[0][1].shape_id or "")
        if pattern is None or not pattern.stops:
            continue
        lines = [stop.distance_along_m for stop in pattern.stops]
        points = []
        for o, m in items:
            assert m.distance_along_m is not None
            index = bisect.bisect_left(lines, m.distance_along_m)
            nearest = min(
                (m.distance_along_m - lines[j] for j in (index - 1, index) if 0 <= j < len(lines)),
                key=abs,
            )
            if abs(nearest) < STATIONARY_WINDOW_M:
                bucket = int(nearest // STATIONARY_BIN_M) * STATIONARY_BIN_M
                positions[bucket] += 1
                stationary[bucket] += o.speed == 0
            points.append(
                RunPoint(m.event_id, o.timestamp, m.distance_along_m, m.confidence, o.speed)
            )
        found, segments = stop_events(run_id, pattern, points, parameters)
        arrivals += found
        traversals += segments

    methods = Counter(a.method for a in arrivals)
    stopped = Counter(a.stopped for a in arrivals)
    known = [t for t in traversals if t.travel_time_s is not None]
    travel_times = [t.travel_time_s for t in known if t.travel_time_s is not None]
    total = len(arrivals)
    dwell_stopped = [a.dwell_s for a in arrivals if a.stopped and a.dwell_s is not None]
    dwell_moving = [a.dwell_s for a in arrivals if a.stopped is False and a.dwell_s is not None]
    dominant_share = [p.trip_share for p in patterns.values()]
    lines_out = [
        f"# Stop events report: {sample.name}",
        "",
        f"Алгоритм `{ALGORITHM_VERSION}`, параметры: `{parameters}`,"
        f" `stop_max_offset_m={stop_max_offset_m}`",
        "",
        "## 1. Stop patterns",
        "",
        f"- shapes с trips: {len({s for s, _ in counts})}; patterns на shape:"
        f" {dict(sorted(patterns_per_shape.items()))}",
        f"- доля trips доминирующего pattern: {describe(dominant_share, 3)}",
        f"- расстояние остановки до shape, м: {describe(offsets)}",
        f"- не спроецировано при {stop_max_offset_m:.0f} м:"
        f" {share(sum(len(p.unprojected_stop_ids) for p in patterns.values()), total_stops)}",
        f"- расстояние между соседними остановками, м: {describe(gaps, 0)};"
        f" зоны перекрываются (< {zone:.0f} м): {share(sum(g < zone for g in gaps), len(gaps))}",
        "",
        "## 2. Положение `matched` наблюдений относительно ближайшей остановки",
        "",
        "Отрицательное — до линии остановки. Пик доли `speed=0` над фоном показывает зону.",
        "",
        "| Интервал, м | Наблюдений | Из них speed=0 | Доля |",
        "|---:|---:|---:|---:|",
        *[
            f"| [{b}, {b + STATIONARY_BIN_M}) | {positions[b]} | {stationary[b]}"
            f" | {share(stationary[b], positions[b])} |"
            for b in range(-STATIONARY_WINDOW_M, STATIONARY_WINDOW_M, STATIONARY_BIN_M)
        ],
        "",
        "## 3. События у остановок",
        "",
        f"- прогонов: {len(runs)}; событий: {len(arrivals)};"
        + "; ".join(f" {k} {share(v, len(arrivals))}" for k, v in sorted(methods.items())),
        f"- stopped: true {share(stopped[True], len(arrivals))},"
        f" false {share(stopped[False], total)}, неизвестно {share(stopped[None], total)}",
        f"- dwell, с: {describe([a.dwell_s for a in arrivals if a.dwell_s is not None])}",
        f"- dwell при stopped=true, с: {describe(dwell_stopped)}",
        f"- dwell при stopped=false, с: {describe(dwell_moving)}",
        f"- интервал интерполяции пересечения, с: {describe([a.crossing_gap_s for a in arrivals])}",
        "",
        "## 4. Прохождения участков",
        "",
        f"- участков: {len(traversals)};"
        f" travel_time известно: {share(len(known), len(traversals))};"
        f" running_time известно:"
        f" {share(sum(t.running_time_s is not None for t in traversals), len(traversals))}",
        f"- travel_time, с: {describe(travel_times)}",
        f"- скорость, м/с: {describe([t.speed_mps for t in known if t.speed_mps is not None])}",
        f"- максимальный интервал наблюдений на участке, с:"
        f" {describe([t.max_gap_s for t in traversals])}",
    ]
    return "\n".join(lines_out) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze T10 stop events on a research sample")
    parser.add_argument("sample", type=Path)
    arguments = parser.parse_args()
    parameters = StopEventParameters(
        **{field: env_float(env) for field, env in STOP_EVENT_PARAMETER_ENV.items()}
    )
    report = analyze(arguments.sample, env_float("STOP_EVENT_STOP_MAX_OFFSET_M"), parameters)
    (arguments.sample / "stop-events-report.md").write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
