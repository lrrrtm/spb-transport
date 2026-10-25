"""Classify the T09/T10 edge cases listed in `docs/research/follow-ups.md` on a research sample.

Runs the T09 matcher (ROUTE_MATCH_* parameters from the environment) over a sample written by
`collect_route_matching_sample.py` and prints a Markdown report:

1. run boundaries in the middle of the same shape: time gap, progress jump, `off_route`
   observations in between, whether the vehicle keeps moving forward afterwards;
2. shape switches in the middle of a route: how long the new run lasts, whether the vehicle
   returns to the previous shape (A → B → A), whether bearings agree with the new shape;
3. repeated coordinates: series of >= 3 consecutive observations with identical coordinates and
   new `source_timestamp`, with `speed > 0` and with `speed = 0`, near stops and between them.

With `--examples <file.csv>` the script also writes up to 20 cases of each boundary kind with
the vehicle's observations around the boundary, to look at them on a map.

Usage:
    set -a; source <(grep -E '^ROUTE_MATCH_' .env); set +a
    uv run python scripts/research/analyze_matcher_edge_cases.py \
        data/research/route-matching/<run> [--examples cases.csv]
"""

from __future__ import annotations

import argparse
import csv
import math
import sys
import zipfile
from collections import Counter, defaultdict
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path

from spb_transport_spark.config import matching_parameters_from_env
from spb_transport_spark.route_matching import (
    MatchingParameters,
    MatchStatus,
    Observation,
    RouteMatch,
    RouteMatcher,
)
from spb_transport_spark.shape_geometry import ShapeGeometry, haversine_m

sys.path.insert(0, str(Path(__file__).parent))
from analyze_route_matching_sample import (  # noqa: E402
    CELL_SIZE_M,
    _read_csv,
    describe,
    load_observations,
    load_static,
    share,
)

NEAR_STOP_M = 40.0
SHORT_RUN_OBSERVATIONS = 5
EXAMPLES_PER_KIND = 20

Pair = tuple[Observation, RouteMatch]


@dataclass(frozen=True)
class Boundary:
    kind: str
    vehicle_id: str
    before: Pair
    after: Pair
    off_route_between: int
    next_run: list[Pair]
    run_after_next: list[Pair]
    next_boundary_terminal: bool | None


def load_stops(feed: Path) -> list[tuple[float, float]]:
    with zipfile.ZipFile(feed) as archive:
        return [
            (float(row["stop_lat"]), float(row["stop_lon"]))
            for row in _read_csv(archive, "stops.txt")
        ]


class StopIndex:
    """Grid over stop coordinates answering "is there a stop within `radius_m`"."""

    def __init__(self, stops: Sequence[tuple[float, float]], radius_m: float) -> None:
        self.radius_m = radius_m
        self.lat_step = radius_m / 111_195.0
        self.lon_step = radius_m / (111_195.0 * math.cos(math.radians(59.9)))
        self.cells: dict[tuple[int, int], list[tuple[float, float]]] = defaultdict(list)
        for lat, lon in stops:
            self.cells[self._cell(lat, lon)].append((lat, lon))

    def _cell(self, lat: float, lon: float) -> tuple[int, int]:
        return int(lat // self.lat_step), int(lon // self.lon_step)

    def near(self, lat: float, lon: float) -> bool:
        row, column = self._cell(lat, lon)
        return any(
            haversine_m(lat, lon, s_lat, s_lon) <= self.radius_m
            for dr in (-1, 0, 1)
            for dc in (-1, 0, 1)
            for s_lat, s_lon in self.cells.get((row + dr, column + dc), ())
        )


def match_sample(
    sample: Path, parameters: MatchingParameters
) -> tuple[dict[str, list[Pair]], list[Observation]]:
    _, shapes_by_route, shape_points = load_static(sample / "feed.zip")
    _, _, sampled = load_observations(sample / "vehicle")
    geometries = {
        shape_id: ShapeGeometry(shape_id, points, cell_size_m=CELL_SIZE_M)
        for shape_id, points in shape_points.items()
        if len(points) >= 2
    }
    matcher = RouteMatcher(
        {
            route_id: [geometries[s] for s in sorted(shape_ids) if s in geometries]
            for route_id, shape_ids in shapes_by_route.items()
        },
        parameters,
    )
    by_vehicle: dict[str, list[Observation]] = defaultdict(list)
    for o in sampled:
        by_vehicle[o.vehicle_id].append(
            Observation(
                f"{o.vehicle_id}@{o.source_timestamp}",
                o.vehicle_id,
                o.route_id,
                o.source_timestamp,
                o.latitude,
                o.longitude,
                o.bearing,
                o.speed,
            )
        )
    tracks: dict[str, list[Pair]] = defaultdict(list)
    for track in by_vehicle.values():
        by_event = {o.event_id: o for o in track}
        for match in matcher.match_vehicle(track):
            tracks[match.track_id].append((by_event[match.event_id], match))
    for pairs in tracks.values():
        pairs.sort(key=lambda pair: pair[0].timestamp)
    observations = [o for track in by_vehicle.values() for o in track]
    return tracks, observations


def at_terminal(runs: Sequence[Sequence[Pair]], index: int, parameters: MatchingParameters) -> bool:
    """Whether the boundary before `runs[index]` is a turnaround at a terminal."""
    m1, m2 = runs[index - 1][-1][1], runs[index][0][1]
    assert m1.distance_along_m is not None and m1.shape_length_m is not None
    assert m2.distance_along_m is not None
    return (
        m1.distance_along_m >= m1.shape_length_m - parameters.terminal_zone_m
        and m2.distance_along_m <= parameters.terminal_zone_m
    )


def boundaries(tracks: dict[str, list[Pair]], parameters: MatchingParameters) -> list[Boundary]:
    found: list[Boundary] = []
    for pairs in tracks.values():
        runs: list[list[Pair]] = []
        off_route_before: list[int] = []
        off_route = 0
        for pair in pairs:
            match = pair[1]
            if match.run_id is None:
                off_route += match.status == MatchStatus.OFF_ROUTE
                continue
            if not runs or runs[-1][-1][1].run_id != match.run_id:
                runs.append([])
                off_route_before.append(off_route)
            runs[-1].append(pair)
            off_route = 0

        for index in range(1, len(runs)):
            if at_terminal(runs, index, parameters):
                continue
            before, after = runs[index - 1][-1], runs[index][0]
            m1, m2 = before[1], after[1]
            found.append(
                Boundary(
                    kind="same_shape" if m1.shape_id == m2.shape_id else "shape_switch",
                    vehicle_id=before[0].vehicle_id,
                    before=before,
                    after=after,
                    off_route_between=off_route_before[index],
                    next_run=runs[index],
                    run_after_next=runs[index + 1] if index + 1 < len(runs) else [],
                    next_boundary_terminal=at_terminal(runs, index + 1, parameters)
                    if index + 1 < len(runs)
                    else None,
                )
            )
    return found


def run_progress(run: Sequence[Pair]) -> float:
    first, last = run[0][1].distance_along_m, run[-1][1].distance_along_m
    assert first is not None and last is not None
    return last - first


def same_shape_section(cases: Sequence[Boundary], runs: int) -> list[str]:
    gaps, jumps, speeds, straight_speeds, offs = [], [], [], [], []
    kinds: Counter[str] = Counter()
    for case in cases:
        (o1, m1), (o2, m2) = case.before, case.after
        assert m1.distance_along_m is not None and m2.distance_along_m is not None
        gap = max(o2.timestamp - o1.timestamp, 1.0)
        jump = m2.distance_along_m - m1.distance_along_m
        straight = haversine_m(o1.latitude, o1.longitude, o2.latitude, o2.longitude)
        gaps.append(gap)
        jumps.append(jump)
        speeds.append(jump / gap)
        straight_speeds.append(straight / gap)
        offs.append(float(case.off_route_between))
        forward_after = (
            len(case.next_run) >= SHORT_RUN_OBSERVATIONS and run_progress(case.next_run) > 0
        )
        if jump < -100:
            kinds["назад больше чем на 100 м"] += 1
        elif jump < -30:
            kinds["назад на 30–100 м"] += 1
        elif jump / gap > 30:
            kinds[
                "вперёд быстрее 30 м/с, дальше движение продолжается"
                if forward_after
                else "вперёд быстрее 30 м/с, дальше короткий прогон"
            ] += 1
        else:
            kinds["вперёд с правдоподобной скоростью"] += 1
    return [
        "## 1. Разрывы прогона в середине того же shape",
        "",
        f"- случаев: {len(cases)} ({len(cases) * 1000 / max(runs, 1):.0f} на 1000 прогонов)",
        f"- интервал между последним наблюдением прогона и первым следующего, с: {describe(gaps)}",
        f"- скачок прогресса, м: {describe(jumps)}",
        f"- скорость по прогрессу через разрыв, м/с: {describe(speeds)}",
        f"- скорость по прямой через разрыв, м/с: {describe(straight_speeds)}",
        f"- наблюдений `off_route` между прогонами: {describe(offs, 0)};"
        f" без `off_route`: {share(sum(o == 0 for o in offs), len(offs))}",
        "",
        "| Вид | Случаев | Доля |",
        "|---|---:|---:|",
        *[
            f"| {kind} | {count} | {share(count, len(cases))} |"
            for kind, count in kinds.most_common()
        ],
    ]


def shape_switch_section(cases: Sequence[Boundary], runs: int) -> list[str]:
    kinds: Counter[str] = Counter()
    lengths, progress, agree = [], [], []
    for case in cases:
        run = case.next_run
        lengths.append(float(len(run)))
        progress.append(run_progress(run))
        moving = [m.bearing_difference_deg for _, m in run if m.bearing_difference_deg is not None]
        if moving:
            agree.append(sum(b <= 45 for b in moving) / len(moving))
        back = (
            bool(case.run_after_next)
            and case.run_after_next[0][1].shape_id == case.before[1].shape_id
        )
        if len(run) < SHORT_RUN_OBSERVATIONS:
            kinds[
                "короткий прогон (< 5 наблюдений), затем возврат на прежний shape"
                if back
                else "короткий прогон (< 5 наблюдений)"
            ] += 1
        elif back and not case.next_boundary_terminal:
            kinds["прогон, затем возврат на прежний shape в середине"] += 1
        elif case.next_boundary_terminal:
            kinds["прогон до конечной нового shape (укороченный рейс)"] += 1
        elif not case.run_after_next:
            kinds["прогон до конца данных"] += 1
        else:
            kinds["прогон, затем снова разрыв в середине"] += 1
    return [
        "## 2. Смены shape в середине маршрута",
        "",
        f"- случаев: {len(cases)} ({len(cases) * 1000 / max(runs, 1):.0f} на 1000 прогонов)",
        f"- наблюдений в новом прогоне: {describe(lengths, 0)}",
        f"- прогресс нового прогона, м: {describe(progress, 0)}",
        f"- доля наблюдений нового прогона в движении с Δbearing ≤ 45°: {describe(agree, 2)}",
        "",
        "| Вид | Случаев | Доля |",
        "|---|---:|---:|",
        *[
            f"| {kind} | {count} | {share(count, len(cases))} |"
            for kind, count in kinds.most_common()
        ],
    ]


def repeated_section(observations: Sequence[Observation], stops: StopIndex) -> list[str]:
    series: Counter[tuple[str, bool]] = Counter()
    observations_in: Counter[tuple[str, bool]] = Counter()
    lengths: list[float] = []
    by_vehicle: dict[str, list[Observation]] = defaultdict(list)
    for o in observations:
        by_vehicle[o.vehicle_id].append(o)
    total = 0
    for track in by_vehicle.values():
        track.sort(key=lambda o: o.timestamp)
        total += len(track)
        start = 0
        for end in range(1, len(track) + 1):
            if end < len(track) and (track[end].latitude, track[end].longitude) == (
                track[start].latitude,
                track[start].longitude,
            ):
                continue
            run = track[start:end]
            start = end
            if len(run) < 3:
                continue
            moving = all((o.speed or 0) > 0 for o in run[1:])
            kind = "speed > 0" if moving else "speed = 0 хотя бы раз"
            near = stops.near(run[0].latitude, run[0].longitude)
            series[(kind, near)] += 1
            observations_in[(kind, near)] += len(run)
            if moving:
                lengths.append(float(len(run)))
    rows = [
        f"| {kind} | {'у остановки' if near else 'между остановками'} | {count} |"
        f" {share(observations_in[(kind, near)], total)} |"
        for (kind, near), count in sorted(series.items())
    ]
    return [
        "## 3. Повтор координат (серии ≥ 3 наблюдений с одинаковыми координатами)",
        "",
        f"`speed > 0` — у всех наблюдений серии после первого; «у остановки» — первая точка"
        f" серии ближе {NEAR_STOP_M:.0f} м к остановке из stops.txt.",
        "",
        "| Серия | Где | Серий | Доля наблюдений выборки |",
        "|---|---|---:|---:|",
        *rows,
        "",
        f"- длина серии со `speed > 0`, наблюдений: {describe(lengths, 0)}",
    ]


def write_examples(
    path: Path, cases: Sequence[Boundary], tracks: dict[str, list[Pair]], window_s: float = 180
) -> None:
    """Up to EXAMPLES_PER_KIND cases of each kind, spread over the sample, with the vehicle's
    observations `window_s` before and after the boundary."""
    by_kind: dict[str, list[Boundary]] = defaultdict(list)
    for case in cases:
        by_kind[case.kind].append(case)
    with path.open("w", newline="", encoding="utf-8") as output:
        writer = csv.writer(output)
        writer.writerow(
            [
                "case",
                "kind",
                "vehicle_id",
                "route_id",
                "point",
                "timestamp",
                "latitude",
                "longitude",
                "status",
                "run_id",
                "shape_id",
                "distance_along_m",
                "speed",
                "bearing",
            ]
        )
        for kind, found in sorted(by_kind.items()):
            step = max(1, len(found) // EXAMPLES_PER_KIND)
            for number, case in enumerate(found[::step][:EXAMPLES_PER_KIND]):
                start, end = case.before[0].timestamp - window_s, case.after[0].timestamp + window_s
                for o, m in tracks[case.before[1].track_id]:
                    if not start <= o.timestamp <= end:
                        continue
                    point = (
                        "before"
                        if m.event_id == case.before[1].event_id
                        else "after"
                        if m.event_id == case.after[1].event_id
                        else "track"
                    )
                    writer.writerow(
                        [
                            f"{kind}-{number:02d}",
                            kind,
                            case.vehicle_id,
                            o.route_id,
                            point,
                            int(o.timestamp),
                            o.latitude,
                            o.longitude,
                            m.status.value,
                            None if m.run_id is None else m.run_id[:8],
                            m.shape_id,
                            None if m.distance_along_m is None else round(m.distance_along_m, 1),
                            o.speed,
                            o.bearing,
                        ]
                    )


def analyze(sample: Path, parameters: MatchingParameters, examples: Path | None) -> str:
    tracks, observations = match_sample(sample, parameters)
    runs = len({m.run_id for pairs in tracks.values() for _, m in pairs if m.run_id is not None})
    found = boundaries(tracks, parameters)
    if examples is not None:
        write_examples(examples, found, tracks)
    stops = StopIndex(load_stops(sample / "feed.zip"), NEAR_STOP_M)
    return "\n".join(
        [
            f"# Edge cases report: {sample.name}",
            "",
            f"Прогонов: {runs}; наблюдений: {len(observations)}",
            "",
            *same_shape_section([c for c in found if c.kind == "same_shape"], runs),
            "",
            *shape_switch_section([c for c in found if c.kind == "shape_switch"], runs),
            "",
            *repeated_section(observations, stops),
            "",
        ]
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("sample", type=Path)
    parser.add_argument("--examples", type=Path, help="CSV with example cases")
    arguments = parser.parse_args()
    report = analyze(arguments.sample, matching_parameters_from_env(), arguments.examples)
    (arguments.sample / "edge-cases-report.md").write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
