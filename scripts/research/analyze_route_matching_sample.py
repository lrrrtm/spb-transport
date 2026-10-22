"""Measure the inputs of T09 route/shape matching on a collected research sample.

Reads a directory written by `collect_route_matching_sample.py` and prints a Markdown report:
route_id coverage in static GTFS, shape variants per route, distance from GPS to route shapes,
bearing agreement, ambiguity between shape variants, source timestamp cadence and progress
along the shape between consecutive observations. The report is the evidence for the
thresholds in `docs/research/route-matching.md`.

Usage:
    uv run python scripts/research/analyze_route_matching_sample.py \
        data/research/route-matching/<run>
"""

from __future__ import annotations

import argparse
import csv
import io
import json
import zipfile
from collections import Counter, defaultdict
from collections.abc import Iterable, Iterator, Sequence
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from spb_transport_collector.vehicle_feed import parse_vehicle_feed
from spb_transport_spark.shape_geometry import (
    ShapeGeometry,
    ShapeProjection,
    bearing_difference_deg,
)

SEARCH_RADIUS_M = 300.0
CELL_SIZE_M = 100.0
OFFSET_BUCKETS_M = (5, 10, 15, 20, 30, 50, 75, 100, 150, 200, 300)
QUANTILES = (0.5, 0.75, 0.9, 0.95, 0.99)
CONSECUTIVE_MAX_GAP_S = 120.0
MOVING_SPEED_THRESHOLDS = (0.5, 1.0, 2.0)


@dataclass(frozen=True)
class Observation:
    vehicle_id: str
    route_id: str
    source_timestamp: float
    latitude: float
    longitude: float
    bearing: float | None
    speed: float | None


def quantile(values: Sequence[float], q: float) -> float:
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    lower = int(position)
    upper = min(lower + 1, len(ordered) - 1)
    return ordered[lower] + (ordered[upper] - ordered[lower]) * (position - lower)


def describe(values: Sequence[float], digits: int = 1) -> str:
    if not values:
        return "нет данных"
    parts = [f"p{round(q * 100)}={quantile(values, q):.{digits}f}" for q in QUANTILES]
    return f"n={len(values)}, min={min(values):.{digits}f}, " + ", ".join(parts)


def share(count: int, total: int) -> str:
    return "—" if total == 0 else f"{count / total:.1%}"


def _read_csv(archive: zipfile.ZipFile, name: str) -> Iterator[dict[str, str]]:
    with archive.open(name) as raw:
        yield from csv.DictReader(io.TextIOWrapper(raw, encoding="utf-8-sig"))


def load_static(
    feed: Path,
) -> tuple[set[str], dict[str, set[str]], dict[str, list[tuple[float, float]]]]:
    with zipfile.ZipFile(feed) as archive:
        route_ids = {row["route_id"] for row in _read_csv(archive, "routes.txt")}
        shapes_by_route: dict[str, set[str]] = defaultdict(set)
        for row in _read_csv(archive, "trips.txt"):
            if row.get("shape_id"):
                shapes_by_route[row["route_id"]].add(row["shape_id"])
        points: dict[str, list[tuple[int, float, float]]] = defaultdict(list)
        for row in _read_csv(archive, "shapes.txt"):
            points[row["shape_id"]].append(
                (
                    int(row["shape_pt_sequence"]),
                    float(row["shape_pt_lat"]),
                    float(row["shape_pt_lon"]),
                )
            )
    shapes = {
        shape_id: [(lat, lon) for _, lat, lon in sorted(rows)] for shape_id, rows in points.items()
    }
    return route_ids, dict(shapes_by_route), shapes


def load_observations(directory: Path) -> tuple[int, int, list[Observation]]:
    polls = 0
    raw_count = 0
    unique: dict[tuple[str, float], Observation] = {}
    for path in sorted(directory.glob("*.pb")):
        polls += 1
        collected_at = datetime.strptime(path.stem, "%Y%m%dT%H%M%S%fZ").replace(tzinfo=UTC)
        for event in parse_vehicle_feed(path.read_bytes(), collected_at).events:
            raw_count += 1
            key = (event.vehicle_id, event.source_timestamp.timestamp())
            unique.setdefault(
                key,
                Observation(
                    vehicle_id=event.vehicle_id,
                    route_id=event.route_id,
                    source_timestamp=key[1],
                    latitude=event.latitude,
                    longitude=event.longitude,
                    bearing=event.bearing,
                    speed=event.speed,
                ),
            )
    return (
        polls,
        raw_count,
        sorted(unique.values(), key=lambda o: (o.vehicle_id, o.source_timestamp)),
    )


def offset_table(offsets: Sequence[float], total: int) -> list[str]:
    rows = ["| Радиус, м | Доля наблюдений с shape маршрута ближе радиуса |", "|---:|---:|"]
    rows += [f"| {r} | {share(sum(o <= r for o in offsets), total)} |" for r in OFFSET_BUCKETS_M]
    return rows


def pairs(observations: Iterable[Observation]) -> Iterator[tuple[Observation, Observation]]:
    previous: Observation | None = None
    for observation in observations:
        if previous is not None and previous.vehicle_id == observation.vehicle_id:
            yield previous, observation
        previous = observation


def analyze(sample: Path) -> str:
    manifest = json.loads((sample / "manifest.json").read_text(encoding="utf-8"))
    route_ids, shapes_by_route, shape_points = load_static(sample / "feed.zip")
    polls, raw_count, observations = load_observations(sample / "vehicle")

    geometries: dict[str, ShapeGeometry] = {}

    def candidates(observation: Observation) -> list[ShapeProjection]:
        found: list[ShapeProjection] = []
        for shape_id in sorted(shapes_by_route.get(observation.route_id, ())):
            if shape_id not in shape_points or len(shape_points[shape_id]) < 2:
                continue
            if shape_id not in geometries:
                geometries[shape_id] = ShapeGeometry(
                    shape_id, shape_points[shape_id], cell_size_m=CELL_SIZE_M
                )
            found += geometries[shape_id].candidates(
                observation.latitude, observation.longitude, SEARCH_RADIUS_M
            )
        return sorted(found, key=lambda projection: projection.offset_m)

    known_route = [o for o in observations if o.route_id in route_ids]
    with_shapes = [o for o in known_route if shapes_by_route.get(o.route_id)]
    best: dict[tuple[str, float], ShapeProjection] = {}
    all_candidates: dict[tuple[str, float], list[ShapeProjection]] = {}
    for observation in with_shapes:
        found = candidates(observation)
        key = (observation.vehicle_id, observation.source_timestamp)
        all_candidates[key] = found
        if found:
            best[key] = found[0]
    offsets = [projection.offset_m for projection in best.values()]

    speeds = [o.speed for o in observations if o.speed is not None]
    bearings = [o.bearing for o in observations if o.bearing is not None]
    lines = [
        f"# Research report: {sample.name}",
        "",
        f"- Период: {manifest['started_at']} — {manifest['finished_at']}",
        f"- Static GTFS: sha256 `{manifest['static']['sha256']}`,"
        f" {manifest['static']['bytes']} bytes",
        f"- Опросов `/vehicle`: {polls} (интервал {manifest['vehicle_poll_interval_seconds']} с,"
        f" ошибок {manifest['vehicle_polls']['failed']})",
        f"- Позиций в ответах: {raw_count}; уникальных `(vehicle_id, source_timestamp)`:"
        f" {len(observations)} (доля повторов {share(raw_count - len(observations), raw_count)})",
        f"- Машин: {len({o.vehicle_id for o in observations})};"
        f" route_id: {len({o.route_id for o in observations})}",
        "",
        "## 1. Покрытие static GTFS",
        "",
        f"- route_id найден в routes.txt: {share(len(known_route), len(observations))}",
        f"- у route_id есть хотя бы один shape: {share(len(with_shapes), len(observations))}",
        f"- найден кандидат в радиусе {SEARCH_RADIUS_M:.0f} м:"
        f" {share(len(best), len(with_shapes))}",
    ]
    variants = [len(shapes_by_route.get(r, ())) for r in {o.route_id for o in with_shapes}]
    variant_counts = Counter(variants)
    lines += [
        "",
        "## 2. Варианты shape на route_id (маршруты из выборки)",
        "",
        "| Число shape | Маршрутов |",
        "|---:|---:|",
        *[f"| {count} | {routes} |" for count, routes in sorted(variant_counts.items())],
        "",
        "## 3. Расстояние GPS до ближайшего shape своего маршрута",
        "",
        f"Отклонение, м: {describe(offsets)}",
        "",
        *offset_table(offsets, len(with_shapes)),
    ]

    moving_diffs: dict[float, list[float]] = {t: [] for t in MOVING_SPEED_THRESHOLDS}
    for observation in with_shapes:
        projection = best.get((observation.vehicle_id, observation.source_timestamp))
        if projection is None or observation.bearing is None or observation.speed is None:
            continue
        difference = bearing_difference_deg(observation.bearing, projection.segment_bearing_deg)
        for threshold in MOVING_SPEED_THRESHOLDS:
            if observation.speed >= threshold:
                moving_diffs[threshold].append(difference)
    lines += [
        "",
        "## 4. Поля speed и bearing",
        "",
        f"- speed присутствует: {share(len(speeds), len(observations))};"
        f" speed=0: {share(sum(s == 0 for s in speeds), len(speeds))}",
        f"- speed (единицы источника): {describe(speeds)}",
        f"- bearing присутствует: {share(len(bearings), len(observations))};"
        f" bearing=0: {share(sum(b == 0 for b in bearings), len(bearings))}",
        "",
        "Разница bearing и направления ближайшего сегмента, градусы:",
        "",
        *[
            f"- speed ≥ {threshold}: {describe(diffs)};"
            f" ≤45°: {share(sum(d <= 45 for d in diffs), len(diffs))},"
            f" ≥135°: {share(sum(d >= 135 for d in diffs), len(diffs))}"
            for threshold, diffs in moving_diffs.items()
        ],
    ]

    ambiguous = {5: 0, 10: 0, 20: 0}
    resolved_by_bearing = {5: 0, 10: 0, 20: 0}
    for observation in with_shapes:
        found = all_candidates.get((observation.vehicle_id, observation.source_timestamp), [])
        if len(found) < 2:
            continue
        for margin in ambiguous:
            close = [p for p in found if p.offset_m <= found[0].offset_m + margin]
            if len({(p.shape_id, round(p.distance_along_m)) for p in close}) < 2:
                continue
            ambiguous[margin] += 1
            if observation.bearing is not None and (observation.speed or 0) >= 1.0:
                aligned = [
                    p
                    for p in close
                    if bearing_difference_deg(observation.bearing, p.segment_bearing_deg) <= 45
                ]
                if len({p.shape_id for p in aligned}) == 1:
                    resolved_by_bearing[margin] += 1
    lines += [
        "",
        "## 5. Неоднозначность между вариантами shape",
        "",
        "| Запас по отклонению, м | Наблюдений с ≥2 близкими кандидатами |"
        " Из них однозначно по bearing (speed ≥ 1) |",
        "|---:|---:|---:|",
        *[
            f"| {margin} | {share(ambiguous[margin], len(best))}"
            f" | {share(resolved_by_bearing[margin], ambiguous[margin])} |"
            for margin in ambiguous
        ],
    ]

    gaps: list[float] = []
    progress: list[float] = []
    implied_speed: list[float] = []
    speed_ratio: list[float] = []
    route_changes = 0
    for first, second in pairs(observations):
        gap = second.source_timestamp - first.source_timestamp
        gaps.append(gap)
        if first.route_id != second.route_id:
            route_changes += 1
            continue
        if gap > CONSECUTIVE_MAX_GAP_S:
            continue
        a = best.get((first.vehicle_id, first.source_timestamp))
        b = best.get((second.vehicle_id, second.source_timestamp))
        if a is None or b is None or a.shape_id != b.shape_id:
            continue
        delta = b.distance_along_m - a.distance_along_m
        progress.append(delta)
        implied_speed.append(delta / gap)
        if second.speed and delta > 0:
            speed_ratio.append(second.speed / (delta / gap))
    lines += [
        "",
        "## 6. Частота обновления source_timestamp",
        "",
        f"- Интервал между соседними наблюдениями машины, с: {describe(gaps)}",
        *[
            f"- доля интервалов > {limit} с: {share(sum(g > limit for g in gaps), len(gaps))}"
            for limit in (30, 60, 120, 300, 900)
        ],
        f"- смена route_id у машины между соседними наблюдениями: {route_changes}",
        "",
        "## 7. Прогресс вдоль shape (соседние наблюдения, один shape,"
        f" интервал ≤ {CONSECUTIVE_MAX_GAP_S:.0f} с)",
        "",
        f"- Δ distance_along, м: {describe(progress)}",
        f"- отрицательный прогресс: {share(sum(d < 0 for d in progress), len(progress))};"
        f" < −20 м: {share(sum(d < -20 for d in progress), len(progress))};"
        f" < −50 м: {share(sum(d < -50 for d in progress), len(progress))}",
        f"- скорость по прогрессу, м/с: {describe(implied_speed)}",
        f"- отношение speed источника к скорости по прогрессу: {describe(speed_ratio, 2)}"
        " (≈1 — м/с, ≈3.6 — км/ч)",
    ]
    return "\n".join(lines) + "\n"


def main() -> None:
    parser = argparse.ArgumentParser(description="Analyze a T09 route matching research sample")
    parser.add_argument("sample", type=Path)
    arguments = parser.parse_args()
    report = analyze(arguments.sample)
    (arguments.sample / "report.md").write_text(report, encoding="utf-8")
    print(report)


if __name__ == "__main__":
    main()
