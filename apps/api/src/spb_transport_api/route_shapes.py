import csv
from collections import defaultdict
from functools import lru_cache
from io import TextIOWrapper
from pathlib import Path
from zipfile import ZipFile


@lru_cache(maxsize=1)
def read_route_shapes(path: Path, modified_ns: int) -> dict[str, list[list[tuple[float, float]]]]:
    """Index distinct trip shapes in a static feed. Its modification time versions the cache."""
    by_route: dict[str, set[str]] = defaultdict(set)
    points: dict[str, list[tuple[int, float, float]]] = {}
    with ZipFile(path) as archive:
        with archive.open("trips.txt") as raw:
            for row in csv.DictReader(TextIOWrapper(raw, encoding="utf-8-sig")):
                if shape := row.get("shape_id"):
                    by_route[row["route_id"]].add(shape)
                    points.setdefault(shape, [])
        with archive.open("shapes.txt") as raw:
            for row in csv.DictReader(TextIOWrapper(raw, encoding="utf-8-sig")):
                if (line := points.get(row["shape_id"])) is not None:
                    line.append(
                        (
                            int(row["shape_pt_sequence"]),
                            float(row["shape_pt_lon"]),
                            float(row["shape_pt_lat"]),
                        )
                    )
    lines = {
        shape: [(lon, lat) for _, lon, lat in sorted(line)]
        for shape, line in points.items()
        if len(line) >= 2
    }
    return {
        route: [lines[s] for s in sorted(shapes) if s in lines]
        for route, shapes in by_route.items()
    }
