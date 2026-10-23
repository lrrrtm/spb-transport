"""Batch job: processed vehicle positions of one day → silver vehicle_route_matches.

Input:
- `HDFS_POSITION_PROCESSED_PATH/event_date=<date>` written by `position_stream.py`;
- `HDFS_STATIC_PROCESSED_PATH/version=<sha256>/{trips,shapes}` written by `static_gtfs.py`.

Output: `HDFS_ROUTE_MATCHES_PATH/algorithm_version=<v>/event_date=<date>` (Parquet). The job
overwrites exactly that partition, so a rerun for the same day and algorithm is idempotent.
"""

from __future__ import annotations

import argparse
import logging
import re
from collections import defaultdict
from collections.abc import Iterable, Iterator
from datetime import UTC, date, datetime
from typing import TYPE_CHECKING, Any

from spb_transport_spark.config import RouteMatchingSettings
from spb_transport_spark.route_matching import (
    ALGORITHM_VERSION,
    MatchingParameters,
    Observation,
    RouteMatcher,
)
from spb_transport_spark.shape_geometry import ShapeGeometry

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)

SHAPE_INDEX_CELL_SIZE_M = 100.0

OUTPUT_COLUMNS = (
    ("event_id", "string"),
    ("vehicle_id", "string"),
    ("route_id", "string"),
    ("source_timestamp", "timestamp"),
    ("latitude", "double"),
    ("longitude", "double"),
    ("bearing", "double"),
    ("speed", "double"),
    ("track_id", "string"),
    ("run_id", "string"),
    ("match_status", "string"),
    ("confidence", "double"),
    ("shape_id", "string"),
    ("direction_id", "int"),
    ("segment_index", "int"),
    ("distance_along_m", "double"),
    ("shape_length_m", "double"),
    ("offset_m", "double"),
    ("bearing_difference_deg", "double"),
    ("static_feed_version", "string"),
    ("algorithm_version", "string"),
)


def output_schema() -> StructType:
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    types = {
        "string": StringType(),
        "timestamp": TimestampType(),
        "double": DoubleType(),
        "int": IntegerType(),
    }
    return StructType([StructField(name, types[kind]) for name, kind in OUTPUT_COLUMNS])


def build_shapes_by_route(
    shape_points: Iterable[tuple[str, int, float, float]],
    route_shapes: Iterable[tuple[str, str]],
) -> dict[str, list[ShapeGeometry]]:
    """Build shape geometries per route from static rows; shapes with < 2 points are skipped."""
    points: dict[str, list[tuple[int, float, float]]] = defaultdict(list)
    for shape_id, sequence, lat, lon in shape_points:
        points[shape_id].append((sequence, lat, lon))
    geometries = {
        shape_id: ShapeGeometry(
            shape_id,
            [(lat, lon) for _, lat, lon in sorted(rows)],
            cell_size_m=SHAPE_INDEX_CELL_SIZE_M,
        )
        for shape_id, rows in points.items()
        if len(rows) >= 2
    }
    by_route: dict[str, list[ShapeGeometry]] = defaultdict(list)
    for route_id, shape_id in sorted(set(route_shapes)):
        if shape_id in geometries:
            by_route[route_id].append(geometries[shape_id])
    return dict(by_route)


def unique_directions(shape_directions: Iterable[tuple[str, int | None]]) -> dict[str, int]:
    """direction_id of a shape when all its trips agree; otherwise the shape has no direction."""
    seen: dict[str, set[int | None]] = defaultdict(set)
    for shape_id, direction_id in shape_directions:
        seen[shape_id].add(direction_id)
    directions: dict[str, int] = {}
    for shape_id, values in seen.items():
        [only] = values if len(values) == 1 else [None]
        if only is not None:
            directions[shape_id] = only
    return directions


def _as_utc_epoch(value: datetime) -> float:
    return (value if value.tzinfo else value.replace(tzinfo=UTC)).timestamp()


_MATCHER_CACHE: dict[str, RouteMatcher] = {}


def match_partition(
    vehicles: Iterable[tuple[Any, Iterable[Any]]],
    *,
    static_version: str,
    static_rows: Any,
    parameters: MatchingParameters,
) -> Iterator[tuple[Any, ...]]:
    """Run the matcher for every vehicle of a Spark partition (executor side)."""
    shape_points, route_shapes, shape_directions = static_rows.value
    matcher = _MATCHER_CACHE.get(static_version)
    if matcher is None:
        matcher = RouteMatcher(build_shapes_by_route(shape_points, route_shapes), parameters)
        _MATCHER_CACHE.clear()
        _MATCHER_CACHE[static_version] = matcher
    directions = unique_directions(shape_directions)
    for _, rows in vehicles:
        by_event: dict[str, Any] = {}
        observations = []
        for row in rows:
            by_event[row.event_id] = row
            observations.append(
                Observation(
                    event_id=row.event_id,
                    vehicle_id=row.vehicle_id,
                    route_id=row.route_id,
                    timestamp=_as_utc_epoch(row.source_timestamp),
                    latitude=row.latitude,
                    longitude=row.longitude,
                    bearing=row.bearing,
                    speed=row.speed,
                )
            )
        for match in matcher.match_vehicle(observations):
            row = by_event[match.event_id]
            yield (
                row.event_id,
                row.vehicle_id,
                row.route_id,
                row.source_timestamp,
                row.latitude,
                row.longitude,
                row.bearing,
                row.speed,
                match.track_id,
                match.run_id,
                match.status.value,
                match.confidence,
                match.shape_id,
                None if match.shape_id is None else directions.get(match.shape_id),
                match.segment_index,
                match.distance_along_m,
                match.shape_length_m,
                match.offset_m,
                match.bearing_difference_deg,
                static_version,
                ALGORITHM_VERSION,
            )


def read_positions(spark: SparkSession, positions_path: str, event_date: date) -> DataFrame:
    from pyspark.sql import functions as F

    return (
        spark.read.parquet(positions_path)
        .filter(F.col("event_date") == F.lit(event_date.isoformat()).cast("date"))
        .select(
            "event_id",
            "vehicle_id",
            "route_id",
            "source_timestamp",
            "latitude",
            "longitude",
            "bearing",
            "speed",
        )
        # Streaming deduplication is bounded by its watermark; a late replay may still be here.
        .dropDuplicates(["vehicle_id", "source_timestamp"])
    )


def shuffle_partitions(spark: SparkSession) -> int:
    return int(str(spark.conf.get("spark.sql.shuffle.partitions", "200")))


def match_positions(
    spark: SparkSession,
    positions: DataFrame,
    static_version: str,
    static_root: str,
    parameters: MatchingParameters,
) -> DataFrame:
    from pyspark.sql import functions as F

    static_path = f"{static_root}/version={static_version}"
    shape_points = [
        (r.shape_id, r.shape_pt_sequence, r.shape_pt_lat, r.shape_pt_lon)
        for r in spark.read.parquet(f"{static_path}/shapes").collect()
    ]
    trips = (
        spark.read.parquet(f"{static_path}/trips")
        .filter(F.col("shape_id").isNotNull())
        .select("route_id", "shape_id", "direction_id")
        .distinct()
        .collect()
    )
    route_shapes = [(r.route_id, r.shape_id) for r in trips]
    shape_directions = [(r.shape_id, r.direction_id) for r in trips]
    static_rows = spark.sparkContext.broadcast((shape_points, route_shapes, shape_directions))

    def run(vehicles: Iterable[tuple[Any, Iterable[Any]]]) -> Iterable[tuple[Any, ...]]:
        return match_partition(
            vehicles,
            static_version=static_version,
            static_rows=static_rows,
            parameters=parameters,
        )

    matched = (
        positions.rdd.map(lambda row: (row.vehicle_id, row))
        .groupByKey(numPartitions=shuffle_partitions(spark))
        .mapPartitions(run)
    )
    return spark.createDataFrame(matched, output_schema())


def static_version_argument(value: str) -> str:
    if re.fullmatch(r"[a-f0-9]{64}", value) is None:
        raise argparse.ArgumentTypeError("static version must be a lowercase SHA-256 hex digest")
    return value


def run_job(
    settings: RouteMatchingSettings, event_date: date, static_version: str
) -> dict[str, int]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder.appName("route-shape-matching")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        positions = read_positions(spark, settings.positions_path, event_date)
        matches = match_positions(
            spark,
            positions,
            static_version,
            settings.static_processed_path,
            settings.parameters,
        ).withColumn("event_date", F.lit(event_date.isoformat()).cast("date"))
        matches.cache()
        (
            matches.write.mode("overwrite")
            .partitionBy("algorithm_version", "event_date")
            .parquet(settings.matches_path)
        )
        counters = {
            row.match_status: row["count"]
            for row in matches.groupBy("match_status").count().collect()
        }
        matches.unpersist()
        return counters
    finally:
        spark.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Match one day of vehicle positions to shapes")
    parser.add_argument("--event-date", required=True, type=date.fromisoformat)
    parser.add_argument("--static-version", required=True, type=static_version_argument)
    arguments = parser.parse_args()
    counters = run_job(
        RouteMatchingSettings.from_env(), arguments.event_date, arguments.static_version
    )
    logger.info(
        "route matching completed: event_date=%s static_version=%s algorithm_version=%s %s",
        arguments.event_date,
        arguments.static_version,
        ALGORITHM_VERSION,
        " ".join(f"{status}={count}" for status, count in sorted(counters.items())),
    )


if __name__ == "__main__":
    main()
