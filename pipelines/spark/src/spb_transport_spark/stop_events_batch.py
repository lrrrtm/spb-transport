"""Batch job: vehicle route matches of one day → stop arrivals and segment traversals (T10).

Input:
- `HDFS_ROUTE_MATCHES_PATH/algorithm_version=<route matcher>/event_date=<date>` (T09);
- `HDFS_STATIC_PROCESSED_PATH/version=<sha256>/{trips,shapes,stops,stop_times}`.

Output (Parquet, dynamic overwrite of exactly the written partitions):
- `HDFS_STOP_PATTERNS_PATH/algorithm_version=<v>/static_feed_version=<sha256>/` — projected
  dominant stop pattern per shape, including unprojected stops for diagnostics;
- `HDFS_STOP_ARRIVALS_PATH/algorithm_version=<v>/event_date=<date>/`;
- `HDFS_SEGMENT_TRAVERSALS_PATH/algorithm_version=<v>/event_date=<date>/`.

Only `matched` observations are used: low-confidence matches cannot place a vehicle at a stop.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Iterable, Iterator
from datetime import UTC, date, datetime
from functools import partial
from typing import TYPE_CHECKING, Any

from spb_transport_spark.config import StopEventSettings
from spb_transport_spark.route_match_batch import (
    as_utc_epoch,
    build_shape_geometries,
    shuffle_partitions,
    static_version_argument,
)
from spb_transport_spark.route_matching import ALGORITHM_VERSION as ROUTE_MATCH_VERSION
from spb_transport_spark.route_matching import MatchStatus
from spb_transport_spark.stop_events import (
    ALGORITHM_VERSION,
    RunPoint,
    StopEventParameters,
    stop_events,
)
from spb_transport_spark.stop_patterns import StopPattern, build_stop_patterns

if TYPE_CHECKING:
    from pyspark.sql import DataFrame, SparkSession
    from pyspark.sql.types import StructType

logger = logging.getLogger(__name__)

RUN_COLUMNS = (
    ("vehicle_id", "string"),
    ("route_id", "string"),
    ("shape_id", "string"),
    ("direction_id", "int"),
    ("run_id", "string"),
    ("track_id", "string"),
    ("pattern_id", "string"),
)

ARRIVAL_COLUMNS = (
    ("event_id", "string"),
    *RUN_COLUMNS,
    ("stop_id", "string"),
    ("stop_index", "int"),
    ("stop_distance_m", "double"),
    ("arrival_time", "timestamp"),
    ("passage_time", "timestamp"),
    ("departure_time", "timestamp"),
    ("dwell_s", "double"),
    ("stopped", "boolean"),
    ("observations_in_zone", "int"),
    ("method", "string"),
    ("crossing_gap_s", "double"),
    ("confidence", "double"),
    ("static_feed_version", "string"),
    ("route_match_algorithm_version", "string"),
    ("algorithm_version", "string"),
)

TRAVERSAL_COLUMNS = (
    ("event_id", "string"),
    *RUN_COLUMNS,
    ("from_stop_id", "string"),
    ("to_stop_id", "string"),
    ("from_stop_index", "int"),
    ("to_stop_index", "int"),
    ("segment_length_m", "double"),
    ("start_time", "timestamp"),
    ("end_time", "timestamp"),
    ("departure_time", "timestamp"),
    ("arrival_time", "timestamp"),
    ("travel_time_s", "double"),
    ("running_time_s", "double"),
    ("speed_mps", "double"),
    ("observations", "int"),
    ("max_gap_s", "double"),
    ("confidence", "double"),
    ("static_feed_version", "string"),
    ("route_match_algorithm_version", "string"),
    ("algorithm_version", "string"),
)

PATTERN_COLUMNS = (
    ("shape_id", "string"),
    ("pattern_id", "string"),
    ("trip_count", "int"),
    ("trip_share", "double"),
    ("stop_index", "int"),
    ("stop_id", "string"),
    ("projected", "boolean"),
    ("distance_along_m", "double"),
    ("offset_m", "double"),
)


def schema(columns: Iterable[tuple[str, str]]) -> StructType:
    from pyspark.sql.types import (
        BooleanType,
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
        "boolean": BooleanType(),
    }
    return StructType([StructField(name, types[kind]) for name, kind in columns])


def pattern_rows(patterns: Iterable[StopPattern]) -> list[tuple[Any, ...]]:
    """One row per stop of each pattern; unprojected stops keep their index with no distance."""
    rows: list[tuple[Any, ...]] = []
    for pattern in sorted(patterns, key=lambda p: p.shape_id):
        projected = {stop.stop_index: stop for stop in pattern.stops}
        total = len(pattern.stops) + len(pattern.unprojected_stop_ids)
        unprojected = iter(pattern.unprojected_stop_ids)
        for index in range(total):
            stop = projected.get(index)
            rows.append(
                (
                    pattern.shape_id,
                    pattern.pattern_id,
                    pattern.trip_count,
                    pattern.trip_share,
                    index,
                    next(unprojected) if stop is None else stop.stop_id,
                    stop is not None,
                    None if stop is None else stop.distance_along_m,
                    None if stop is None else stop.offset_m,
                )
            )
    return rows


def _time(value: float | None) -> datetime | None:
    return None if value is None else datetime.fromtimestamp(value, UTC)


def events_for_run(
    rows: Iterable[Any],
    patterns: dict[str, StopPattern],
    parameters: StopEventParameters,
    static_version: str,
) -> Iterator[tuple[str, tuple[Any, ...]]]:
    """Tagged arrival ("A") and traversal ("T") rows of one matched run (executor side)."""
    rows = list(rows)
    first = rows[0]
    pattern = patterns.get(first.shape_id)
    if pattern is None:
        return
    points = [
        RunPoint(
            row.event_id, as_utc_epoch(row.source_timestamp), row.distance_along_m, row.confidence
        )
        for row in rows
    ]
    arrivals, traversals = stop_events(first.run_id, pattern, points, parameters)
    run = (
        first.vehicle_id,
        first.route_id,
        first.shape_id,
        first.direction_id,
        first.run_id,
        first.track_id,
        pattern.pattern_id,
    )
    lineage = (static_version, ROUTE_MATCH_VERSION, ALGORITHM_VERSION)
    for a in arrivals:
        yield (
            "A",
            (
                a.event_id,
                *run,
                a.stop_id,
                a.stop_index,
                a.stop_distance_m,
                _time(a.arrival_time),
                _time(a.passage_time),
                _time(a.departure_time),
                a.dwell_s,
                a.stopped,
                a.observations_in_zone,
                a.method,
                a.crossing_gap_s,
                a.confidence,
                *lineage,
            ),
        )
    for t in traversals:
        yield (
            "T",
            (
                t.event_id,
                *run,
                t.from_stop_id,
                t.to_stop_id,
                t.from_stop_index,
                t.to_stop_index,
                t.segment_length_m,
                _time(t.start_time),
                _time(t.end_time),
                _time(t.departure_time),
                _time(t.arrival_time),
                t.travel_time_s,
                t.running_time_s,
                t.speed_mps,
                t.observations,
                t.max_gap_s,
                t.confidence,
                *lineage,
            ),
        )


def _has_tag(tag: str, item: tuple[str, tuple[Any, ...]]) -> bool:
    return item[0] == tag


def _payload(item: tuple[str, tuple[Any, ...]]) -> tuple[Any, ...]:
    return item[1]


def load_patterns(
    spark: SparkSession, static_root: str, static_version: str, stop_max_offset_m: float
) -> dict[str, StopPattern]:
    from pyspark.sql import functions as F

    static_path = f"{static_root}/version={static_version}"
    geometries = build_shape_geometries(
        (r.shape_id, r.shape_pt_sequence, r.shape_pt_lat, r.shape_pt_lon)
        for r in spark.read.parquet(f"{static_path}/shapes").collect()
    )
    stop_coordinates = {
        r.stop_id: (r.stop_lat, r.stop_lon)
        for r in spark.read.parquet(f"{static_path}/stops").collect()
    }
    trip_stops = (
        spark.read.parquet(f"{static_path}/stop_times")
        .groupBy("trip_id")
        .agg(F.sort_array(F.collect_list(F.struct("stop_sequence", "stop_id"))).alias("ordered"))
        .select("trip_id", F.col("ordered.stop_id").alias("stop_ids"))
    )
    pattern_counts = (
        spark.read.parquet(f"{static_path}/trips")
        .filter(F.col("shape_id").isNotNull())
        .select("trip_id", "shape_id")
        .join(trip_stops, "trip_id")
        .groupBy("shape_id", "stop_ids")
        .count()
        .collect()
    )
    return build_stop_patterns(
        geometries,
        [(r.shape_id, r.stop_ids, r["count"]) for r in pattern_counts],
        stop_coordinates,
        stop_max_offset_m,
    )


def read_matched(
    spark: SparkSession, matches_root: str, event_date: date, version: str
) -> DataFrame:
    from pyspark.sql import functions as F

    return (
        spark.read.parquet(matches_root)
        .filter(
            (F.col("algorithm_version") == ROUTE_MATCH_VERSION)
            & (F.col("event_date") == F.lit(event_date.isoformat()).cast("date"))
            & (F.col("static_feed_version") == version)
            & (F.col("match_status") == MatchStatus.MATCHED.value)
            & F.col("run_id").isNotNull()
        )
        .select(
            "event_id",
            "vehicle_id",
            "route_id",
            "shape_id",
            "direction_id",
            "track_id",
            "run_id",
            "source_timestamp",
            "distance_along_m",
            "confidence",
        )
    )


def run_job(settings: StopEventSettings, event_date: date, static_version: str) -> dict[str, int]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        SparkSession.builder.appName("stop-arrival-segment-events")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        patterns = load_patterns(
            spark, settings.static_processed_path, static_version, settings.stop_max_offset_m
        )
        (
            spark.createDataFrame(pattern_rows(patterns.values()), schema(PATTERN_COLUMNS))
            .withColumn("algorithm_version", F.lit(ALGORITHM_VERSION))
            .withColumn("static_feed_version", F.lit(static_version))
            .write.mode("overwrite")
            .partitionBy("algorithm_version", "static_feed_version")
            .parquet(settings.stop_patterns_path)
        )

        broadcast = spark.sparkContext.broadcast(patterns)
        parameters = settings.parameters

        def per_run(item: tuple[Any, Iterable[Any]]) -> Iterator[tuple[str, tuple[Any, ...]]]:
            return events_for_run(item[1], broadcast.value, parameters, static_version)

        matched = read_matched(spark, settings.matches_path, event_date, static_version)
        tagged = (
            matched.rdd.map(lambda row: (row.run_id, row))
            .groupByKey(numPartitions=shuffle_partitions(spark))
            .flatMap(per_run)
            .cache()
        )
        partition = F.lit(event_date.isoformat()).cast("date")
        outputs = {
            "A": (ARRIVAL_COLUMNS, settings.stop_arrivals_path, ("passage_time", "departure_time")),
            "T": (
                TRAVERSAL_COLUMNS,
                settings.segment_traversals_path,
                ("start_time", "departure_time"),
            ),
        }
        counters: dict[str, int] = {"runs": matched.select("run_id").distinct().count()}
        for tag, (columns, path, time_columns) in outputs.items():
            rows = tagged.filter(partial(_has_tag, tag)).map(_payload)
            frame = (
                spark.createDataFrame(rows, schema(columns))
                .withColumn(
                    "service_date",
                    F.to_date(F.from_utc_timestamp(F.coalesce(*time_columns), "Europe/Moscow")),
                )
                .withColumn("event_date", partition)
            )
            frame.write.mode("overwrite").partitionBy("algorithm_version", "event_date").parquet(
                path
            )
            written = spark.read.parquet(path).filter(
                (F.col("algorithm_version") == ALGORITHM_VERSION)
                & (F.col("event_date") == partition)
            )
            name = "stop_arrivals" if tag == "A" else "segment_traversals"
            counters[name] = written.count()
            if tag == "A":
                counters["stop_arrivals_interpolated"] = written.filter(
                    F.col("method") == "interpolated"
                ).count()
                counters["stop_arrivals_departure_only"] = written.filter(
                    F.col("method") == "departure_only"
                ).count()
                counters["stop_arrivals_stopped"] = written.filter(F.col("stopped")).count()
        tagged.unpersist()
        counters["patterns"] = len(patterns)
        counters["patterns_with_unprojected_stops"] = sum(
            1 for pattern in patterns.values() if pattern.unprojected_stop_ids
        )
        return counters
    finally:
        spark.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Derive stop arrivals and segment traversals")
    parser.add_argument("--event-date", required=True, type=date.fromisoformat)
    parser.add_argument("--static-version", required=True, type=static_version_argument)
    arguments = parser.parse_args()
    counters = run_job(StopEventSettings.from_env(), arguments.event_date, arguments.static_version)
    logger.info(
        "stop events completed: event_date=%s static_version=%s algorithm_version=%s %s",
        arguments.event_date,
        arguments.static_version,
        ALGORITHM_VERSION,
        " ".join(f"{name}={count}" for name, count in sorted(counters.items())),
    )


if __name__ == "__main__":
    main()
