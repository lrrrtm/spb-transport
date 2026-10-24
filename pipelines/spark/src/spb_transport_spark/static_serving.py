"""Batch job: publish route and stop metadata of one static GTFS version to Cassandra (T11).

Input: `HDFS_STATIC_PROCESSED_PATH/version=<sha256>/{routes,stops,trips,shapes,stop_times}`.
Output: Cassandra `routes_by_feed`, `stops_by_route` and finally `static_feed_current`, which
switches the API to the new version only after both tables are written. Rows are keyed by
static feed version, so republishing a version is idempotent and older versions stay readable.

Stops of a route are the dominant stop pattern of each of its shapes, projected onto the shape
exactly as in T10 (`stop_patterns.py`), so the API and the stop events agree on stop order and
distances.
"""

from __future__ import annotations

import argparse
import logging
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

from spb_transport_spark.cassandra_sink import (
    STATIC_TABLES,
    configure_cassandra,
    write_static_table,
)
from spb_transport_spark.config import (
    CassandraSettings,
    required,
    required_finite_float,
)
from spb_transport_spark.route_match_batch import static_version_argument, unique_directions
from spb_transport_spark.stop_events_batch import load_patterns
from spb_transport_spark.stop_patterns import StopPattern

if TYPE_CHECKING:
    from pyspark.sql import SparkSession

logger = logging.getLogger(__name__)

FEED_NAME = "gtfs"


@dataclass(frozen=True)
class StaticServingSettings:
    static_processed_path: str
    stop_max_offset_m: float
    cassandra: CassandraSettings

    @classmethod
    def from_env(cls) -> StaticServingSettings:
        static_processed_path = required("HDFS_STATIC_PROCESSED_PATH").rstrip("/")
        stop_max_offset_m = required_finite_float("STOP_EVENT_STOP_MAX_OFFSET_M")
        if stop_max_offset_m <= 0:
            raise ValueError("STOP_EVENT_STOP_MAX_OFFSET_M must be positive")
        return cls(
            static_processed_path=static_processed_path,
            stop_max_offset_m=stop_max_offset_m,
            cassandra=CassandraSettings.from_env(),
        )


@dataclass(frozen=True)
class StopInfo:
    stop_code: str | None
    stop_name: str | None
    stop_lat: float
    stop_lon: float


def stop_rows(
    static_version: str,
    route_shapes: Iterable[tuple[str, str]],
    shape_directions: Iterable[tuple[str, int | None]],
    patterns: Mapping[str, StopPattern],
    stops: Mapping[str, StopInfo],
) -> list[tuple[Any, ...]]:
    """`stops_by_route` rows: every stop of the pattern of each route shape, in pattern order.

    Unprojected stops keep their position with `distance_along_m = null`; a stop missing from
    `stops` (broken reference) is skipped.
    """
    directions = unique_directions(shape_directions)
    rows: list[tuple[Any, ...]] = []
    for route_id, shape_id in sorted(set(route_shapes)):
        pattern = patterns.get(shape_id)
        if pattern is None:
            continue
        projected = {stop.stop_index: stop for stop in pattern.stops}
        unprojected = iter(pattern.unprojected_stop_ids)
        for index in range(len(pattern.stops) + len(pattern.unprojected_stop_ids)):
            placed = projected.get(index)
            stop_id = next(unprojected) if placed is None else placed.stop_id
            info = stops.get(stop_id)
            if info is None:
                continue
            rows.append(
                (
                    static_version,
                    route_id,
                    shape_id,
                    index,
                    directions.get(shape_id),
                    pattern.pattern_id,
                    stop_id,
                    info.stop_code,
                    info.stop_name,
                    info.stop_lat,
                    info.stop_lon,
                    None if placed is None else placed.distance_along_m,
                )
            )
    return rows


def publish(
    spark: SparkSession, settings: StaticServingSettings, static_version: str
) -> dict[str, int]:
    from pyspark.sql import functions as F
    from pyspark.sql.types import (
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
        TimestampType,
    )

    routes_table, stops_table, current_table = STATIC_TABLES
    static_path = f"{settings.static_processed_path}/version={static_version}"
    routes = (
        spark.read.parquet(f"{static_path}/routes")
        .withColumn("static_feed_version", F.lit(static_version))
        .select(*routes_table.columns)
    )
    trips = (
        spark.read.parquet(f"{static_path}/trips")
        .filter(F.col("shape_id").isNotNull())
        .select("route_id", "shape_id", "direction_id")
        .distinct()
        .collect()
    )
    stops = {
        r.stop_id: StopInfo(r.stop_code, r.stop_name, r.stop_lat, r.stop_lon)
        for r in spark.read.parquet(f"{static_path}/stops").collect()
    }
    patterns = load_patterns(
        spark, settings.static_processed_path, static_version, settings.stop_max_offset_m
    )
    rows = stop_rows(
        static_version,
        [(r.route_id, r.shape_id) for r in trips],
        [(r.shape_id, r.direction_id) for r in trips],
        patterns,
        stops,
    )
    stops_schema = StructType(
        [
            StructField(name, kind)
            for name, kind in zip(
                stops_table.columns,
                (
                    StringType(),
                    StringType(),
                    StringType(),
                    IntegerType(),
                    IntegerType(),
                    StringType(),
                    StringType(),
                    StringType(),
                    StringType(),
                    DoubleType(),
                    DoubleType(),
                    DoubleType(),
                ),
                strict=True,
            )
        ]
    )
    current_schema = StructType(
        [
            StructField("feed", StringType()),
            StructField("static_feed_version", StringType()),
            StructField("published_at", TimestampType()),
        ]
    )
    write_static_table(routes, settings.cassandra, routes_table)
    write_static_table(spark.createDataFrame(rows, stops_schema), settings.cassandra, stops_table)
    write_static_table(
        spark.createDataFrame([(FEED_NAME, static_version, datetime.now(UTC))], current_schema),
        settings.cassandra,
        current_table,
    )
    return {
        "routes": routes.count(),
        "route_stops": len(rows),
        "routes_with_stops": len({row[1] for row in rows}),
    }


def main() -> None:
    from pyspark.sql import SparkSession

    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Publish static GTFS metadata to Cassandra")
    parser.add_argument("--static-version", required=True, type=static_version_argument)
    arguments = parser.parse_args()
    settings = StaticServingSettings.from_env()
    spark = (
        configure_cassandra(SparkSession.builder.appName("static-gtfs-serving"), settings.cassandra)
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        counters = publish(spark, settings, arguments.static_version)
    finally:
        spark.stop()
    logger.info(
        "static metadata published: static_version=%s %s",
        arguments.static_version,
        " ".join(f"{name}={count}" for name, count in sorted(counters.items())),
    )


if __name__ == "__main__":
    main()
