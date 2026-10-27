"""T14: transport headway, bunching, stationary, disappearance and GPS anomalies."""

from __future__ import annotations

import argparse
import logging
from datetime import date
from typing import TYPE_CHECKING

from spb_transport_spark.cassandra_sink import configure_cassandra, write_anomalies
from spb_transport_spark.config import AnomalySettings
from spb_transport_spark.route_matching import ALGORITHM_VERSION as MATCH_VERSION
from spb_transport_spark.stop_events import ALGORITHM_VERSION as STOP_EVENT_VERSION

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

logger = logging.getLogger(__name__)

ALGORITHM_VERSION = "transport-anomalies-v1"
EARTH_RADIUS_M = 6_371_000.0


def _events(
    frame: DataFrame,
    anomaly_type: str | Column,
    *,
    started_at: Column,
    ended_at: Column,
    related_vehicle_id: Column | None = None,
    stop_id: Column | None = None,
    duration_s: Column | None = None,
    distance_gap_m: Column | None = None,
    headway_s: Column | None = None,
) -> DataFrame:
    from pyspark.sql import functions as F

    null_string = F.lit(None).cast("string")
    null_double = F.lit(None).cast("double")
    result = frame.select(
        F.lit(anomaly_type).alias("anomaly_type")
        if isinstance(anomaly_type, str)
        else anomaly_type.alias("anomaly_type"),
        "static_feed_version",
        "route_id",
        "shape_id",
        "direction_id",
        "vehicle_id",
        (related_vehicle_id if related_vehicle_id is not None else null_string).alias(
            "related_vehicle_id"
        ),
        (stop_id if stop_id is not None else null_string).alias("stop_id"),
        started_at.alias("started_at"),
        ended_at.alias("ended_at"),
        (duration_s if duration_s is not None else null_double).cast("double").alias("duration_s"),
        (distance_gap_m if distance_gap_m is not None else null_double)
        .cast("double")
        .alias("distance_gap_m"),
        (headway_s if headway_s is not None else null_double).cast("double").alias("headway_s"),
        F.col("latitude").cast("double").alias("latitude")
        if "latitude" in frame.columns
        else null_double.alias("latitude"),
        F.col("longitude").cast("double").alias("longitude")
        if "longitude" in frame.columns
        else null_double.alias("longitude"),
    )
    identity = [
        "anomaly_type",
        "static_feed_version",
        "route_id",
        "shape_id",
        "vehicle_id",
        "related_vehicle_id",
        "stop_id",
        "started_at",
        "ended_at",
    ]
    return result.withColumn("anomaly_id", F.sha2(F.to_json(F.struct(*identity)), 256)).select(
        "anomaly_id", *result.columns
    )


def _matched(matches: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    return matches.filter(
        (F.col("match_status") == "matched")
        & (F.col("algorithm_version") == MATCH_VERSION)
        & F.col("run_id").isNotNull()
    ).withColumn("direction_id", F.coalesce("direction_id", F.lit(-1)))


def bunching_events(matches: DataFrame, settings: AnomalySettings) -> DataFrame:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    matched = _matched(matches).withColumn(
        "snapshot", (F.unix_timestamp("source_timestamp") / settings.snapshot_seconds).cast("long")
    )
    latest = matched.withColumn(
        "snapshot_rank",
        F.row_number().over(
            Window.partitionBy("route_id", "shape_id", "snapshot", "vehicle_id").orderBy(
                F.col("source_timestamp").desc()
            )
        ),
    ).filter("snapshot_rank = 1")
    ordered = (
        latest.withColumn(
            "related_vehicle_id",
            F.lag("vehicle_id").over(
                Window.partitionBy("route_id", "shape_id", "snapshot").orderBy(
                    "distance_along_m", "vehicle_id"
                )
            ),
        )
        .withColumn(
            "distance_gap_m",
            F.col("distance_along_m")
            - F.lag("distance_along_m").over(
                Window.partitionBy("route_id", "shape_id", "snapshot").orderBy(
                    "distance_along_m", "vehicle_id"
                )
            ),
        )
        .filter(
            F.col("related_vehicle_id").isNotNull()
            & (F.col("distance_gap_m") <= settings.bunching_distance_m)
        )
    )
    return _events(
        ordered,
        "bunching",
        started_at=F.col("source_timestamp"),
        ended_at=F.col("source_timestamp"),
        related_vehicle_id=F.col("related_vehicle_id"),
        distance_gap_m=F.col("distance_gap_m"),
    )


def headway_events(arrivals: DataFrame, settings: AnomalySettings) -> DataFrame:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    ordered = (
        arrivals.filter(
            (F.col("algorithm_version") == STOP_EVENT_VERSION) & F.col("passage_time").isNotNull()
        )
        .withColumn("direction_id", F.coalesce("direction_id", F.lit(-1)))
        .withColumn(
            "previous_vehicle_id",
            F.lag("vehicle_id").over(
                Window.partitionBy("route_id", "shape_id", "stop_id").orderBy("passage_time")
            ),
        )
        .withColumn(
            "previous_time",
            F.lag("passage_time").over(
                Window.partitionBy("route_id", "shape_id", "stop_id").orderBy("passage_time")
            ),
        )
        .withColumn(
            "headway_s",
            F.unix_timestamp("passage_time") - F.unix_timestamp("previous_time"),
        )
        .filter(F.col("previous_vehicle_id") != F.col("vehicle_id"))
        .withColumn(
            "anomaly_type",
            F.when(F.col("headway_s") < settings.short_headway_seconds, "short_headway").when(
                F.col("headway_s") > settings.long_headway_seconds, "long_headway"
            ),
        )
        .filter(F.col("anomaly_type").isNotNull())
    )
    return _events(
        ordered,
        F.col("anomaly_type"),
        started_at=F.col("previous_time"),
        ended_at=F.col("passage_time"),
        related_vehicle_id=F.col("previous_vehicle_id"),
        stop_id=F.col("stop_id"),
        duration_s=F.col("headway_s"),
        headway_s=F.col("headway_s"),
    )


def stationary_events(matches: DataFrame, settings: AnomalySettings) -> DataFrame:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    order = Window.partitionBy("vehicle_id", "run_id").orderBy("source_timestamp")
    sequenced = (
        _matched(matches)
        .withColumn("previous_time", F.lag("source_timestamp").over(order))
        .withColumn("previous_progress", F.lag("distance_along_m").over(order))
        .withColumn(
            "observation_gap_s",
            F.unix_timestamp("source_timestamp") - F.unix_timestamp("previous_time"),
        )
        .withColumn(
            "stationary_edge",
            (F.col("speed") <= settings.stationary_speed_mps)
            & (
                F.abs(F.col("distance_along_m") - F.col("previous_progress"))
                <= settings.stationary_progress_m
            )
            & (F.col("observation_gap_s") <= settings.max_observation_gap_seconds),
        )
        .withColumn(
            "stationary_group",
            F.sum(F.when(~F.coalesce("stationary_edge", F.lit(False)), 1).otherwise(0)).over(
                order.rowsBetween(Window.unboundedPreceding, Window.currentRow)
            ),
        )
        .filter("stationary_edge")
    )
    grouped = sequenced.groupBy(
        "static_feed_version",
        "route_id",
        "shape_id",
        "direction_id",
        "vehicle_id",
        "run_id",
        "stationary_group",
    ).agg(
        F.min("previous_time").alias("started_at"),
        F.max("source_timestamp").alias("ended_at"),
        F.avg("latitude").alias("latitude"),
        F.avg("longitude").alias("longitude"),
    )
    grouped = grouped.withColumn(
        "duration_s", F.unix_timestamp("ended_at") - F.unix_timestamp("started_at")
    ).filter(F.col("duration_s") >= settings.stationary_duration_seconds)
    return _events(
        grouped,
        "stationary",
        started_at=F.col("started_at"),
        ended_at=F.col("ended_at"),
        duration_s=F.col("duration_s"),
    )


def disappearance_events(matches: DataFrame, settings: AnomalySettings) -> DataFrame:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    matched = _matched(matches)
    latest = matched.withColumn(
        "rank",
        F.row_number().over(
            Window.partitionBy("vehicle_id").orderBy(F.col("source_timestamp").desc())
        ),
    ).filter("rank = 1")
    end = matches.agg(F.max("source_timestamp").alias("window_end"))
    missing = (
        latest.crossJoin(end)
        .withColumn(
            "duration_s",
            F.unix_timestamp("window_end") - F.unix_timestamp("source_timestamp"),
        )
        .filter(
            (F.col("duration_s") >= settings.disappearance_seconds)
            & ((F.col("shape_length_m") - F.col("distance_along_m")) > settings.terminal_zone_m)
        )
    )
    return _events(
        missing,
        "disappearance",
        started_at=F.col("source_timestamp"),
        ended_at=F.col("window_end"),
        duration_s=F.col("duration_s"),
    )


def gps_events(matches: DataFrame, settings: AnomalySettings) -> DataFrame:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    order = Window.partitionBy("vehicle_id").orderBy("source_timestamp")
    points = (
        matches.withColumn("direction_id", F.coalesce("direction_id", F.lit(-1)))
        .withColumn("previous_time", F.lag("source_timestamp").over(order))
        .withColumn("previous_latitude", F.lag("latitude").over(order))
        .withColumn("previous_longitude", F.lag("longitude").over(order))
    )
    lat1, lat2 = F.radians("previous_latitude"), F.radians("latitude")
    dlat = lat2 - lat1
    dlon = F.radians("longitude") - F.radians("previous_longitude")
    haversine = F.lit(2 * EARTH_RADIUS_M) * F.asin(
        F.sqrt(F.pow(F.sin(dlat / 2), 2) + F.cos(lat1) * F.cos(lat2) * F.pow(F.sin(dlon / 2), 2))
    )
    points = points.withColumn(
        "duration_s", F.unix_timestamp("source_timestamp") - F.unix_timestamp("previous_time")
    ).withColumn("implied_speed_mps", haversine / F.col("duration_s"))
    jumps = points.filter(
        (F.col("duration_s") > 0)
        & (F.col("duration_s") <= settings.max_observation_gap_seconds)
        & (F.col("implied_speed_mps") > settings.gps_jump_speed_mps)
    )
    offsets = points.filter(F.col("offset_m") > settings.shape_offset_m)
    return _events(
        jumps,
        "gps_jump",
        started_at=F.col("previous_time"),
        ended_at=F.col("source_timestamp"),
        duration_s=F.col("duration_s"),
        distance_gap_m=haversine,
    ).unionByName(
        _events(
            offsets,
            "shape_offset",
            started_at=F.col("source_timestamp"),
            ended_at=F.col("source_timestamp"),
            distance_gap_m=F.col("offset_m"),
        )
    )


def build_anomalies(
    matches: DataFrame, arrivals: DataFrame, settings: AnomalySettings
) -> DataFrame:
    from functools import reduce

    frames = (
        bunching_events(matches, settings),
        headway_events(arrivals, settings),
        stationary_events(matches, settings),
        disappearance_events(matches, settings),
        gps_events(matches, settings),
    )
    return reduce(lambda left, right: left.unionByName(right), frames)


def run_job(settings: AnomalySettings, event_date: date) -> dict[str, int]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    spark = (
        configure_cassandra(SparkSession.builder.appName("transport-anomalies"), settings.cassandra)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        day = F.lit(event_date.isoformat()).cast("date")
        matches = spark.read.parquet(settings.matches_path).filter(F.col("event_date") == day)
        arrivals = spark.read.parquet(settings.stop_arrivals_path).filter(
            F.col("event_date") == day
        )
        anomalies = (
            build_anomalies(matches, arrivals, settings)
            .withColumn("event_date", day)
            .withColumn("algorithm_version", F.lit(ALGORITHM_VERSION))
            .cache()
        )
        counters = {
            row.anomaly_type: int(row["count"])
            for row in anomalies.groupBy("anomaly_type").count().collect()
        }
        anomalies.write.mode("overwrite").partitionBy("algorithm_version", "event_date").parquet(
            settings.anomalies_path
        )
        write_anomalies(anomalies, settings.cassandra)
        anomalies.unpersist()
        return counters
    finally:
        spark.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Detect transport movement anomalies")
    parser.add_argument("--event-date", required=True, type=date.fromisoformat)
    arguments = parser.parse_args()
    counters = run_job(AnomalySettings.from_env(), arguments.event_date)
    logger.info(
        "anomaly detection completed: %s", " ".join(f"{k}={v}" for k, v in sorted(counters.items()))
    )


if __name__ == "__main__":
    main()
