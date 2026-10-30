"""T13: robust hourly metrics for directed public-transport segments."""

from __future__ import annotations

import argparse
import logging
from datetime import date
from typing import TYPE_CHECKING

from spb_transport_spark.cassandra_sink import (
    configure_cassandra,
    write_analytics_current,
    write_segment_metrics,
)
from spb_transport_spark.config import SegmentMetricsSettings

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

ALGORITHM_VERSION = "segment-metrics-v1"
TIMEZONE = "Europe/Moscow"
SEGMENT_KEYS = (
    "static_feed_version",
    "route_id",
    "shape_id",
    "direction_id",
    "pattern_id",
    "from_stop_id",
    "to_stop_id",
    "from_stop_index",
    "to_stop_index",
)
BUCKET_KEYS = (*SEGMENT_KEYS, "day_type", "hour")


def build_metrics(
    traversals: DataFrame,
    *,
    min_observations: int,
    max_gap_seconds: float,
    min_confidence: float,
    min_samples: int,
) -> DataFrame:
    """Aggregate valid traversals while retaining input-quality coverage."""
    from pyspark.sql import functions as F

    local_start = F.from_utc_timestamp("start_time", TIMEZONE)
    prepared = (
        traversals.filter(F.col("travel_time_s").isNotNull() & (F.col("travel_time_s") > 0))
        .withColumn("direction_id", F.coalesce(F.col("direction_id"), F.lit(-1)))
        .withColumn(
            "day_type", F.when(F.dayofweek(local_start).isin(1, 7), "weekend").otherwise("weekday")
        )
        .withColumn("hour", F.hour(local_start))
        .withColumn(
            "is_peak", (F.col("day_type") == "weekday") & F.col("hour").isin(7, 8, 9, 16, 17, 18)
        )
        .withColumn(
            "quality_ok",
            (F.col("observations") >= min_observations)
            & (F.col("max_gap_s") <= max_gap_seconds)
            & (F.col("confidence") >= min_confidence),
        )
        .withColumn(
            "stop_delay_share",
            F.when(
                F.col("running_time_s").isNotNull(),
                F.greatest(F.lit(0.0), F.col("travel_time_s") - F.col("running_time_s"))
                / F.col("travel_time_s"),
            ),
        )
    )
    totals = prepared.groupBy(*BUCKET_KEYS).agg(F.count("*").alias("total_samples"))
    valid = prepared.filter("quality_ok")
    base = valid.groupBy(*BUCKET_KEYS).agg(
        F.count("*").alias("qualified_samples"),
        F.countDistinct("vehicle_id").alias("vehicle_count"),
        F.countDistinct("service_date").alias("service_day_count"),
        F.first("segment_length_m", ignorenulls=True).alias("segment_length_m"),
        F.percentile_approx("travel_time_s", 0.25, 10000).alias("travel_time_p25_s"),
        F.percentile_approx("travel_time_s", 0.5, 10000).alias("travel_time_p50_s"),
        F.percentile_approx("travel_time_s", 0.75, 10000).alias("travel_time_p75_s"),
        F.percentile_approx("travel_time_s", 0.9, 10000).alias("travel_time_p90_s"),
        F.percentile_approx("travel_time_s", 0.95, 10000).alias("travel_time_p95_s"),
        F.percentile_approx("speed_mps", 0.5, 10000).alias("speed_p50_mps"),
        F.avg("stop_delay_share").alias("stop_delay_share"),
    )
    deviations = (
        valid.join(base.select(*BUCKET_KEYS, "travel_time_p50_s"), list(BUCKET_KEYS))
        .withColumn(
            "absolute_deviation_s", F.abs(F.col("travel_time_s") - F.col("travel_time_p50_s"))
        )
        .groupBy(*BUCKET_KEYS)
        .agg(F.percentile_approx("absolute_deviation_s", 0.5, 10000).alias("travel_time_mad_s"))
    )
    period_medians = valid.groupBy(*SEGMENT_KEYS, "day_type", "is_peak").agg(
        F.percentile_approx("travel_time_s", 0.5, 10000).alias("period_median_s")
    )
    ratios = (
        period_medians.groupBy(*SEGMENT_KEYS, "day_type")
        .agg(
            F.max(F.when(F.col("is_peak"), F.col("period_median_s"))).alias("peak_median_s"),
            F.max(F.when(~F.col("is_peak"), F.col("period_median_s"))).alias("offpeak_median_s"),
        )
        .withColumn(
            "peak_offpeak_ratio",
            F.when(
                F.col("offpeak_median_s") > 0, F.col("peak_median_s") / F.col("offpeak_median_s")
            ),
        )
    )
    return (
        totals.join(base, list(BUCKET_KEYS), "left")
        .join(deviations, list(BUCKET_KEYS), "left")
        .join(
            ratios.select(*SEGMENT_KEYS, "day_type", "peak_offpeak_ratio"),
            [*SEGMENT_KEYS, "day_type"],
            "left",
        )
        .fillna({"qualified_samples": 0, "vehicle_count": 0, "service_day_count": 0})
        .withColumn("coverage", F.col("qualified_samples") / F.col("total_samples"))
        .withColumn("travel_time_iqr_s", F.col("travel_time_p75_s") - F.col("travel_time_p25_s"))
        .withColumn("enough_data", F.col("qualified_samples") >= min_samples)
    )


def run_job(settings: SegmentMetricsSettings, start_date: date, end_date: date) -> int:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    if end_date < start_date:
        raise ValueError("end date must not precede start date")
    spark = (
        configure_cassandra(
            SparkSession.builder.appName("segment-bottleneck-metrics"), settings.cassandra
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        traversals = spark.read.parquet(settings.segment_traversals_path).filter(
            F.col("event_date").between(start_date.isoformat(), end_date.isoformat())
        )
        metrics = (
            build_metrics(
                traversals,
                min_observations=settings.min_observations,
                max_gap_seconds=settings.max_gap_seconds,
                min_confidence=settings.min_confidence,
                min_samples=settings.min_samples,
            )
            .withColumn("algorithm_version", F.lit(ALGORITHM_VERSION))
            .withColumn("analysis_start", F.lit(start_date.isoformat()).cast("date"))
            .withColumn("analysis_end", F.lit(end_date.isoformat()).cast("date"))
            .cache()
        )
        count = metrics.count()
        metrics.write.mode("overwrite").partitionBy(
            "algorithm_version", "analysis_start", "analysis_end"
        ).parquet(settings.segment_metrics_path)
        write_segment_metrics(metrics, settings.cassandra)
        pointer = (
            traversals.select("static_feed_version")
            .distinct()
            .limit(1)
            .select(
                F.lit("segment_metrics").alias("dataset"),
                F.lit(ALGORITHM_VERSION).alias("algorithm_version"),
                F.lit(None).cast("string").alias("model_id"),
                "static_feed_version",
                F.lit(start_date.isoformat()).cast("date").alias("period_start"),
                F.lit(end_date.isoformat()).cast("date").alias("period_end"),
                F.lit(None).cast("date").alias("training_start"),
                F.lit(None).cast("date").alias("training_end"),
                F.current_timestamp().alias("updated_at"),
            )
        )
        write_analytics_current(pointer, settings.cassandra)
        metrics.unpersist()
        return int(count)
    finally:
        spark.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Build robust segment bottleneck metrics")
    parser.add_argument("--start-date", required=True, type=date.fromisoformat)
    parser.add_argument("--end-date", required=True, type=date.fromisoformat)
    arguments = parser.parse_args()
    count = run_job(SegmentMetricsSettings.from_env(), arguments.start_date, arguments.end_date)
    logger.info("segment metrics completed: rows=%d", count)


if __name__ == "__main__":
    main()
