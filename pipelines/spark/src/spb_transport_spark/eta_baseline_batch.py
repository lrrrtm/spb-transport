"""T15: historical segment-median ETA baseline and offline official ETA comparison."""

from __future__ import annotations

import argparse
import logging
from datetime import date
from typing import TYPE_CHECKING

from spb_transport_spark.cassandra_sink import configure_cassandra, write_eta_comparison
from spb_transport_spark.config import EtaBaselineSettings
from spb_transport_spark.stop_events import ALGORITHM_VERSION as STOP_EVENT_VERSION

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame

logger = logging.getLogger(__name__)

ALGORITHM_VERSION = "eta-baseline-v1"
TIMEZONE = "Europe/Moscow"


def _day_type(timestamp: str) -> Column:
    from pyspark.sql import functions as F

    local = F.from_utc_timestamp(timestamp, TIMEZONE)
    return F.when(F.dayofweek(local).isin(1, 7), "weekend").otherwise("weekday")


def build_predictions(
    eta: DataFrame,
    arrivals: DataFrame,
    traversals: DataFrame,
    *,
    training_start: date,
    training_end: date,
    evaluation_date: date,
    settings: EtaBaselineSettings,
) -> DataFrame:
    """Create paired official/baseline predictions with the same future-arrival label."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    forecasts = (
        eta.filter(F.col("collected_date") == evaluation_date.isoformat())
        .select(
            "event_id",
            "vehicle_id",
            "route_id",
            "collected_at",
            F.explode("forecasts").alias("forecast"),
        )
        .select(
            "event_id",
            "vehicle_id",
            "route_id",
            "collected_at",
            F.col("forecast.forecast_rank").alias("forecast_rank"),
            F.col("forecast.stop_id").alias("target_stop_id"),
            F.col("forecast.arrival_time").alias("official_arrival_time"),
        )
        .withColumn(
            "forecast_key",
            F.sha2(
                F.concat_ws(
                    "|",
                    "event_id",
                    F.col("forecast_rank").cast("string"),
                    F.col("collected_at").cast("string"),
                ),
                256,
            ),
        )
        .withColumn(
            "official_lead_s",
            F.unix_timestamp("official_arrival_time") - F.unix_timestamp("collected_at"),
        )
        .filter(
            (F.col("official_lead_s") > 0)
            & (F.col("official_lead_s") <= settings.max_horizon_seconds)
        )
        .withColumn("day_type", _day_type("collected_at"))
        .withColumn("hour", F.hour(F.from_utc_timestamp("collected_at", TIMEZONE)))
    )

    passed = (
        arrivals.filter(
            (F.col("algorithm_version") == STOP_EVENT_VERSION) & F.col("passage_time").isNotNull()
        )
        .select(
            "vehicle_id",
            "route_id",
            "shape_id",
            "pattern_id",
            "stop_id",
            "stop_index",
            F.col("passage_time").alias("current_stop_time"),
            "static_feed_version",
        )
        .alias("passed")
    )
    current_candidates = (
        forecasts.alias("forecast")
        .join(
            passed,
            (F.col("forecast.vehicle_id") == F.col("passed.vehicle_id"))
            & (F.col("forecast.route_id") == F.col("passed.route_id"))
            & (F.col("passed.current_stop_time") <= F.col("forecast.collected_at")),
        )
        .withColumn(
            "current_stop_age_s",
            F.unix_timestamp("forecast.collected_at")
            - F.unix_timestamp("passed.current_stop_time"),
        )
        .filter(F.col("current_stop_age_s") <= settings.max_current_stop_age_seconds)
        .withColumn(
            "current_rank",
            F.row_number().over(
                Window.partitionBy("forecast.forecast_key").orderBy(
                    F.col("passed.current_stop_time").desc()
                )
            ),
        )
        .filter("current_rank = 1")
        .select(
            "forecast.*",
            "passed.shape_id",
            "passed.pattern_id",
            F.col("passed.stop_index").alias("current_stop_index"),
            "passed.static_feed_version",
        )
    )

    targets = traversals.select(
        "static_feed_version",
        "route_id",
        "pattern_id",
        F.col("to_stop_id").alias("target_stop_id"),
        "to_stop_index",
    ).dropDuplicates()
    routed = current_candidates.join(
        targets,
        ["static_feed_version", "route_id", "pattern_id", "target_stop_id"],
        "inner",
    ).filter(F.col("to_stop_index") > F.col("current_stop_index"))

    historical_segments = (
        traversals.filter(
            F.col("service_date").between(training_start.isoformat(), training_end.isoformat())
            & F.col("travel_time_s").isNotNull()
            & (F.col("travel_time_s") > 0)
            & (F.col("algorithm_version") == STOP_EVENT_VERSION)
        )
        .withColumn("day_type", _day_type("start_time"))
        .withColumn("hour", F.hour(F.from_utc_timestamp("start_time", TIMEZONE)))
        .groupBy(
            "static_feed_version",
            "route_id",
            "pattern_id",
            "day_type",
            "hour",
            "from_stop_index",
            "to_stop_index",
        )
        .agg(
            F.count("*").alias("segment_samples"),
            F.percentile_approx("travel_time_s", 0.5, 10000).alias("segment_median_s"),
        )
        .filter(F.col("segment_samples") >= settings.min_segment_samples)
        .alias("segment")
    )
    baseline = (
        routed.alias("forecast")
        .join(
            historical_segments,
            (F.col("forecast.static_feed_version") == F.col("segment.static_feed_version"))
            & (F.col("forecast.route_id") == F.col("segment.route_id"))
            & (F.col("forecast.pattern_id") == F.col("segment.pattern_id"))
            & (F.col("forecast.day_type") == F.col("segment.day_type"))
            & (F.col("forecast.hour") == F.col("segment.hour"))
            & (F.col("segment.from_stop_index") >= F.col("forecast.current_stop_index"))
            & (F.col("segment.to_stop_index") <= F.col("forecast.to_stop_index")),
        )
        .groupBy("forecast.forecast_key")
        .agg(
            F.sum("segment.segment_median_s").alias("baseline_lead_s"),
            F.count("*").alias("baseline_segment_count"),
            F.first(F.col("forecast.to_stop_index") - F.col("forecast.current_stop_index")).alias(
                "expected_segment_count"
            ),
        )
        .filter(F.col("baseline_segment_count") == F.col("expected_segment_count"))
    )

    future_arrivals = arrivals.filter(
        (F.col("algorithm_version") == STOP_EVENT_VERSION)
        & F.coalesce("passage_time", "arrival_time").isNotNull()
    ).select(
        "vehicle_id",
        "route_id",
        F.col("stop_id").alias("target_stop_id"),
        F.coalesce("passage_time", "arrival_time").alias("actual_arrival_time"),
    )
    labeled_candidates = (
        forecasts.alias("forecast")
        .join(future_arrivals.alias("actual"), ["vehicle_id", "route_id", "target_stop_id"])
        .withColumn(
            "actual_lead_s",
            F.unix_timestamp("actual_arrival_time") - F.unix_timestamp("collected_at"),
        )
        .filter(
            (F.col("actual_lead_s") > 0) & (F.col("actual_lead_s") <= settings.max_horizon_seconds)
        )
        .withColumn(
            "actual_rank",
            F.row_number().over(Window.partitionBy("forecast_key").orderBy("actual_arrival_time")),
        )
        .filter("actual_rank = 1")
        .select("forecast.*", "actual_arrival_time", "actual_lead_s")
    )
    return (
        labeled_candidates.join(baseline, "forecast_key", "left")
        .withColumn(
            "baseline_arrival_time",
            F.timestamp_seconds(F.unix_timestamp("collected_at") + F.col("baseline_lead_s")),
        )
        .withColumn(
            "official_absolute_error_s",
            F.abs(
                F.unix_timestamp("official_arrival_time") - F.unix_timestamp("actual_arrival_time")
            ),
        )
        .withColumn(
            "baseline_absolute_error_s",
            F.abs(F.col("baseline_lead_s") - F.col("actual_lead_s")),
        )
        .withColumn(
            "horizon_bucket",
            F.when(F.col("actual_lead_s") <= 300, "00-05m")
            .when(F.col("actual_lead_s") <= 900, "05-15m")
            .when(F.col("actual_lead_s") <= 1800, "15-30m")
            .otherwise("30m+"),
        )
    )


def build_metrics(
    predictions: DataFrame, training_start: date, training_end: date, evaluation_date: date
) -> DataFrame:
    """Compare models only on labels where the baseline is also available."""
    from pyspark.sql import functions as F

    return (
        predictions.groupBy("route_id", "horizon_bucket")
        .agg(
            F.count("*").alias("labeled_forecasts"),
            F.count("baseline_lead_s").alias("paired_forecasts"),
            F.avg(
                F.when(F.col("baseline_lead_s").isNotNull(), F.col("official_absolute_error_s"))
            ).alias("official_mae_s"),
            F.percentile_approx(
                F.when(F.col("baseline_lead_s").isNotNull(), F.col("official_absolute_error_s")),
                0.5,
                10000,
            ).alias("official_median_absolute_error_s"),
            F.avg("baseline_absolute_error_s").alias("baseline_mae_s"),
            F.percentile_approx("baseline_absolute_error_s", 0.5, 10000).alias(
                "baseline_median_absolute_error_s"
            ),
        )
        .withColumn("baseline_coverage", F.col("paired_forecasts") / F.col("labeled_forecasts"))
        .withColumn("training_start", F.lit(training_start.isoformat()).cast("date"))
        .withColumn("training_end", F.lit(training_end.isoformat()).cast("date"))
        .withColumn("evaluation_date", F.lit(evaluation_date.isoformat()).cast("date"))
        .withColumn("algorithm_version", F.lit(ALGORITHM_VERSION))
    )


def run_job(
    settings: EtaBaselineSettings,
    training_start: date,
    training_end: date,
    evaluation_date: date,
) -> dict[str, int]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    if not training_start <= training_end < evaluation_date:
        raise ValueError("training range must end before evaluation date")
    spark = (
        configure_cassandra(
            SparkSession.builder.appName("eta-baseline-evaluation"), settings.cassandra
        )
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        predictions = (
            build_predictions(
                spark.read.parquet(settings.eta_history_path),
                spark.read.parquet(settings.stop_arrivals_path),
                spark.read.parquet(settings.segment_traversals_path),
                training_start=training_start,
                training_end=training_end,
                evaluation_date=evaluation_date,
                settings=settings,
            )
            .withColumn("training_start", F.lit(training_start.isoformat()).cast("date"))
            .withColumn("training_end", F.lit(training_end.isoformat()).cast("date"))
            .withColumn("algorithm_version", F.lit(ALGORITHM_VERSION))
            .cache()
        )
        metrics = build_metrics(predictions, training_start, training_end, evaluation_date).cache()
        counts = {"predictions": predictions.count(), "metric_rows": metrics.count()}
        predictions.withColumn("evaluation_date", F.col("collected_at").cast("date")).write.mode(
            "overwrite"
        ).partitionBy("algorithm_version", "evaluation_date").parquet(
            f"{settings.output_path}/predictions"
        )
        metrics.write.mode("overwrite").partitionBy("algorithm_version", "evaluation_date").parquet(
            f"{settings.output_path}/metrics"
        )
        write_eta_comparison(metrics, settings.cassandra)
        predictions.unpersist()
        metrics.unpersist()
        return counts
    finally:
        spark.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Evaluate historical segment ETA baseline")
    parser.add_argument("--training-start", required=True, type=date.fromisoformat)
    parser.add_argument("--training-end", required=True, type=date.fromisoformat)
    parser.add_argument("--evaluation-date", required=True, type=date.fromisoformat)
    arguments = parser.parse_args()
    counts = run_job(
        EtaBaselineSettings.from_env(),
        arguments.training_start,
        arguments.training_end,
        arguments.evaluation_date,
    )
    logger.info("ETA evaluation completed: %s", " ".join(f"{k}={v}" for k, v in counts.items()))


if __name__ == "__main__":
    main()
