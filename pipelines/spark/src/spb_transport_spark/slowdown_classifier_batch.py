"""T16: leakage-safe dataset and logistic baseline for next-segment slowdown."""

from __future__ import annotations

import argparse
import logging
import math
from datetime import date
from typing import TYPE_CHECKING

from spb_transport_spark.config import SlowdownClassifierSettings
from spb_transport_spark.stop_events import ALGORITHM_VERSION as STOP_EVENT_VERSION

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

ALGORITHM_VERSION = "slowdown-classifier-v1"
TIMEZONE = "Europe/Moscow"
FEATURE_COLUMNS = (
    "current_travel_ratio",
    "current_speed_mps",
    "current_stop_delay_share",
    "current_confidence",
    "current_observations_log",
    "current_max_gap_s",
    "hour_sin",
    "hour_cos",
)
IDENTITY_COLUMNS = (
    "event_id",
    "vehicle_id",
    "route_id",
    "shape_id",
    "pattern_id",
    "run_id",
    "from_stop_id",
    "to_stop_id",
    "from_stop_index",
    "to_stop_index",
    "start_time",
    "service_date",
    "static_feed_version",
)


def segment_baselines(
    traversals: DataFrame, start_date: date, end_date: date, min_samples: int
) -> DataFrame:
    from pyspark.sql import functions as F

    local_time = F.from_utc_timestamp("start_time", TIMEZONE)
    return (
        traversals.filter(
            F.col("service_date").between(start_date.isoformat(), end_date.isoformat())
            & (F.col("algorithm_version") == STOP_EVENT_VERSION)
            & F.col("travel_time_s").isNotNull()
            & (F.col("travel_time_s") > 0)
        )
        .withColumn(
            "day_type",
            F.when(F.dayofweek(local_time).isin(1, 7), "weekend").otherwise("weekday"),
        )
        .withColumn("hour", F.hour(local_time))
        .groupBy(
            "static_feed_version",
            "route_id",
            "pattern_id",
            "from_stop_index",
            "to_stop_index",
            "day_type",
            "hour",
        )
        .agg(
            F.count("*").alias("baseline_samples"),
            F.percentile_approx("travel_time_s", 0.5, 10000).alias("travel_time_p50_s"),
            F.percentile_approx("travel_time_s", 0.9, 10000).alias("travel_time_p90_s"),
        )
        .filter(F.col("baseline_samples") >= min_samples)
    )


def build_dataset(traversals: DataFrame, baselines: DataFrame, start: date, end: date) -> DataFrame:
    """Features use the current traversal; the label uses only the immediately next one."""
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    order = Window.partitionBy("run_id").orderBy("from_stop_index", "start_time")
    local_time = F.from_utc_timestamp("start_time", TIMEZONE)
    sequenced = (
        traversals.filter(
            F.col("service_date").between(start.isoformat(), end.isoformat())
            & (F.col("algorithm_version") == STOP_EVENT_VERSION)
            & F.col("travel_time_s").isNotNull()
            & (F.col("travel_time_s") > 0)
        )
        .withColumn("next_from_stop_index", F.lead("from_stop_index").over(order))
        .withColumn("next_to_stop_index", F.lead("to_stop_index").over(order))
        .withColumn("next_travel_time_s", F.lead("travel_time_s").over(order))
        .filter(F.col("to_stop_index") == F.col("next_from_stop_index"))
        .withColumn(
            "day_type",
            F.when(F.dayofweek(local_time).isin(1, 7), "weekend").otherwise("weekday"),
        )
        .withColumn("hour", F.hour(local_time))
        .alias("row")
    )
    current = baselines.select(
        *[
            F.col(name).alias(f"current_{name}")
            for name in (
                "static_feed_version",
                "route_id",
                "pattern_id",
                "from_stop_index",
                "to_stop_index",
                "day_type",
                "hour",
            )
        ],
        F.col("travel_time_p50_s").alias("current_p50_s"),
    ).alias("current")
    next_ = baselines.select(
        *[
            F.col(name).alias(f"next_{name}")
            for name in ("static_feed_version", "route_id", "pattern_id", "day_type", "hour")
        ],
        F.col("from_stop_index").alias("baseline_next_from_stop_index"),
        F.col("to_stop_index").alias("baseline_next_to_stop_index"),
        F.col("travel_time_p90_s").alias("next_p90_s"),
    ).alias("next")
    joined = sequenced.join(
        current,
        (F.col("row.static_feed_version") == F.col("current.current_static_feed_version"))
        & (F.col("row.route_id") == F.col("current.current_route_id"))
        & (F.col("row.pattern_id") == F.col("current.current_pattern_id"))
        & (F.col("row.from_stop_index") == F.col("current.current_from_stop_index"))
        & (F.col("row.to_stop_index") == F.col("current.current_to_stop_index"))
        & (F.col("row.day_type") == F.col("current.current_day_type"))
        & (F.col("row.hour") == F.col("current.current_hour")),
    ).join(
        next_,
        (F.col("row.static_feed_version") == F.col("next.next_static_feed_version"))
        & (F.col("row.route_id") == F.col("next.next_route_id"))
        & (F.col("row.pattern_id") == F.col("next.next_pattern_id"))
        & (F.col("row.next_from_stop_index") == F.col("next.baseline_next_from_stop_index"))
        & (F.col("row.next_to_stop_index") == F.col("next.baseline_next_to_stop_index"))
        & (F.col("row.day_type") == F.col("next.next_day_type"))
        & (F.col("row.hour") == F.col("next.next_hour")),
    )
    angle = F.col("row.hour") * F.lit(2 * math.pi / 24)
    return joined.select(
        *[F.col(f"row.{name}").alias(name) for name in IDENTITY_COLUMNS],
        F.col("row.next_travel_time_s"),
        F.col("next.next_p90_s"),
        (F.col("row.next_travel_time_s") > F.col("next.next_p90_s")).cast("double").alias("label"),
        (F.col("row.travel_time_s") / F.col("current.current_p50_s")).alias("current_travel_ratio"),
        F.coalesce(F.col("row.speed_mps"), F.lit(0.0)).alias("current_speed_mps"),
        F.when(
            F.col("row.running_time_s").isNotNull(),
            F.greatest(F.lit(0.0), F.col("row.travel_time_s") - F.col("row.running_time_s"))
            / F.col("row.travel_time_s"),
        )
        .otherwise(0.0)
        .alias("current_stop_delay_share"),
        F.col("row.confidence").alias("current_confidence"),
        F.log1p(F.col("row.observations")).alias("current_observations_log"),
        F.col("row.max_gap_s").alias("current_max_gap_s"),
        F.sin(angle).alias("hour_sin"),
        F.cos(angle).alias("hour_cos"),
    ).dropna(subset=list(FEATURE_COLUMNS))


def classify_slowdown(evaluation: DataFrame, settings: SlowdownClassifierSettings) -> DataFrame:
    """Explainable baseline: a currently slow segment predicts another slow segment."""
    from pyspark.sql import functions as F

    return evaluation.withColumn("risk_score", F.col("current_travel_ratio")).withColumn(
        "prediction", (F.col("risk_score") >= settings.risk_threshold).cast("double")
    )


def evaluation_metrics(predictions: DataFrame, evaluation_date: date) -> DataFrame:
    from pyspark.sql import Window
    from pyspark.sql import functions as F

    totals = predictions.agg(
        F.count("*").alias("samples"),
        F.sum(F.when((F.col("label") == 1) & (F.col("prediction") == 1), 1).otherwise(0)).alias(
            "tp"
        ),
        F.sum(F.when((F.col("label") == 0) & (F.col("prediction") == 0), 1).otherwise(0)).alias(
            "tn"
        ),
        F.sum(F.when((F.col("label") == 0) & (F.col("prediction") == 1), 1).otherwise(0)).alias(
            "fp"
        ),
        F.sum(F.when((F.col("label") == 1) & (F.col("prediction") == 0), 1).otherwise(0)).alias(
            "fn"
        ),
    )
    metrics = (
        totals.withColumn("precision", F.col("tp") / (F.col("tp") + F.col("fp")))
        .withColumn("recall", F.col("tp") / (F.col("tp") + F.col("fn")))
        .withColumn(
            "f1",
            2 * F.col("precision") * F.col("recall") / (F.col("precision") + F.col("recall")),
        )
        .withColumn("accuracy", (F.col("tp") + F.col("tn")) / F.col("samples"))
    )
    score_groups = predictions.groupBy("risk_score").agg(
        F.sum(F.when(F.col("label") == 1, 1).otherwise(0)).alias("positives"),
        F.sum(F.when(F.col("label") == 0, 1).otherwise(0)).alias("negatives"),
    )
    lower_scores = Window.orderBy("risk_score").rowsBetween(Window.unboundedPreceding, -1)
    auc_parts = score_groups.withColumn(
        "negatives_below", F.coalesce(F.sum("negatives").over(lower_scores), F.lit(0))
    ).agg(
        F.sum("positives").alias("positive_total"),
        F.sum("negatives").alias("negative_total"),
        F.sum(
            F.col("positives") * (F.col("negatives_below") + F.lit(0.5) * F.col("negatives"))
        ).alias("correct_pairs"),
    )
    return (
        metrics.crossJoin(auc_parts)
        .withColumn(
            "roc_auc",
            F.when(
                (F.col("positive_total") > 0) & (F.col("negative_total") > 0),
                F.col("correct_pairs") / (F.col("positive_total") * F.col("negative_total")),
            ),
        )
        .drop("positive_total", "negative_total", "correct_pairs")
        .withColumn("evaluation_date", F.lit(evaluation_date.isoformat()).cast("date"))
        .withColumn("algorithm_version", F.lit(ALGORITHM_VERSION))
    )


def run_job(
    settings: SlowdownClassifierSettings,
    training_start: date,
    training_end: date,
    evaluation_date: date,
) -> dict[str, int]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    if not training_start <= training_end < evaluation_date:
        raise ValueError("training range must end before evaluation date")
    spark = (
        SparkSession.builder.appName("slowdown-classifier")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        traversals = spark.read.parquet(settings.segment_traversals_path)
        baselines = segment_baselines(
            traversals, training_start, training_end, settings.min_baseline_samples
        ).cache()
        training = build_dataset(traversals, baselines, training_start, training_end).cache()
        evaluation = build_dataset(traversals, baselines, evaluation_date, evaluation_date).cache()
        predictions = classify_slowdown(evaluation, settings)
        predictions = (
            predictions.withColumn("training_start", F.lit(training_start.isoformat()).cast("date"))
            .withColumn("training_end", F.lit(training_end.isoformat()).cast("date"))
            .withColumn("evaluation_date", F.lit(evaluation_date.isoformat()).cast("date"))
            .withColumn("algorithm_version", F.lit(ALGORITHM_VERSION))
            .cache()
        )
        metrics = (
            evaluation_metrics(predictions, evaluation_date)
            .withColumn("training_start", F.lit(training_start.isoformat()).cast("date"))
            .withColumn("training_end", F.lit(training_end.isoformat()).cast("date"))
        )
        counts = {"training_rows": training.count(), "evaluation_rows": predictions.count()}
        predictions.write.mode("overwrite").partitionBy(
            "algorithm_version", "evaluation_date"
        ).parquet(f"{settings.output_path}/predictions")
        metrics.write.mode("overwrite").partitionBy("algorithm_version", "evaluation_date").parquet(
            f"{settings.output_path}/metrics"
        )
        baselines.unpersist()
        training.unpersist()
        evaluation.unpersist()
        predictions.unpersist()
        return counts
    finally:
        spark.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(
        description="Train and evaluate next-segment slowdown baseline"
    )
    parser.add_argument("--training-start", required=True, type=date.fromisoformat)
    parser.add_argument("--training-end", required=True, type=date.fromisoformat)
    parser.add_argument("--evaluation-date", required=True, type=date.fromisoformat)
    arguments = parser.parse_args()
    counts = run_job(
        SlowdownClassifierSettings.from_env(),
        arguments.training_start,
        arguments.training_end,
        arguments.evaluation_date,
    )
    logger.info(
        "slowdown classification completed: %s", " ".join(f"{k}={v}" for k, v in counts.items())
    )


if __name__ == "__main__":
    main()
