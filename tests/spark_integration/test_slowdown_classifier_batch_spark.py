"""Spark checks for T16 leakage-safe slowdown classification (`make test-spark`)."""

from datetime import UTC, date, datetime, timedelta

import pytest
from spb_transport_spark.config import SlowdownClassifierSettings
from spb_transport_spark.slowdown_classifier_batch import (
    build_dataset,
    classify_slowdown,
    evaluation_metrics,
    segment_baselines,
)
from spb_transport_spark.stop_events import ALGORITHM_VERSION as STOP_VERSION

from spark_integration.spark_lake import local_spark

pytestmark = pytest.mark.spark


def test_temporal_dataset_and_logistic_baseline() -> None:
    spark = local_spark()
    training_day = date(2026, 10, 5)
    evaluation_day = date(2026, 10, 6)
    rows = []

    def add_run(run: str, day: date, current_s: float, next_s: float) -> None:
        started = datetime(day.year, day.month, day.day, 8, tzinfo=UTC)
        common = ("v" + run, "r1", "shape", "pattern", run, day, "f" * 64, STOP_VERSION)
        rows.extend(
            [
                (
                    f"{run}-0",
                    *common,
                    "A",
                    "B",
                    0,
                    1,
                    started,
                    current_s,
                    current_s - 10,
                    500 / current_s,
                    0.95,
                    5,
                    20.0,
                ),
                (
                    f"{run}-1",
                    *common,
                    "B",
                    "C",
                    1,
                    2,
                    started + timedelta(seconds=current_s),
                    next_s,
                    next_s - 10,
                    500 / next_s,
                    0.95,
                    5,
                    20.0,
                ),
            ]
        )

    for index in range(20):
        add_run(f"train-{index}", training_day, 80.0 + index * 2, 100.0 + index * 5)
    add_run("eval-slow", evaluation_day, 130.0, 250.0)
    add_run("eval-normal", evaluation_day, 80.0, 100.0)
    traversals = spark.createDataFrame(
        rows,
        "event_id string, vehicle_id string, route_id string, shape_id string, pattern_id string,"
        " run_id string, service_date date, static_feed_version string, algorithm_version string,"
        " from_stop_id string, to_stop_id string, from_stop_index int, to_stop_index int,"
        " start_time timestamp, travel_time_s double, running_time_s double, speed_mps double,"
        " confidence double, observations int, max_gap_s double",
    )
    settings = SlowdownClassifierSettings("traversals", "output", 5, 1.1)
    baselines = segment_baselines(traversals, training_day, training_day, 5)
    training = build_dataset(traversals, baselines, training_day, training_day)
    evaluation = build_dataset(traversals, baselines, evaluation_day, evaluation_day)

    predictions = classify_slowdown(evaluation, settings)
    training_labels = {row.label for row in training.select("label").collect()}
    labels = {row.label for row in predictions.select("label").collect()}
    metrics = evaluation_metrics(predictions, evaluation_day).first()
    assert metrics is not None

    assert training_labels == {0.0, 1.0}
    assert labels == {0.0, 1.0}
    assert predictions.count() == 2
    assert metrics.samples == 2
    assert metrics.roc_auc is not None
