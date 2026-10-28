"""Spark checks for T15 ETA baseline and evaluation (`make test-spark`)."""

from datetime import UTC, date, datetime, timedelta

import pytest
from spb_transport_spark.config import CassandraSettings, EtaBaselineSettings
from spb_transport_spark.eta_baseline_batch import build_metrics, build_predictions
from spb_transport_spark.stop_events import ALGORITHM_VERSION as STOP_VERSION

from spark_integration.spark_lake import local_spark

pytestmark = pytest.mark.spark


def _settings() -> EtaBaselineSettings:
    return EtaBaselineSettings(
        "eta",
        "arrivals",
        "traversals",
        "output",
        600,
        3600,
        1,
        CassandraSettings("c", 9042, "dc", "ks", 120, None, None),
    )


def test_baseline_sums_historical_segments_and_uses_same_actual_label() -> None:
    spark = local_spark()
    training_day = date(2026, 10, 5)
    evaluation_day = date(2026, 10, 6)
    training_time = datetime(2026, 10, 5, 8, tzinfo=UTC)
    collected = datetime(2026, 10, 6, 8, 1, tzinfo=UTC)
    eta = spark.createDataFrame(
        [
            (
                "eta1",
                "v1",
                "r1",
                collected,
                evaluation_day,
                [(0, "C", collected + timedelta(seconds=300))],
            )
        ],
        "event_id string, vehicle_id string, route_id string, collected_at timestamp,"
        " collected_date date, forecasts array<struct<forecast_rank:int,stop_id:string,"
        "arrival_time:timestamp>>",
    )
    arrivals = spark.createDataFrame(
        [
            (
                "v1",
                "r1",
                "shape",
                0,
                "pattern",
                "A",
                0,
                collected - timedelta(seconds=60),
                None,
                "f" * 64,
                STOP_VERSION,
            ),
            (
                "v1",
                "r1",
                "shape",
                0,
                "pattern",
                "C",
                2,
                collected + timedelta(seconds=330),
                None,
                "f" * 64,
                STOP_VERSION,
            ),
        ],
        "vehicle_id string, route_id string, shape_id string, direction_id int, pattern_id string,"
        " stop_id string, stop_index int, passage_time timestamp, arrival_time timestamp,"
        " static_feed_version string, algorithm_version string",
    )
    traversals = spark.createDataFrame(
        [
            (
                "r1",
                "pattern",
                "A",
                "B",
                0,
                1,
                100.0,
                training_time,
                training_day,
                "f" * 64,
                STOP_VERSION,
            ),
            (
                "r1",
                "pattern",
                "B",
                "C",
                1,
                2,
                200.0,
                training_time,
                training_day,
                "f" * 64,
                STOP_VERSION,
            ),
        ],
        "route_id string, pattern_id string, from_stop_id string, to_stop_id string,"
        " from_stop_index int, to_stop_index int, travel_time_s double, start_time timestamp,"
        " service_date date, static_feed_version string, algorithm_version string",
    )

    predictions = build_predictions(
        eta,
        arrivals,
        traversals,
        training_start=training_day,
        training_end=training_day,
        evaluation_date=evaluation_day,
        settings=_settings(),
    )
    prediction = predictions.first()
    metric = build_metrics(predictions, training_day, training_day, evaluation_day).first()

    assert prediction is not None
    assert metric is not None
    assert prediction.baseline_lead_s == pytest.approx(300)
    assert prediction.actual_lead_s == 330
    assert prediction.official_absolute_error_s == 30
    assert prediction.baseline_absolute_error_s == pytest.approx(30)
    assert metric.paired_forecasts == 1 and metric.baseline_coverage == pytest.approx(1)
    assert metric.official_mae_s == pytest.approx(metric.baseline_mae_s)
