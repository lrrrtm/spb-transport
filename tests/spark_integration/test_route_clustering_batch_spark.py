"""Spark feature engineering checks for T17 route clustering (`make test-spark`)."""

from datetime import date

import pytest
from spb_transport_spark.anomalies_batch import ALGORITHM_VERSION as ANOMALY_VERSION
from spb_transport_spark.route_clustering_batch import build_route_features
from spb_transport_spark.segment_metrics_batch import ALGORITHM_VERSION as METRICS_VERSION

from spark_integration.spark_lake import local_spark

pytestmark = pytest.mark.spark


def test_route_features_join_segment_quality_and_anomaly_rates() -> None:
    spark = local_spark()
    start, end = date(2026, 10, 1), date(2026, 10, 2)
    metrics = spark.createDataFrame(
        [
            ("f", "r1", start, end, 60, 80, 10.0, 20.0, 100.0, 1.5, 0.2, METRICS_VERSION),
            ("f", "r1", start, end, 50, 70, 8.0, 30.0, 120.0, 1.5, 0.3, METRICS_VERSION),
            ("f", "r2", start, end, 5, 10, 5.0, 10.0, 100.0, None, 0.1, METRICS_VERSION),
        ],
        "static_feed_version string, route_id string, analysis_start date, analysis_end date,"
        " qualified_samples long, total_samples long, speed_p50_mps double,"
        " travel_time_iqr_s double, travel_time_p50_s double, peak_offpeak_ratio double,"
        " stop_delay_share double, algorithm_version string",
    )
    anomalies = spark.createDataFrame(
        [
            ("f", "r1", start, "bunching", ANOMALY_VERSION),
            ("f", "r1", end, "long_headway", ANOMALY_VERSION),
            ("f", "r1", end, "stationary", ANOMALY_VERSION),
        ],
        "static_feed_version string, route_id string, event_date date, anomaly_type string,"
        " algorithm_version string",
    )

    rows = build_route_features(metrics, anomalies, start, end, 50).collect()

    assert len(rows) == 1 and rows[0].route_id == "r1"
    assert rows[0].qualified_samples == 110
    assert rows[0].bunching_per_day == pytest.approx(0.5)
    assert rows[0].irregular_headways_per_day == pytest.approx(0.5)
    assert rows[0].stationary_per_day == pytest.approx(0.5)
