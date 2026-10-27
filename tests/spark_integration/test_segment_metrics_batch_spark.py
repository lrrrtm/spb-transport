"""Spark transformations for T13 robust segment metrics (`make test-spark`)."""

from datetime import UTC, date, datetime, timedelta

import pytest
from spb_transport_spark.segment_metrics_batch import build_metrics

from spark_integration.spark_lake import local_spark

pytestmark = pytest.mark.spark


def test_metrics_filter_bad_coverage_and_calculate_robust_statistics() -> None:
    spark = local_spark()
    start = datetime(2026, 10, 5, 4, tzinfo=UTC)  # Monday, 07:00 MSK
    travel_times = [100.0, 110.0, 120.0, 1000.0]
    rows = []
    for index, travel_time in enumerate(travel_times):
        rows.append(
            (
                f"v{index}",
                "r1",
                "shape",
                0,
                "pattern",
                "a",
                "b",
                0,
                1,
                500.0,
                start + timedelta(minutes=index),
                travel_time,
                travel_time - 10,
                500 / travel_time,
                5,
                20.0,
                0.9,
                date(2026, 10, 5),
                "f" * 64,
            )
        )
    rows[-1] = (*rows[-1][:-4], 200.0, *rows[-1][-3:])  # max_gap_s fails quality
    for index, travel_time in enumerate((50.0, 60.0), start=4):
        rows.append(
            (
                f"v{index}",
                "r1",
                "shape",
                0,
                "pattern",
                "a",
                "b",
                0,
                1,
                500.0,
                start + timedelta(hours=5, minutes=index),
                travel_time,
                travel_time - 5,
                500 / travel_time,
                5,
                20.0,
                0.9,
                date(2026, 10, 5),
                "f" * 64,
            )
        )
    traversals = spark.createDataFrame(
        rows,
        "vehicle_id string, route_id string, shape_id string, direction_id int,"
        " pattern_id string, from_stop_id string, to_stop_id string, from_stop_index int,"
        " to_stop_index int, segment_length_m double, start_time timestamp, travel_time_s double,"
        " running_time_s double, speed_mps double, observations int, max_gap_s double,"
        " confidence double, service_date date, static_feed_version string",
    )

    result = (
        build_metrics(
            traversals,
            min_observations=3,
            max_gap_seconds=60,
            min_confidence=0.8,
            min_samples=3,
        )
        .filter("hour = 7")
        .first()
    )

    assert result.day_type == "weekday" and result.hour == 7
    assert result.total_samples == 4 and result.qualified_samples == 3
    assert result.coverage == pytest.approx(0.75)
    assert result.travel_time_p50_s == pytest.approx(110)
    assert result.travel_time_iqr_s == pytest.approx(20)
    assert result.travel_time_mad_s == pytest.approx(10)
    assert result.stop_delay_share == pytest.approx((0.1 + 10 / 110 + 10 / 120) / 3)
    assert result.peak_offpeak_ratio == pytest.approx(2.2)
    assert result.enough_data is True
