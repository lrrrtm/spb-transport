"""Spark checks for T14 anomaly detection (`make test-spark`)."""

from datetime import UTC, datetime, timedelta

import pytest
from spb_transport_spark.anomalies_batch import build_anomalies
from spb_transport_spark.config import AnomalySettings, CassandraSettings
from spb_transport_spark.route_matching import ALGORITHM_VERSION as MATCH_VERSION
from spb_transport_spark.stop_events import ALGORITHM_VERSION as STOP_VERSION

from spark_integration.spark_lake import local_spark

pytestmark = pytest.mark.spark


def _settings() -> AnomalySettings:
    return AnomalySettings(
        matches_path="matches",
        stop_arrivals_path="arrivals",
        anomalies_path="anomalies",
        bunching_distance_m=150,
        snapshot_seconds=30,
        short_headway_seconds=60,
        long_headway_seconds=300,
        stationary_speed_mps=0.5,
        stationary_progress_m=10,
        stationary_duration_seconds=120,
        max_observation_gap_seconds=60,
        disappearance_seconds=300,
        terminal_zone_m=100,
        gps_jump_speed_mps=50,
        shape_offset_m=100,
        cassandra=CassandraSettings("c", 9042, "dc", "ks", 120, None, None),
    )


def test_build_anomalies_detects_each_required_kind() -> None:
    spark = local_spark()
    start = datetime(2026, 10, 5, 8, tzinfo=UTC)
    rows = []

    def add(vehicle: str, seconds: int, progress: float, **overrides: float) -> None:
        values = {
            "latitude": 59.93,
            "longitude": 30.31,
            "speed": 5.0,
            "offset": 2.0,
            "shape_length": 2000.0,
        }
        values.update(overrides)
        rows.append(
            (
                f"{vehicle}-{seconds}",
                vehicle,
                "r1",
                start + timedelta(seconds=seconds),
                values["latitude"],
                values["longitude"],
                values["speed"],
                f"run-{vehicle}",
                "matched",
                "shape",
                0,
                float(progress),
                values["shape_length"],
                values["offset"],
                "f" * 64,
                MATCH_VERSION,
            )
        )

    add("b1", 0, 100)
    add("b2", 0, 200)
    add("stationary", 0, 500, speed=0.0)
    add("stationary", 60, 502, speed=0.0)
    add("stationary", 120, 504, speed=0.0)
    add("jump", 0, 800)
    add("jump", 10, 810, latitude=60.03)
    add("offset", 0, 900, offset=150.0)
    add("active", 600, 1950, shape_length=2000.0)
    matches = spark.createDataFrame(
        rows,
        "event_id string, vehicle_id string, route_id string, source_timestamp timestamp,"
        " latitude double, longitude double, speed double, run_id string, match_status string,"
        " shape_id string, direction_id int, distance_along_m double, shape_length_m double,"
        " offset_m double, static_feed_version string, algorithm_version string",
    )
    arrivals = spark.createDataFrame(
        [
            ("a", "r1", "shape", 0, "stop", start, "f" * 64, STOP_VERSION),
            ("b", "r1", "shape", 0, "stop", start + timedelta(seconds=30), "f" * 64, STOP_VERSION),
            ("c", "r1", "shape", 0, "stop", start + timedelta(seconds=400), "f" * 64, STOP_VERSION),
        ],
        "vehicle_id string, route_id string, shape_id string, direction_id int, stop_id string,"
        " passage_time timestamp, static_feed_version string, algorithm_version string",
    )

    events = build_anomalies(matches, arrivals, _settings()).collect()
    kinds = {event.anomaly_type for event in events}

    assert {"bunching", "short_headway", "long_headway", "stationary"} <= kinds
    assert {"disappearance", "gps_jump", "shape_offset"} <= kinds
    assert len({event.anomaly_id for event in events}) == len(events)
