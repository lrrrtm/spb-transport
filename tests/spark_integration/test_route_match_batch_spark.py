"""End-to-end check of the T09 batch job on a local Spark session (`make test-spark`)."""

from __future__ import annotations

import math
from datetime import UTC, date, datetime
from pathlib import Path
from typing import Any

import pytest
from pytest import MonkeyPatch
from spb_transport_spark.config import ROUTE_MATCH_PARAMETER_ENV, RouteMatchingSettings
from spb_transport_spark.route_match_batch import run_job

pytestmark = pytest.mark.spark
pyspark = pytest.importorskip("pyspark")

VERSION = "a" * 64
LAT0, LON0 = 59.93, 30.31
M_PER_DEG_LAT = 111_195.0
M_PER_DEG_LON = M_PER_DEG_LAT * math.cos(math.radians(LAT0))
PARAMETERS = {
    "max_offset_m": "75",
    "gps_sigma_m": "15",
    "outlier_offset_m": "50",
    "bearing_weight": "2",
    "bearing_min_speed": "1",
    "max_backtrack_m": "30",
    "max_speed_mps": "30",
    "transition_beta_m": "20",
    "switch_cost": "12",
    "terminal_restart_cost": "2",
    "off_route_transition_cost": "3",
    "terminal_zone_m": "300",
    "max_gap_seconds": "300",
    "min_confidence": "0.8",
}


def at(north_m: float, east_m: float) -> tuple[float, float]:
    return LAT0 + north_m / M_PER_DEG_LAT, LON0 + east_m / M_PER_DEG_LON


def local_spark() -> Any:
    from pyspark.sql import SparkSession

    return (
        SparkSession.builder.master("local[1]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )


def write_inputs(root: Path) -> None:
    from pyspark.sql import functions as F

    spark = local_spark()
    shapes = []
    for shape_id, points in {"north": [(0, 0), (2000, 0)], "south": [(2000, 12), (0, 12)]}.items():
        for index, (north, east) in enumerate(points):
            lat, lon = at(north, east)
            shapes.append((shape_id, lat, lon, index + 1, None))
    static = root / f"static/version={VERSION}"
    spark.createDataFrame(
        shapes,
        "shape_id string, shape_pt_lat double, shape_pt_lon double,"
        " shape_pt_sequence int, shape_dist_traveled double",
    ).write.parquet(str(static / "shapes"))
    spark.createDataFrame(
        [("t1", "r1", "s", 0, "north"), ("t2", "r1", "s", 1, "south")],
        "trip_id string, route_id string, service_id string, direction_id int, shape_id string",
    ).write.parquet(str(static / "trips"))

    start = datetime(2026, 10, 1, 8, tzinfo=UTC).timestamp()
    rows = []
    for vehicle, route in (("v1", "r1"), ("v2", "unknown")):
        for i in range(40):
            outbound = i <= 20
            north = 100 * i if outbound else 2000 - 100 * (i - 20)
            lat, lon = at(north, 3 if outbound else 9)
            rows.append(
                (
                    f"{vehicle}{i:04d}".ljust(64, "0"),
                    vehicle,
                    route,
                    datetime.fromtimestamp(start + 15 * i, UTC),
                    lat,
                    lon,
                    0.0 if outbound else 180.0,
                    5.0,
                    "2026-10-01",
                    "08",
                )
            )
    rows.append(rows[0])  # a replay that streaming deduplication missed
    spark.createDataFrame(
        rows,
        "event_id string, vehicle_id string, route_id string, source_timestamp timestamp,"
        " latitude double, longitude double, bearing double, speed double,"
        " event_date string, event_hour string",
    ).withColumn("event_date", F.col("event_date").cast("date")).write.partitionBy(
        "event_date", "event_hour"
    ).parquet(str(root / "positions"))


def test_route_matching_job_is_correct_and_idempotent(
    tmp_path: Path, monkeypatch: MonkeyPatch
) -> None:
    write_inputs(tmp_path)
    monkeypatch.setenv("HDFS_POSITION_PROCESSED_PATH", str(tmp_path / "positions"))
    monkeypatch.setenv("HDFS_STATIC_PROCESSED_PATH", str(tmp_path / "static"))
    monkeypatch.setenv("HDFS_ROUTE_MATCHES_PATH", str(tmp_path / "matches"))
    for field, name in ROUTE_MATCH_PARAMETER_ENV.items():
        monkeypatch.setenv(name, PARAMETERS[field])
    settings = RouteMatchingSettings.from_env()

    first = run_job(settings, date(2026, 10, 1), VERSION)
    local_spark()  # run_job stops its session; the next run must start from scratch
    second = run_job(settings, date(2026, 10, 1), VERSION)

    assert first == second == {"matched": 40, "no_static_shape": 40}
    matches = local_spark().read.parquet(str(tmp_path / "matches"))
    rows = matches.collect()
    assert len(rows) == 80
    assert {r.algorithm_version for r in rows} == {"route-match-hmm-v1"}
    assert {r.static_feed_version for r in rows} == {VERSION}
    v1 = sorted((r for r in rows if r.vehicle_id == "v1"), key=lambda r: r.source_timestamp)
    assert [r.shape_id for r in v1] == ["north"] * 21 + ["south"] * 19
    assert [r.direction_id for r in v1[:1] + v1[-1:]] == [0, 1]
    assert len({r.run_id for r in v1}) == 2
    assert all(r.confidence >= 0.8 for r in v1)
