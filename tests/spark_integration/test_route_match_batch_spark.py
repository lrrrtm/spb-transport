"""End-to-end check of the T09 batch job on a local Spark session (`make test-spark`)."""

from datetime import date
from pathlib import Path

import pytest
from spb_transport_spark.config import RouteMatchingSettings
from spb_transport_spark.route_match_batch import run_job

from spark_integration.spark_lake import VERSION, local_spark

pytestmark = pytest.mark.spark


def test_route_matching_job_is_correct_and_idempotent(lake: Path) -> None:
    settings = RouteMatchingSettings.from_env()

    first = run_job(settings, date(2026, 10, 1), VERSION)
    local_spark()  # run_job stops its session; the next run must start from scratch
    second = run_job(settings, date(2026, 10, 1), VERSION)

    assert first == second == {"matched": 40, "no_static_shape": 40}
    matches = local_spark().read.parquet(str(lake / "matches"))
    rows = matches.collect()
    assert len(rows) == 80
    assert {r.algorithm_version for r in rows} == {"route-match-hmm-v1"}
    assert {r.static_feed_version for r in rows} == {VERSION}
    v1 = sorted((r for r in rows if r.vehicle_id == "v1"), key=lambda r: r.source_timestamp)
    assert [r.shape_id for r in v1] == ["north"] * 21 + ["south"] * 19
    assert [r.direction_id for r in v1[:1] + v1[-1:]] == [0, 1]
    assert len({r.run_id for r in v1}) == 2
    assert all(r.confidence >= 0.8 for r in v1)
