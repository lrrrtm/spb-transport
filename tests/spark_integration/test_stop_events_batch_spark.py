"""End-to-end check of the T10 batch job on a local Spark session (`make test-spark`)."""

from datetime import date
from pathlib import Path

import pytest
from pytest import MonkeyPatch
from spb_transport_spark import route_match_batch, stop_events_batch
from spb_transport_spark.config import RouteMatchingSettings, StopEventSettings

from spark_integration.spark_lake import VERSION, local_spark

pytestmark = pytest.mark.spark

STOP_EVENT_ENV = {
    "STOP_EVENT_STOP_MAX_OFFSET_M": "40",
    "STOP_EVENT_ZONE_BEFORE_M": "40",
    "STOP_EVENT_ZONE_AFTER_M": "40",
    "STOP_EVENT_MAX_INTERPOLATION_GAP_SECONDS": "120",
    "STOP_EVENT_STOPPED_MAX_SPEED_MPS": "0.5",
}


def test_stop_events_job_is_correct_and_idempotent(lake: Path, monkeypatch: MonkeyPatch) -> None:
    for name in ("STOP_PATTERNS", "STOP_ARRIVALS", "SEGMENT_TRAVERSALS"):
        monkeypatch.setenv(f"HDFS_{name}_PATH", str(lake / name.lower()))
    for name, value in STOP_EVENT_ENV.items():
        monkeypatch.setenv(name, value)
    route_match_batch.run_job(RouteMatchingSettings.from_env(), date(2026, 10, 1), VERSION)
    settings = StopEventSettings.from_env()

    local_spark()
    first = stop_events_batch.run_job(settings, date(2026, 10, 1), VERSION)
    local_spark()
    second = stop_events_batch.run_job(settings, date(2026, 10, 1), VERSION)

    assert first == second
    assert first["patterns"] == 2 and first["patterns_with_unprojected_stops"] == 1
    spark = local_spark()
    arrivals = spark.read.parquet(str(lake / "stop_arrivals")).collect()
    traversals = spark.read.parquet(str(lake / "segment_traversals")).collect()
    patterns = spark.read.parquet(str(lake / "stop_patterns")).collect()

    # The vehicle starts at N1 (departure only), passes N2, ends the outbound run on N3 and
    # starts the return run past S1's zone, so only S2 is seen on the way back.
    events = sorted((a.passage_time or a.departure_time, a.stop_id, a.method) for a in arrivals)
    assert [(stop, method) for _, stop, method in events] == [
        ("N1", "departure_only"),
        ("N2", "observed"),
        ("N3", "observed"),
        ("S2", "observed"),
    ]
    assert len({a.event_id for a in arrivals}) == 4
    assert {a.service_date.isoformat() for a in arrivals} == {"2026-10-01"}
    assert sorted((t.from_stop_id, t.to_stop_id) for t in traversals) == [
        ("N1", "N2"),
        ("N2", "N3"),
    ]
    n2_n3 = next(t for t in traversals if t.from_stop_id == "N2")
    assert n2_n3.travel_time_s == pytest.approx(150)
    assert [(p.stop_id, p.projected) for p in patterns if p.shape_id == "south"] == [
        ("S1", True),
        ("S2", True),
        ("FAR", False),
        ("S3", True),
    ]
