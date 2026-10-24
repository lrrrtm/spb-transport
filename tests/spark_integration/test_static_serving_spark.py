"""Static metadata publication on a local Spark session; Cassandra writes are captured."""

from pathlib import Path
from typing import Any

import pytest
from conftest import VERSION, local_spark
from pytest import MonkeyPatch
from spb_transport_spark import static_serving
from spb_transport_spark.config import CassandraSettings

pytestmark = pytest.mark.spark

CASSANDRA = CassandraSettings("cassandra", 9042, "dc1", "spb_transport", 120, None, None)


def test_publish_writes_routes_stops_and_switches_feed_last(
    lake: Path, monkeypatch: MonkeyPatch
) -> None:
    spark = local_spark()
    spark.createDataFrame(
        [("r1", "a1", "1", "Route one", 3)],
        "route_id string, agency_id string, route_short_name string,"
        " route_long_name string, route_type int",
    ).write.parquet(str(lake / f"static/version={VERSION}/routes"))
    written: list[tuple[str, list[Any]]] = []
    monkeypatch.setattr(
        static_serving,
        "write_static_table",
        lambda frame, _settings, table: written.append((table.name, frame.collect())),
    )
    settings = static_serving.StaticServingSettings(str(lake / "static"), 40.0, CASSANDRA)

    counters = static_serving.publish(spark, settings, VERSION)

    assert [name for name, _ in written] == [
        "routes_by_feed",
        "stops_by_route",
        "static_feed_current",
    ]
    routes, stops, current = (rows for _, rows in written)
    assert [(r.static_feed_version, r.route_id, r.route_type) for r in routes] == [
        (VERSION, "r1", 3)
    ]
    by_shape: dict[str, list[str]] = {}
    for row in sorted(stops, key=lambda r: (r.shape_id, r.stop_index)):
        by_shape.setdefault(row.shape_id, []).append(row.stop_id)
    assert by_shape == {"north": ["N1", "N2", "N3"], "south": ["S1", "S2", "FAR", "S3"]}
    assert {r.direction_id for r in stops if r.shape_id == "south"} == {1}
    assert [(r.feed, r.static_feed_version) for r in current] == [("gtfs", VERSION)]
    assert counters == {"routes": 1, "route_stops": 7, "routes_with_stops": 1}
