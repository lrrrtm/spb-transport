import pytest
from pytest import MonkeyPatch
from spb_transport_spark.cassandra_sink import STATIC_TABLES
from spb_transport_spark.static_serving import StaticServingSettings, StopInfo, stop_rows
from spb_transport_spark.stop_patterns import PatternStop, StopPattern

VERSION = "v" * 64
PATTERN = StopPattern(
    pattern_id="p1",
    shape_id="north",
    trip_count=3,
    trip_share=1.0,
    stops=(PatternStop("a", 0, 0.0, 1.0), PatternStop("c", 2, 900.0, 2.0)),
    unprojected_stop_ids=("b",),
)
STOPS = {
    "a": StopInfo("101", "Невский пр.", 59.93, 30.31),
    "b": StopInfo(None, "Далеко", 59.94, 30.40),
    "c": StopInfo("103", "Сенная пл.", 59.94, 30.32),
}


def test_stop_rows_follow_pattern_order_and_keep_unprojected_stops() -> None:
    rows = stop_rows(
        VERSION,
        [("r1", "north"), ("r1", "north"), ("r1", "no-pattern")],
        [("north", 0), ("north", 0)],
        {"north": PATTERN},
        STOPS,
    )

    assert [(r[1], r[2], r[3], r[6], r[11]) for r in rows] == [
        ("r1", "north", 0, "a", 0.0),
        ("r1", "north", 1, "b", None),
        ("r1", "north", 2, "c", 900.0),
    ]
    assert {r[4] for r in rows} == {0}
    assert all(len(r) == len(STATIC_TABLES[1].columns) for r in rows)


def test_stop_rows_skip_unknown_stops_and_ambiguous_directions() -> None:
    rows = stop_rows(
        VERSION,
        [("r1", "north")],
        [("north", 0), ("north", 1)],
        {"north": PATTERN},
        {"a": STOPS["a"]},
    )

    assert [r[6] for r in rows] == ["a"]
    assert rows[0][4] is None


def test_settings_require_stop_offset(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("HDFS_STATIC_PROCESSED_PATH", "hdfs://nn/processed/static_gtfs/")
    monkeypatch.setenv("STOP_EVENT_STOP_MAX_OFFSET_M", "0")
    for name, value in {
        "CASSANDRA_HOST": "cassandra",
        "CASSANDRA_NATIVE_PORT": "9042",
        "CASSANDRA_DATACENTER": "datacenter1",
        "CASSANDRA_KEYSPACE": "spb_transport",
        "CASSANDRA_LATEST_TTL_SECONDS": "120",
    }.items():
        monkeypatch.setenv(name, value)

    with pytest.raises(ValueError, match="STOP_EVENT_STOP_MAX_OFFSET_M"):
        StaticServingSettings.from_env()

    monkeypatch.setenv("STOP_EVENT_STOP_MAX_OFFSET_M", "40")
    settings = StaticServingSettings.from_env()
    assert settings.static_processed_path == "hdfs://nn/processed/static_gtfs"
    assert settings.cassandra.keyspace == "spb_transport"
