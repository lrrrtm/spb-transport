from datetime import UTC, datetime
from types import SimpleNamespace

import pytest
from pytest import MonkeyPatch
from spb_transport_spark.config import STOP_EVENT_PARAMETER_ENV, StopEventSettings
from spb_transport_spark.stop_events import StopEventParameters
from spb_transport_spark.stop_events_batch import (
    ARRIVAL_COLUMNS,
    PATTERN_COLUMNS,
    TRAVERSAL_COLUMNS,
    events_for_run,
    pattern_rows,
)
from spb_transport_spark.stop_patterns import PatternStop, StopPattern

VALUES = {
    "STOP_EVENT_ZONE_BEFORE_M": "30",
    "STOP_EVENT_ZONE_AFTER_M": "30",
    "STOP_EVENT_MAX_INTERPOLATION_GAP_SECONDS": "120",
    "STOP_EVENT_MIN_DWELL_SECONDS": "10",
    "STOP_EVENT_STOP_MAX_OFFSET_M": "40",
}


def _set_environment(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("HDFS_ROUTE_MATCHES_PATH", "hdfs://nn/silver/vehicle_route_matches")
    monkeypatch.setenv("HDFS_STATIC_PROCESSED_PATH", "hdfs://nn/processed/static_gtfs")
    monkeypatch.setenv("HDFS_STOP_PATTERNS_PATH", "hdfs://nn/silver/shape_stop_patterns/")
    monkeypatch.setenv("HDFS_STOP_ARRIVALS_PATH", "hdfs://nn/silver/stop_arrival_events")
    monkeypatch.setenv("HDFS_SEGMENT_TRAVERSALS_PATH", "hdfs://nn/silver/segment_traversals")
    for name, value in VALUES.items():
        monkeypatch.setenv(name, value)


def test_settings_read_every_parameter(monkeypatch: MonkeyPatch) -> None:
    _set_environment(monkeypatch)

    settings = StopEventSettings.from_env()

    assert settings.stop_patterns_path == "hdfs://nn/silver/shape_stop_patterns"
    assert settings.stop_max_offset_m == 40
    assert settings.parameters.max_interpolation_gap_s == 120


@pytest.mark.parametrize(
    "name",
    [
        *STOP_EVENT_PARAMETER_ENV.values(),
        "STOP_EVENT_STOP_MAX_OFFSET_M",
        "HDFS_STOP_ARRIVALS_PATH",
        "HDFS_SEGMENT_TRAVERSALS_PATH",
    ],
)
def test_settings_require_each_value(monkeypatch: MonkeyPatch, name: str) -> None:
    _set_environment(monkeypatch)
    monkeypatch.delenv(name)

    with pytest.raises(ValueError, match=name):
        StopEventSettings.from_env()


def test_settings_reject_invalid_values(monkeypatch: MonkeyPatch) -> None:
    _set_environment(monkeypatch)
    monkeypatch.setenv("STOP_EVENT_STOP_MAX_OFFSET_M", "0")
    with pytest.raises(ValueError, match="STOP_EVENT_STOP_MAX_OFFSET_M"):
        StopEventSettings.from_env()

    _set_environment(monkeypatch)
    monkeypatch.setenv("STOP_EVENT_ZONE_BEFORE_M", "-1")
    with pytest.raises(ValueError, match="must not be negative"):
        StopEventSettings.from_env()


def test_pattern_rows_keep_unprojected_stops_in_their_positions() -> None:
    pattern = StopPattern(
        pattern_id="p",
        shape_id="s",
        trip_count=3,
        trip_share=0.75,
        stops=(PatternStop("a", 0, 0.0, 2.0), PatternStop("c", 2, 900.0, 4.0)),
        unprojected_stop_ids=("b", "d"),
    )

    rows = pattern_rows([pattern])

    assert [(r[4], r[5], r[6], r[7]) for r in rows] == [
        (0, "a", True, 0.0),
        (1, "b", False, None),
        (2, "c", True, 900.0),
        (3, "d", False, None),
    ]
    assert all(len(row) == len(PATTERN_COLUMNS) for row in rows)


@pytest.mark.parametrize("columns", [ARRIVAL_COLUMNS, TRAVERSAL_COLUMNS, PATTERN_COLUMNS])
def test_output_columns_are_unique(columns: tuple[tuple[str, str], ...]) -> None:
    names = [name for name, _ in columns]
    assert len(names) == len(set(names))


def test_events_for_run_rows_match_output_schemas() -> None:
    pattern = StopPattern(
        pattern_id="p",
        shape_id="s",
        trip_count=1,
        trip_share=1.0,
        stops=(PatternStop("a", 0, 100.0, 2.0), PatternStop("b", 1, 600.0, 2.0)),
        unprojected_stop_ids=(),
    )
    rows = [
        SimpleNamespace(
            event_id=f"e{i}",
            vehicle_id="v",
            route_id="r",
            shape_id="s",
            direction_id=0,
            run_id="run",
            track_id="track",
            source_timestamp=datetime.fromtimestamp(1_790_000_000 + 10 * i, UTC),
            distance_along_m=50.0 * i,
            confidence=0.99,
        )
        for i in range(16)
    ]
    parameters = StopEventParameters(
        zone_before_m=30, zone_after_m=30, max_interpolation_gap_s=120, min_dwell_s=10
    )

    tagged = list(events_for_run(rows, {"s": pattern}, parameters, "v" * 64))
    missing_pattern = list(events_for_run(rows, {}, parameters, "v" * 64))

    assert [tag for tag, _ in tagged] == ["A", "A", "T"]
    assert all(len(row) == len(ARRIVAL_COLUMNS) for tag, row in tagged if tag == "A")
    assert all(len(row) == len(TRAVERSAL_COLUMNS) for tag, row in tagged if tag == "T")
    assert tagged[0][1][
        [n for n, _ in ARRIVAL_COLUMNS].index("passage_time")
    ] == datetime.fromtimestamp(1_790_000_020, UTC)
    assert missing_pattern == []
