import argparse

import pytest
from pytest import MonkeyPatch
from spb_transport_spark.config import ROUTE_MATCH_PARAMETER_ENV, RouteMatchingSettings
from spb_transport_spark.route_match_batch import (
    OUTPUT_COLUMNS,
    build_shapes_by_route,
    static_version_argument,
    unique_directions,
)

PARAMETER_VALUES = {
    "max_offset_m": "75",
    "gps_sigma_m": "15",
    "outlier_offset_m": "50",
    "bearing_weight": "2",
    "bearing_min_speed": "1",
    "max_backtrack_m": "30",
    "max_speed_mps": "25",
    "transition_beta_m": "50",
    "switch_cost": "8",
    "terminal_restart_cost": "2",
    "off_route_transition_cost": "3",
    "terminal_zone_m": "300",
    "max_gap_seconds": "300",
    "min_confidence": "0.8",
}


def _set_environment(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("HDFS_POSITION_PROCESSED_PATH", "hdfs://nn/processed/positions/")
    monkeypatch.setenv("HDFS_STATIC_PROCESSED_PATH", "hdfs://nn/processed/static_gtfs")
    monkeypatch.setenv("HDFS_ROUTE_MATCHES_PATH", "hdfs://nn/silver/vehicle_route_matches")
    for field, name in ROUTE_MATCH_PARAMETER_ENV.items():
        monkeypatch.setenv(name, PARAMETER_VALUES[field])


def test_settings_read_every_matching_parameter(monkeypatch: MonkeyPatch) -> None:
    _set_environment(monkeypatch)

    settings = RouteMatchingSettings.from_env()

    assert settings.positions_path == "hdfs://nn/processed/positions"
    assert settings.parameters.min_confidence == 0.8
    assert settings.parameters.max_gap_seconds == 300


@pytest.mark.parametrize("name", sorted(ROUTE_MATCH_PARAMETER_ENV.values()))
def test_settings_require_each_parameter(monkeypatch: MonkeyPatch, name: str) -> None:
    _set_environment(monkeypatch)
    monkeypatch.delenv(name)

    with pytest.raises(ValueError, match=name):
        RouteMatchingSettings.from_env()


@pytest.mark.parametrize("value", ["abc", "nan", "inf"])
def test_settings_reject_non_finite_parameter(monkeypatch: MonkeyPatch, value: str) -> None:
    _set_environment(monkeypatch)
    monkeypatch.setenv("ROUTE_MATCH_GPS_SIGMA_M", value)

    with pytest.raises(ValueError, match="ROUTE_MATCH_GPS_SIGMA_M"):
        RouteMatchingSettings.from_env()


def test_settings_reject_out_of_range_parameter(monkeypatch: MonkeyPatch) -> None:
    _set_environment(monkeypatch)
    monkeypatch.setenv("ROUTE_MATCH_MIN_CONFIDENCE", "1.5")

    with pytest.raises(ValueError, match="min_confidence"):
        RouteMatchingSettings.from_env()


def test_shapes_are_ordered_by_sequence_and_grouped_by_route() -> None:
    points = [
        ("a", 2, 59.931, 30.31),
        ("a", 1, 59.930, 30.31),
        ("b", 1, 59.930, 30.32),
        ("b", 2, 59.931, 30.32),
        ("single", 1, 59.93, 30.33),
    ]
    routes = [("r1", "a"), ("r1", "b"), ("r1", "a"), ("r2", "single"), ("r3", "missing")]

    by_route = build_shapes_by_route(points, routes)

    assert sorted(by_route) == ["r1"]
    assert [s.shape_id for s in by_route["r1"]] == ["a", "b"]
    [projection] = by_route["r1"][0].candidates(59.9305, 30.31, max_offset_m=20)
    assert projection.segment_bearing_deg == pytest.approx(0, abs=0.5)


def test_direction_is_kept_only_when_all_trips_agree() -> None:
    directions = unique_directions([("a", 0), ("a", 0), ("b", 0), ("b", 1), ("c", None), ("d", 1)])

    assert directions == {"a": 0, "d": 1}


def test_static_version_must_be_sha256() -> None:
    assert static_version_argument("a" * 64) == "a" * 64
    with pytest.raises(argparse.ArgumentTypeError):
        static_version_argument("../../etc")


def test_output_columns_are_unique() -> None:
    names = [name for name, _ in OUTPUT_COLUMNS]
    assert len(names) == len(set(names))
