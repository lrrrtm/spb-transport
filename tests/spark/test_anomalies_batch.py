import pytest
from pytest import MonkeyPatch
from spb_transport_spark.config import AnomalySettings


def _environment(monkeypatch: MonkeyPatch) -> None:
    values = {
        "HDFS_ROUTE_MATCHES_PATH": "hdfs://nn/silver/matches",
        "HDFS_STOP_ARRIVALS_PATH": "hdfs://nn/silver/arrivals",
        "HDFS_TRANSPORT_ANOMALIES_PATH": "hdfs://nn/gold/anomalies/",
        "ANOMALY_BUNCHING_DISTANCE_M": "300",
        "ANOMALY_SNAPSHOT_SECONDS": "30",
        "ANOMALY_SHORT_HEADWAY_SECONDS": "120",
        "ANOMALY_LONG_HEADWAY_SECONDS": "900",
        "ANOMALY_STATIONARY_SPEED_MPS": "0.5",
        "ANOMALY_STATIONARY_PROGRESS_M": "10",
        "ANOMALY_STATIONARY_DURATION_SECONDS": "300",
        "ANOMALY_MAX_OBSERVATION_GAP_SECONDS": "60",
        "ANOMALY_DISAPPEARANCE_SECONDS": "300",
        "ANOMALY_TERMINAL_ZONE_M": "300",
        "ANOMALY_GPS_JUMP_SPEED_MPS": "50",
        "ANOMALY_SHAPE_OFFSET_M": "100",
        "CASSANDRA_HOST": "cassandra",
        "CASSANDRA_NATIVE_PORT": "9042",
        "CASSANDRA_DATACENTER": "datacenter1",
        "CASSANDRA_KEYSPACE": "spb_transport",
        "CASSANDRA_LATEST_TTL_SECONDS": "120",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def test_settings_read_anomaly_thresholds(monkeypatch: MonkeyPatch) -> None:
    _environment(monkeypatch)

    settings = AnomalySettings.from_env()

    assert settings.anomalies_path == "hdfs://nn/gold/anomalies"
    assert settings.bunching_distance_m == 300
    assert settings.stationary_duration_seconds == 300


def test_short_headway_must_be_less_than_long(monkeypatch: MonkeyPatch) -> None:
    _environment(monkeypatch)
    monkeypatch.setenv("ANOMALY_SHORT_HEADWAY_SECONDS", "900")

    with pytest.raises(ValueError, match="SHORT_HEADWAY"):
        AnomalySettings.from_env()
