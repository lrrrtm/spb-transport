import pytest
from pytest import MonkeyPatch
from spb_transport_spark.config import SegmentMetricsSettings


def _environment(monkeypatch: MonkeyPatch) -> None:
    values = {
        "HDFS_SEGMENT_TRAVERSALS_PATH": "hdfs://nn/silver/segment_traversals/",
        "HDFS_SEGMENT_METRICS_PATH": "hdfs://nn/gold/segment_metrics/",
        "SEGMENT_METRICS_MIN_OBSERVATIONS": "3",
        "SEGMENT_METRICS_MAX_GAP_SECONDS": "60",
        "SEGMENT_METRICS_MIN_CONFIDENCE": "0.8",
        "SEGMENT_METRICS_MIN_SAMPLES": "10",
        "CASSANDRA_HOST": "cassandra",
        "CASSANDRA_NATIVE_PORT": "9042",
        "CASSANDRA_DATACENTER": "datacenter1",
        "CASSANDRA_KEYSPACE": "spb_transport",
        "CASSANDRA_LATEST_TTL_SECONDS": "120",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def test_settings_read_quality_thresholds(monkeypatch: MonkeyPatch) -> None:
    _environment(monkeypatch)

    settings = SegmentMetricsSettings.from_env()

    assert settings.segment_metrics_path == "hdfs://nn/gold/segment_metrics"
    assert settings.min_observations == 3
    assert settings.max_gap_seconds == 60
    assert settings.min_confidence == 0.8
    assert settings.min_samples == 10


@pytest.mark.parametrize(
    ("name", "value"),
    [
        ("SEGMENT_METRICS_MAX_GAP_SECONDS", "0"),
        ("SEGMENT_METRICS_MIN_CONFIDENCE", "1.1"),
        ("SEGMENT_METRICS_MIN_OBSERVATIONS", "0"),
    ],
)
def test_settings_reject_invalid_thresholds(
    monkeypatch: MonkeyPatch, name: str, value: str
) -> None:
    _environment(monkeypatch)
    monkeypatch.setenv(name, value)

    with pytest.raises(ValueError, match=name):
        SegmentMetricsSettings.from_env()
