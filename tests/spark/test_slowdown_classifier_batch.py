import pytest
from pytest import MonkeyPatch
from spb_transport_spark.config import SlowdownClassifierSettings


def _environment(monkeypatch: MonkeyPatch) -> None:
    values = {
        "HDFS_SEGMENT_TRAVERSALS_PATH": "hdfs://nn/silver/traversals",
        "HDFS_SLOWDOWN_CLASSIFIER_PATH": "hdfs://nn/gold/slowdown/",
        "SLOWDOWN_MIN_BASELINE_SAMPLES": "10",
        "SLOWDOWN_CLASSIFIER_RISK_THRESHOLD": "1.1",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def test_settings_read_classifier_parameters(monkeypatch: MonkeyPatch) -> None:
    _environment(monkeypatch)

    settings = SlowdownClassifierSettings.from_env()

    assert settings.output_path == "hdfs://nn/gold/slowdown"
    assert settings.min_baseline_samples == 10
    assert settings.risk_threshold == 1.1


def test_settings_reject_non_positive_threshold(monkeypatch: MonkeyPatch) -> None:
    _environment(monkeypatch)
    monkeypatch.setenv("SLOWDOWN_CLASSIFIER_RISK_THRESHOLD", "0")

    with pytest.raises(ValueError, match="RISK_THRESHOLD"):
        SlowdownClassifierSettings.from_env()
