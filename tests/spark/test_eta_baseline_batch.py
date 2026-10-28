from pytest import MonkeyPatch
from spb_transport_spark.config import EtaBaselineSettings


def test_settings_read_eta_baseline_inputs(monkeypatch: MonkeyPatch) -> None:
    values = {
        "HDFS_ETA_HISTORY_PATH": "hdfs://nn/processed/eta",
        "HDFS_STOP_ARRIVALS_PATH": "hdfs://nn/silver/arrivals",
        "HDFS_SEGMENT_TRAVERSALS_PATH": "hdfs://nn/silver/traversals",
        "HDFS_ETA_COMPARISON_PATH": "hdfs://nn/gold/eta/",
        "ETA_BASELINE_MAX_CURRENT_STOP_AGE_SECONDS": "600",
        "ETA_BASELINE_MAX_HORIZON_SECONDS": "3600",
        "ETA_BASELINE_MIN_SEGMENT_SAMPLES": "5",
        "CASSANDRA_HOST": "cassandra",
        "CASSANDRA_NATIVE_PORT": "9042",
        "CASSANDRA_DATACENTER": "datacenter1",
        "CASSANDRA_KEYSPACE": "spb_transport",
        "CASSANDRA_LATEST_TTL_SECONDS": "120",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)

    settings = EtaBaselineSettings.from_env()

    assert settings.output_path == "hdfs://nn/gold/eta"
    assert settings.max_horizon_seconds == 3600
    assert settings.min_segment_samples == 5
