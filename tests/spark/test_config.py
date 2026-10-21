from pytest import MonkeyPatch, raises
from spb_transport_spark.config import PositionStreamSettings, RawArchiveSettings


def test_raw_archive_settings_require_every_value(monkeypatch: MonkeyPatch) -> None:
    for name in (
        "KAFKA_BOOTSTRAP_SERVERS",
        "KAFKA_RAW_RESPONSE_TOPIC",
        "SPARK_KAFKA_STARTING_OFFSETS",
        "SPARK_TRIGGER_INTERVAL",
        "HDFS_RAW_REALTIME_PATH",
        "HDFS_RAW_QUARANTINE_PATH",
        "HDFS_RAW_CHECKPOINT_PATH",
    ):
        monkeypatch.delenv(name, raising=False)

    with raises(ValueError, match="KAFKA_BOOTSTRAP_SERVERS"):
        RawArchiveSettings.from_env()


def test_raw_archive_settings_reject_invalid_starting_offsets(monkeypatch: MonkeyPatch) -> None:
    monkeypatch.setenv("SPARK_KAFKA_STARTING_OFFSETS", "somewhere")
    monkeypatch.setenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:29092")

    with raises(ValueError, match="earliest or latest"):
        RawArchiveSettings.from_env()


def _set_position_environment(monkeypatch: MonkeyPatch) -> None:
    values = {
        "KAFKA_BOOTSTRAP_SERVERS": "kafka:29092",
        "KAFKA_VEHICLE_POSITION_TOPIC": "vehicle-position.v1",
        "SPARK_KAFKA_STARTING_OFFSETS": "earliest",
        "SPARK_TRIGGER_INTERVAL": "10 seconds",
        "SPARK_POSITION_WATERMARK": "10 minutes",
        "SPARK_POSITION_MAX_SOURCE_AGE_SECONDS": "300",
        "SPARK_POSITION_MAX_FUTURE_SKEW_SECONDS": "30",
        "HDFS_POSITION_PROCESSED_PATH": "hdfs://namenode/processed/positions",
        "HDFS_POSITION_QUARANTINE_PATH": "hdfs://namenode/quarantine/positions",
        "HDFS_POSITION_CHECKPOINT_PATH": "hdfs://namenode/checkpoints/positions",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def test_position_stream_settings_load_explicit_values(monkeypatch: MonkeyPatch) -> None:
    _set_position_environment(monkeypatch)

    settings = PositionStreamSettings.from_env()

    assert settings.watermark == "10 minutes"
    assert settings.max_source_age_seconds == 300
    assert settings.max_future_skew_seconds == 30


def test_position_stream_settings_reject_non_positive_threshold(
    monkeypatch: MonkeyPatch,
) -> None:
    _set_position_environment(monkeypatch)
    monkeypatch.setenv("SPARK_POSITION_MAX_SOURCE_AGE_SECONDS", "0")

    with raises(ValueError, match="positive integer"):
        PositionStreamSettings.from_env()
