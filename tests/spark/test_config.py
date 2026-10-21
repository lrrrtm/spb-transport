from pytest import MonkeyPatch, raises
from spb_transport_spark.config import RawArchiveSettings


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
