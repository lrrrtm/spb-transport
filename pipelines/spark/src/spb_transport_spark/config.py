import os
from dataclasses import dataclass


def required(name: str) -> str:
    value = os.environ.get(name)
    if value is None or not value.strip():
        raise ValueError(f"required environment variable {name} is missing or empty")
    return value


def required_starting_offsets() -> str:
    value = required("SPARK_KAFKA_STARTING_OFFSETS")
    if value not in {"earliest", "latest"}:
        raise ValueError("SPARK_KAFKA_STARTING_OFFSETS must be earliest or latest")
    return value


@dataclass(frozen=True)
class RawArchiveSettings:
    kafka_bootstrap_servers: str
    kafka_topic: str
    starting_offsets: str
    raw_path: str
    quarantine_path: str
    checkpoint_path: str
    trigger_interval: str

    @classmethod
    def from_env(cls) -> "RawArchiveSettings":
        kafka_bootstrap_servers = required("KAFKA_BOOTSTRAP_SERVERS")
        starting_offsets = required_starting_offsets()
        return cls(
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            kafka_topic=required("KAFKA_RAW_RESPONSE_TOPIC"),
            starting_offsets=starting_offsets,
            raw_path=required("HDFS_RAW_REALTIME_PATH"),
            quarantine_path=required("HDFS_RAW_QUARANTINE_PATH"),
            checkpoint_path=required("HDFS_RAW_CHECKPOINT_PATH"),
            trigger_interval=required("SPARK_TRIGGER_INTERVAL"),
        )


def required_positive_int(name: str) -> int:
    value = required(name)
    try:
        parsed = int(value)
    except ValueError as error:
        raise ValueError(f"{name} must be a positive integer") from error
    if parsed <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return parsed


@dataclass(frozen=True)
class PositionStreamSettings:
    kafka_bootstrap_servers: str
    kafka_topic: str
    starting_offsets: str
    trigger_interval: str
    watermark: str
    max_source_age_seconds: int
    max_future_skew_seconds: int
    processed_path: str
    quarantine_path: str
    checkpoint_path: str

    @classmethod
    def from_env(cls) -> "PositionStreamSettings":
        kafka_bootstrap_servers = required("KAFKA_BOOTSTRAP_SERVERS")
        starting_offsets = required_starting_offsets()
        return cls(
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            kafka_topic=required("KAFKA_VEHICLE_POSITION_TOPIC"),
            starting_offsets=starting_offsets,
            trigger_interval=required("SPARK_TRIGGER_INTERVAL"),
            watermark=required("SPARK_POSITION_WATERMARK"),
            max_source_age_seconds=required_positive_int("SPARK_POSITION_MAX_SOURCE_AGE_SECONDS"),
            max_future_skew_seconds=required_positive_int("SPARK_POSITION_MAX_FUTURE_SKEW_SECONDS"),
            processed_path=required("HDFS_POSITION_PROCESSED_PATH"),
            quarantine_path=required("HDFS_POSITION_QUARANTINE_PATH"),
            checkpoint_path=required("HDFS_POSITION_CHECKPOINT_PATH"),
        )


@dataclass(frozen=True)
class EtaStreamSettings:
    kafka_bootstrap_servers: str
    kafka_topic: str
    starting_offsets: str
    trigger_interval: str
    watermark: str
    history_path: str
    quarantine_path: str
    checkpoint_path: str

    @classmethod
    def from_env(cls) -> "EtaStreamSettings":
        kafka_bootstrap_servers = required("KAFKA_BOOTSTRAP_SERVERS")
        starting_offsets = required_starting_offsets()
        return cls(
            kafka_bootstrap_servers=kafka_bootstrap_servers,
            kafka_topic=required("KAFKA_VEHICLE_ETA_TOPIC"),
            starting_offsets=starting_offsets,
            trigger_interval=required("SPARK_TRIGGER_INTERVAL"),
            watermark=required("SPARK_ETA_WATERMARK"),
            history_path=required("HDFS_ETA_HISTORY_PATH"),
            quarantine_path=required("HDFS_ETA_QUARANTINE_PATH"),
            checkpoint_path=required("HDFS_ETA_CHECKPOINT_PATH"),
        )
