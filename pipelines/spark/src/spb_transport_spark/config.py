import math
import os
import re
from dataclasses import dataclass

from spb_transport_spark.route_matching import MatchingParameters
from spb_transport_spark.stop_events import StopEventParameters


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


def required_positive_int(name: str) -> int:
    value = required(name)
    try:
        parsed = int(value)
    except ValueError as error:
        raise ValueError(f"{name} must be a positive integer") from error
    if parsed <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return parsed


def required_bool(name: str) -> bool:
    value = required(name).strip().lower()
    if value not in {"true", "false"}:
        raise ValueError(f"{name} must be true or false")
    return value == "true"


@dataclass(frozen=True)
class KafkaSourceSettings:
    """Options shared by every Structured Streaming Kafka source."""

    bootstrap_servers: str
    topic: str
    starting_offsets: str
    fail_on_data_loss: bool
    max_offsets_per_trigger: int

    @classmethod
    def from_env(cls, topic_variable: str) -> "KafkaSourceSettings":
        bootstrap_servers = required("KAFKA_BOOTSTRAP_SERVERS")
        starting_offsets = required_starting_offsets()
        topic = required(topic_variable)
        fail_on_data_loss = required_bool("SPARK_KAFKA_FAIL_ON_DATA_LOSS")
        return cls(
            bootstrap_servers=bootstrap_servers,
            topic=topic,
            starting_offsets=starting_offsets,
            fail_on_data_loss=fail_on_data_loss,
            max_offsets_per_trigger=required_positive_int("SPARK_KAFKA_MAX_OFFSETS_PER_TRIGGER"),
        )

    def reader_options(self) -> dict[str, str]:
        return {
            "kafka.bootstrap.servers": self.bootstrap_servers,
            "subscribe": self.topic,
            "startingOffsets": self.starting_offsets,
            # With a bounded Kafka retention a stopped stream can resume from offsets that are
            # already deleted. false skips them (the data is gone either way) instead of
            # crash-looping; true stops the job so the loss is noticed.
            "failOnDataLoss": str(self.fail_on_data_loss).lower(),
            # Bounds a catch-up batch after downtime so it does not read the whole backlog.
            "maxOffsetsPerTrigger": str(self.max_offsets_per_trigger),
        }


@dataclass(frozen=True)
class RawArchiveSettings:
    kafka: KafkaSourceSettings
    raw_path: str
    quarantine_path: str
    checkpoint_path: str
    trigger_interval: str

    @classmethod
    def from_env(cls) -> "RawArchiveSettings":
        kafka = KafkaSourceSettings.from_env("KAFKA_RAW_RESPONSE_TOPIC")
        return cls(
            kafka=kafka,
            raw_path=required("HDFS_RAW_REALTIME_PATH"),
            quarantine_path=required("HDFS_RAW_QUARANTINE_PATH"),
            checkpoint_path=required("HDFS_RAW_CHECKPOINT_PATH"),
            trigger_interval=required("SPARK_TRIGGER_INTERVAL"),
        )


@dataclass(frozen=True)
class CassandraSettings:
    host: str
    port: int
    local_dc: str
    keyspace: str
    latest_ttl_seconds: int
    username: str | None
    password: str | None

    @classmethod
    def from_env(cls) -> "CassandraSettings":
        keyspace = required("CASSANDRA_KEYSPACE")
        if re.fullmatch(r"[a-z][a-z0-9_]*", keyspace) is None:
            raise ValueError("CASSANDRA_KEYSPACE must be a lowercase CQL identifier")
        username = os.environ.get("CASSANDRA_USERNAME", "").strip() or None
        password = os.environ.get("CASSANDRA_PASSWORD", "").strip() or None
        if (username is None) != (password is None):
            raise ValueError("CASSANDRA_USERNAME and CASSANDRA_PASSWORD must be set together")
        return cls(
            host=required("CASSANDRA_HOST"),
            port=required_positive_int("CASSANDRA_NATIVE_PORT"),
            local_dc=required("CASSANDRA_DATACENTER"),
            keyspace=keyspace,
            latest_ttl_seconds=required_positive_int("CASSANDRA_LATEST_TTL_SECONDS"),
            username=username,
            password=password,
        )


@dataclass(frozen=True)
class PositionStreamSettings:
    kafka: KafkaSourceSettings
    trigger_interval: str
    watermark: str
    max_source_age_seconds: int
    max_future_skew_seconds: int
    processed_path: str
    quarantine_path: str
    checkpoint_path: str
    cassandra: CassandraSettings

    @classmethod
    def from_env(cls) -> "PositionStreamSettings":
        kafka = KafkaSourceSettings.from_env("KAFKA_VEHICLE_POSITION_TOPIC")
        return cls(
            kafka=kafka,
            trigger_interval=required("SPARK_TRIGGER_INTERVAL"),
            watermark=required("SPARK_POSITION_WATERMARK"),
            max_source_age_seconds=required_positive_int("SPARK_POSITION_MAX_SOURCE_AGE_SECONDS"),
            max_future_skew_seconds=required_positive_int("SPARK_POSITION_MAX_FUTURE_SKEW_SECONDS"),
            processed_path=required("HDFS_POSITION_PROCESSED_PATH"),
            quarantine_path=required("HDFS_POSITION_QUARANTINE_PATH"),
            checkpoint_path=required("HDFS_POSITION_CHECKPOINT_PATH"),
            cassandra=CassandraSettings.from_env(),
        )


@dataclass(frozen=True)
class EtaStreamSettings:
    kafka: KafkaSourceSettings
    trigger_interval: str
    watermark: str
    history_path: str
    quarantine_path: str
    checkpoint_path: str
    cassandra: CassandraSettings

    @classmethod
    def from_env(cls) -> "EtaStreamSettings":
        kafka = KafkaSourceSettings.from_env("KAFKA_VEHICLE_ETA_TOPIC")
        return cls(
            kafka=kafka,
            trigger_interval=required("SPARK_TRIGGER_INTERVAL"),
            watermark=required("SPARK_ETA_WATERMARK"),
            history_path=required("HDFS_ETA_HISTORY_PATH"),
            quarantine_path=required("HDFS_ETA_QUARANTINE_PATH"),
            checkpoint_path=required("HDFS_ETA_CHECKPOINT_PATH"),
            cassandra=CassandraSettings.from_env(),
        )


def required_finite_float(name: str) -> float:
    value = required(name)
    try:
        parsed = float(value)
    except ValueError as error:
        raise ValueError(f"{name} must be a number") from error
    if not math.isfinite(parsed):
        raise ValueError(f"{name} must be a finite number")
    return parsed


ROUTE_MATCH_PARAMETER_ENV = {
    "max_offset_m": "ROUTE_MATCH_MAX_OFFSET_M",
    "gps_sigma_m": "ROUTE_MATCH_GPS_SIGMA_M",
    "outlier_offset_m": "ROUTE_MATCH_OUTLIER_OFFSET_M",
    "bearing_weight": "ROUTE_MATCH_BEARING_WEIGHT",
    "bearing_min_speed": "ROUTE_MATCH_BEARING_MIN_SPEED",
    "max_backtrack_m": "ROUTE_MATCH_MAX_BACKTRACK_M",
    "max_speed_mps": "ROUTE_MATCH_MAX_SPEED_MPS",
    "transition_beta_m": "ROUTE_MATCH_TRANSITION_BETA_M",
    "switch_cost": "ROUTE_MATCH_SWITCH_COST",
    "terminal_restart_cost": "ROUTE_MATCH_TERMINAL_RESTART_COST",
    "off_route_transition_cost": "ROUTE_MATCH_OFF_ROUTE_TRANSITION_COST",
    "terminal_zone_m": "ROUTE_MATCH_TERMINAL_ZONE_M",
    "max_gap_seconds": "ROUTE_MATCH_MAX_GAP_SECONDS",
    "min_confidence": "ROUTE_MATCH_MIN_CONFIDENCE",
}


def matching_parameters_from_env() -> MatchingParameters:
    return MatchingParameters(
        **{field: required_finite_float(env) for field, env in ROUTE_MATCH_PARAMETER_ENV.items()}
    )


@dataclass(frozen=True)
class RouteMatchingSettings:
    positions_path: str
    static_processed_path: str
    matches_path: str
    parameters: MatchingParameters

    @classmethod
    def from_env(cls) -> "RouteMatchingSettings":
        positions_path = required("HDFS_POSITION_PROCESSED_PATH").rstrip("/")
        static_processed_path = required("HDFS_STATIC_PROCESSED_PATH").rstrip("/")
        matches_path = required("HDFS_ROUTE_MATCHES_PATH").rstrip("/")
        return cls(
            positions_path=positions_path,
            static_processed_path=static_processed_path,
            matches_path=matches_path,
            parameters=matching_parameters_from_env(),
        )


STOP_EVENT_PARAMETER_ENV = {
    "zone_before_m": "STOP_EVENT_ZONE_BEFORE_M",
    "zone_after_m": "STOP_EVENT_ZONE_AFTER_M",
    "max_interpolation_gap_s": "STOP_EVENT_MAX_INTERPOLATION_GAP_SECONDS",
    "stopped_max_speed_mps": "STOP_EVENT_STOPPED_MAX_SPEED_MPS",
}


@dataclass(frozen=True)
class StopEventSettings:
    matches_path: str
    static_processed_path: str
    stop_patterns_path: str
    stop_arrivals_path: str
    segment_traversals_path: str
    stop_max_offset_m: float
    parameters: StopEventParameters

    @classmethod
    def from_env(cls) -> "StopEventSettings":
        matches_path = required("HDFS_ROUTE_MATCHES_PATH").rstrip("/")
        static_processed_path = required("HDFS_STATIC_PROCESSED_PATH").rstrip("/")
        stop_patterns_path = required("HDFS_STOP_PATTERNS_PATH").rstrip("/")
        stop_arrivals_path = required("HDFS_STOP_ARRIVALS_PATH").rstrip("/")
        segment_traversals_path = required("HDFS_SEGMENT_TRAVERSALS_PATH").rstrip("/")
        stop_max_offset_m = required_finite_float("STOP_EVENT_STOP_MAX_OFFSET_M")
        if stop_max_offset_m <= 0:
            raise ValueError("STOP_EVENT_STOP_MAX_OFFSET_M must be positive")
        return cls(
            matches_path=matches_path,
            static_processed_path=static_processed_path,
            stop_patterns_path=stop_patterns_path,
            stop_arrivals_path=stop_arrivals_path,
            segment_traversals_path=segment_traversals_path,
            stop_max_offset_m=stop_max_offset_m,
            parameters=StopEventParameters(
                **{
                    field: required_finite_float(env)
                    for field, env in STOP_EVENT_PARAMETER_ENV.items()
                }
            ),
        )
