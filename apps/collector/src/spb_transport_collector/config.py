from pathlib import Path

from pydantic import Field, HttpUrl, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class GtfsHttpSettings(BaseSettings):
    model_config = SettingsConfigDict(extra="ignore", populate_by_name=True)

    base_url: HttpUrl = Field(validation_alias="GTFS_REALTIME_BASE_URL")
    tls_verify: bool = Field(default=True, validation_alias="GTFS_TLS_VERIFY")
    ca_bundle: Path | None = Field(default=None, validation_alias="GTFS_CA_BUNDLE")
    connect_timeout_seconds: float = Field(gt=0, validation_alias="GTFS_CONNECT_TIMEOUT_SECONDS")
    read_timeout_seconds: float = Field(gt=0, validation_alias="GTFS_READ_TIMEOUT_SECONDS")
    write_timeout_seconds: float = Field(gt=0, validation_alias="GTFS_WRITE_TIMEOUT_SECONDS")
    pool_timeout_seconds: float = Field(gt=0, validation_alias="GTFS_POOL_TIMEOUT_SECONDS")
    retry_max_attempts: int = Field(ge=1, validation_alias="GTFS_RETRY_MAX_ATTEMPTS")
    retry_backoff_base_seconds: float = Field(
        ge=0, validation_alias="GTFS_RETRY_BACKOFF_BASE_SECONDS"
    )
    retry_backoff_max_seconds: float = Field(
        ge=0, validation_alias="GTFS_RETRY_BACKOFF_MAX_SECONDS"
    )
    retry_jitter_seconds: float = Field(ge=0, validation_alias="GTFS_RETRY_JITTER_SECONDS")

    @field_validator("ca_bundle", mode="before")
    @classmethod
    def empty_ca_bundle_is_unset(cls, value: object) -> object:
        return None if value == "" else value

    @model_validator(mode="after")
    def validate_tls_and_backoff(self) -> "GtfsHttpSettings":
        if self.ca_bundle is not None:
            if not self.tls_verify:
                raise ValueError("GTFS_CA_BUNDLE cannot be used when GTFS_TLS_VERIFY=false")
            if not self.ca_bundle.is_file():
                raise ValueError(f"GTFS_CA_BUNDLE is not a readable file: {self.ca_bundle}")
        if self.retry_backoff_max_seconds < self.retry_backoff_base_seconds:
            raise ValueError(
                "GTFS_RETRY_BACKOFF_MAX_SECONDS must be greater than or equal to "
                "GTFS_RETRY_BACKOFF_BASE_SECONDS"
            )
        return self


class VehicleCollectorSettings(GtfsHttpSettings):
    vehicle_poll_interval_seconds: float = Field(
        gt=0, validation_alias="GTFS_VEHICLE_POLL_INTERVAL_SECONDS"
    )
    vehicle_trips_poll_interval_seconds: float = Field(
        gt=0, validation_alias="GTFS_VEHICLE_TRIPS_POLL_INTERVAL_SECONDS"
    )
    vehicle_trips_batch_size: int = Field(gt=0, validation_alias="GTFS_VEHICLE_TRIPS_BATCH_SIZE")
    kafka_bootstrap_servers: str = Field(min_length=1, validation_alias="KAFKA_BOOTSTRAP_SERVERS")
    kafka_vehicle_position_topic: str = Field(
        min_length=1, validation_alias="KAFKA_VEHICLE_POSITION_TOPIC"
    )
    kafka_vehicle_eta_topic: str = Field(min_length=1, validation_alias="KAFKA_VEHICLE_ETA_TOPIC")
    kafka_raw_response_topic: str = Field(min_length=1, validation_alias="KAFKA_RAW_RESPONSE_TOPIC")
    kafka_client_id: str = Field(min_length=1, validation_alias="KAFKA_CLIENT_ID")
    kafka_linger_ms: int = Field(ge=0, validation_alias="KAFKA_LINGER_MS")
    kafka_max_batch_size_bytes: int = Field(gt=0, validation_alias="KAFKA_MAX_BATCH_SIZE_BYTES")
    kafka_max_request_size_bytes: int = Field(gt=0, validation_alias="KAFKA_MAX_REQUEST_SIZE_BYTES")
    kafka_request_timeout_ms: int = Field(gt=0, validation_alias="KAFKA_REQUEST_TIMEOUT_MS")
