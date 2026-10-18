from pathlib import Path

from pydantic import Field, HttpUrl, model_validator
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
