from pydantic import Field, SecretStr, field_validator, model_validator
from pydantic_settings import BaseSettings, SettingsConfigDict


class ApiSettings(BaseSettings):
    """API configuration from the environment; every value except credentials is required."""

    model_config = SettingsConfigDict(extra="ignore", populate_by_name=True)

    host: str = Field(min_length=1, validation_alias="API_HOST")
    port: int = Field(gt=0, lt=65536, validation_alias="API_PORT")
    cassandra_host: str = Field(min_length=1, validation_alias="CASSANDRA_HOST")
    cassandra_port: int = Field(gt=0, lt=65536, validation_alias="CASSANDRA_NATIVE_PORT")
    cassandra_datacenter: str = Field(min_length=1, validation_alias="CASSANDRA_DATACENTER")
    cassandra_keyspace: str = Field(
        pattern=r"^[a-z][a-z0-9_]*$", validation_alias="CASSANDRA_KEYSPACE"
    )
    cassandra_username: str | None = Field(default=None, validation_alias="CASSANDRA_USERNAME")
    cassandra_password: SecretStr | None = Field(
        default=None, validation_alias="CASSANDRA_PASSWORD"
    )
    cassandra_request_timeout_seconds: float = Field(
        gt=0, validation_alias="API_CASSANDRA_REQUEST_TIMEOUT_SECONDS"
    )
    vehicle_freshness_seconds: int = Field(gt=0, validation_alias="API_VEHICLE_FRESHNESS_SECONDS")
    default_page_size: int = Field(gt=0, validation_alias="API_DEFAULT_PAGE_SIZE")
    max_page_size: int = Field(gt=0, validation_alias="API_MAX_PAGE_SIZE")
    history_max_range_hours: int = Field(
        gt=0, le=72, validation_alias="API_HISTORY_MAX_RANGE_HOURS"
    )
    history_default_range_minutes: int = Field(
        gt=0, validation_alias="API_HISTORY_DEFAULT_RANGE_MINUTES"
    )
    cors_origins: str = Field(validation_alias="API_CORS_ORIGINS")

    @field_validator("cassandra_username", "cassandra_password", mode="before")
    @classmethod
    def empty_credential_is_unset(cls, value: object) -> object:
        return None if isinstance(value, str) and not value.strip() else value

    @model_validator(mode="after")
    def validate_consistency(self) -> "ApiSettings":
        if (self.cassandra_username is None) != (self.cassandra_password is None):
            raise ValueError("CASSANDRA_USERNAME and CASSANDRA_PASSWORD must be set together")
        if self.default_page_size > self.max_page_size:
            raise ValueError("API_DEFAULT_PAGE_SIZE must not exceed API_MAX_PAGE_SIZE")
        if self.history_default_range_minutes > self.history_max_range_hours * 60:
            raise ValueError(
                "API_HISTORY_DEFAULT_RANGE_MINUTES must fit into API_HISTORY_MAX_RANGE_HOURS"
            )
        return self

    @property
    def cors_origin_list(self) -> list[str]:
        return [origin.strip() for origin in self.cors_origins.split(",") if origin.strip()]
