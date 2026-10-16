from enum import StrEnum
from typing import Annotated, Literal

from pydantic import AwareDatetime, BaseModel, ConfigDict, Field, model_validator

NonEmptyString = Annotated[str, Field(min_length=1)]
EventId = Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]


class ContractModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class VehiclePositionV1(ContractModel):
    schema_version: Literal["1"]
    event_id: EventId
    collected_at: AwareDatetime
    source_timestamp: AwareDatetime
    vehicle_id: NonEmptyString
    route_id: NonEmptyString
    latitude: Annotated[float, Field(ge=-90, le=90)]
    longitude: Annotated[float, Field(ge=-180, le=180)]
    bearing: Annotated[float, Field(ge=0, lt=360)] | None = None
    speed: Annotated[float, Field(ge=0)] | None = None
    label: str | None = None
    license_plate: str | None = None


class StopArrivalForecastV1(ContractModel):
    forecast_rank: Annotated[int, Field(ge=0)]
    stop_id: NonEmptyString
    arrival_time: AwareDatetime


class VehicleEtaSnapshotV1(ContractModel):
    schema_version: Literal["1"]
    event_id: EventId
    collected_at: AwareDatetime
    vehicle_id: NonEmptyString
    route_id: NonEmptyString
    forecasts: tuple[StopArrivalForecastV1, ...]

    @model_validator(mode="after")
    def ranks_follow_payload_order(self) -> "VehicleEtaSnapshotV1":
        ranks = [forecast.forecast_rank for forecast in self.forecasts]
        if ranks != list(range(len(ranks))):
            raise ValueError("forecast_rank must be contiguous and start at zero")
        return self


class RawEndpoint(StrEnum):
    VEHICLE = "vehicle"
    VEHICLE_TRIPS = "vehicletrips"


class RawResponseV1(ContractModel):
    schema_version: Literal["1"]
    request_id: NonEmptyString
    endpoint: RawEndpoint
    requested_at: AwareDatetime
    collected_at: AwareDatetime
    http_status: Annotated[int, Field(ge=100, le=599)] | None
    content_type: str | None = None
    payload_base64: str
    sha256: EventId
    decode_error: str | None = None

    @model_validator(mode="after")
    def collection_follows_request(self) -> "RawResponseV1":
        if self.collected_at < self.requested_at:
            raise ValueError("collected_at must not be earlier than requested_at")
        return self
