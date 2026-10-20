from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any

from google.transit import gtfs_realtime_pb2  # type: ignore[import-untyped]
from pydantic import ValidationError
from spb_transport_contracts import VehiclePositionV1


@dataclass(frozen=True)
class VehicleFeedResult:
    events: tuple[VehiclePositionV1, ...]
    invalid_entities: int


def parse_vehicle_feed(payload: bytes, collected_at: datetime) -> VehicleFeedResult:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)

    events: list[VehiclePositionV1] = []
    invalid_entities = 0
    for entity in feed.entity:
        try:
            event = _parse_entity(entity, collected_at)
        except (OSError, OverflowError, ValidationError, ValueError):
            invalid_entities += 1
            continue
        if event is None:
            invalid_entities += 1
        else:
            events.append(event)

    return VehicleFeedResult(tuple(events), invalid_entities)


def _parse_entity(
    entity: Any,
    collected_at: datetime,
) -> VehiclePositionV1 | None:
    if not entity.HasField("vehicle"):
        return None

    observation = entity.vehicle
    if not (
        observation.HasField("trip")
        and observation.trip.HasField("route_id")
        and observation.HasField("vehicle")
        and observation.vehicle.HasField("id")
        and observation.HasField("position")
        and observation.position.HasField("latitude")
        and observation.position.HasField("longitude")
        and observation.HasField("timestamp")
    ):
        return None

    source_timestamp = datetime.fromtimestamp(observation.timestamp, UTC)
    vehicle_id = observation.vehicle.id
    route_id = observation.trip.route_id
    event_id = sha256(f"{vehicle_id}\0{observation.timestamp}".encode()).hexdigest()

    return VehiclePositionV1(
        schema_version="1",
        event_id=event_id,
        collected_at=collected_at,
        source_timestamp=source_timestamp,
        vehicle_id=vehicle_id,
        route_id=route_id,
        latitude=observation.position.latitude,
        longitude=observation.position.longitude,
        bearing=(
            observation.position.bearing if observation.position.HasField("bearing") else None
        ),
        speed=(observation.position.speed if observation.position.HasField("speed") else None),
        label=(observation.vehicle.label if observation.vehicle.HasField("label") else None),
        license_plate=(
            observation.vehicle.license_plate
            if observation.vehicle.HasField("license_plate")
            else None
        ),
    )
