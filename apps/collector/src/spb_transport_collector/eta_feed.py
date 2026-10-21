from dataclasses import dataclass
from datetime import UTC, datetime
from hashlib import sha256
from typing import Any

from google.transit import gtfs_realtime_pb2  # type: ignore[import-untyped]
from pydantic import ValidationError
from spb_transport_contracts import StopArrivalForecastV1, VehicleEtaSnapshotV1


@dataclass(frozen=True)
class EtaFeedResult:
    events: tuple[VehicleEtaSnapshotV1, ...]
    invalid_entities: int


def parse_eta_feed(payload: bytes, collected_at: datetime) -> EtaFeedResult:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.ParseFromString(payload)

    events: list[VehicleEtaSnapshotV1] = []
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

    return EtaFeedResult(tuple(events), invalid_entities)


def _parse_entity(entity: Any, collected_at: datetime) -> VehicleEtaSnapshotV1 | None:
    if not entity.HasField("trip_update"):
        return None

    trip_update = entity.trip_update
    if not (
        trip_update.HasField("trip")
        and trip_update.trip.HasField("route_id")
        and trip_update.HasField("vehicle")
        and trip_update.vehicle.HasField("id")
    ):
        return None

    event_id_parts = [trip_update.vehicle.id, trip_update.trip.route_id]
    forecasts: list[StopArrivalForecastV1] = []
    for rank, update in enumerate(trip_update.stop_time_update):
        if not (
            update.HasField("stop_id")
            and update.HasField("arrival")
            and update.arrival.HasField("time")
        ):
            raise ValueError("stop_time_update lacks confirmed arrival fields")
        forecasts.append(
            StopArrivalForecastV1(
                forecast_rank=rank,
                stop_id=update.stop_id,
                arrival_time=datetime.fromtimestamp(update.arrival.time, UTC),
            )
        )
        event_id_parts.extend((str(rank), update.stop_id, str(update.arrival.time)))

    return VehicleEtaSnapshotV1(
        schema_version="1",
        event_id=sha256("\0".join(event_id_parts).encode()).hexdigest(),
        collected_at=collected_at,
        vehicle_id=trip_update.vehicle.id,
        route_id=trip_update.trip.route_id,
        forecasts=tuple(forecasts),
    )
