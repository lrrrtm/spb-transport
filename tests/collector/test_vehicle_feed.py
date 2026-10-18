from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2  # type: ignore[import-untyped]
from spb_transport_collector.vehicle_feed import parse_vehicle_feed

COLLECTED_AT = datetime(2026, 9, 13, 9, 0, 3, tzinfo=UTC)


def vehicle_feed(*, speed: float | None = 0) -> bytes:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    entity = feed.entity.add()
    entity.id = "source-entity-id"
    entity.vehicle.vehicle.id = "18342"
    entity.vehicle.vehicle.label = "Bus 18342"
    entity.vehicle.vehicle.license_plate = "TEST"
    entity.vehicle.trip.route_id = "42"
    entity.vehicle.position.latitude = 59.9343
    entity.vehicle.position.longitude = 30.3351
    entity.vehicle.position.bearing = 180
    if speed is not None:
        entity.vehicle.position.speed = speed
    entity.vehicle.timestamp = 1_757_754_000
    return cast(bytes, feed.SerializeToString())


def test_parses_confirmed_fields_and_preserves_zero_speed() -> None:
    result = parse_vehicle_feed(vehicle_feed(), COLLECTED_AT)

    assert result.invalid_entities == 0
    assert len(result.events) == 1
    event = result.events[0]
    assert event.vehicle_id == "18342"
    assert event.route_id == "42"
    assert event.speed == 0
    assert event.label == "Bus 18342"
    assert event.license_plate == "TEST"


def test_event_id_does_not_depend_on_poll_time() -> None:
    first = parse_vehicle_feed(vehicle_feed(), COLLECTED_AT).events[0]
    second = parse_vehicle_feed(vehicle_feed(), COLLECTED_AT + timedelta(seconds=10)).events[0]

    assert first.event_id == second.event_id
    assert first.collected_at != second.collected_at


def test_missing_optional_speed_stays_none() -> None:
    event = parse_vehicle_feed(vehicle_feed(speed=None), COLLECTED_AT).events[0]

    assert event.speed is None


def test_skips_entity_without_required_vehicle_fields() -> None:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    feed.entity.add().id = "not-a-vehicle"

    result = parse_vehicle_feed(feed.SerializeToString(), COLLECTED_AT)

    assert result.events == ()
    assert result.invalid_entities == 1


def test_rejects_broken_protobuf() -> None:
    with pytest.raises(DecodeError):
        parse_vehicle_feed(b"\x80", COLLECTED_AT)
