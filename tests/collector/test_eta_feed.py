from datetime import UTC, datetime, timedelta
from typing import cast

import pytest
from google.protobuf.message import DecodeError
from google.transit import gtfs_realtime_pb2  # type: ignore[import-untyped]
from spb_transport_collector.eta_feed import parse_eta_feed

COLLECTED_AT = datetime(2026, 9, 13, 9, 0, 3, tzinfo=UTC)


def eta_feed(*, include_forecasts: bool = True) -> bytes:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    entity = feed.entity.add()
    entity.id = "eta-source-entity"
    trip_update = entity.trip_update
    trip_update.vehicle.id = "18342"
    trip_update.trip.route_id = "42"
    if include_forecasts:
        first = trip_update.stop_time_update.add()
        first.stop_id = "1001"
        first.arrival.time = 1_757_754_240
        second = trip_update.stop_time_update.add()
        second.stop_id = "1002"
        second.arrival.time = 1_757_754_540
    return cast(bytes, feed.SerializeToString())


def test_parses_ordered_arrival_forecasts() -> None:
    result = parse_eta_feed(eta_feed(), COLLECTED_AT)

    assert result.invalid_entities == 0
    assert len(result.events) == 1
    snapshot = result.events[0]
    assert snapshot.vehicle_id == "18342"
    assert snapshot.route_id == "42"
    assert [forecast.forecast_rank for forecast in snapshot.forecasts] == [0, 1]
    assert [forecast.stop_id for forecast in snapshot.forecasts] == ["1001", "1002"]


def test_empty_stop_time_update_is_valid_snapshot() -> None:
    result = parse_eta_feed(eta_feed(include_forecasts=False), COLLECTED_AT)

    assert result.invalid_entities == 0
    assert result.events[0].forecasts == ()


def test_eta_event_id_does_not_depend_on_poll_time() -> None:
    first = parse_eta_feed(eta_feed(), COLLECTED_AT).events[0]
    second = parse_eta_feed(eta_feed(), COLLECTED_AT + timedelta(seconds=10)).events[0]

    assert first.event_id == second.event_id
    assert first.collected_at != second.collected_at


def test_entity_with_partial_forecast_is_rejected() -> None:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    entity = feed.entity.add()
    entity.id = "partial"
    entity.trip_update.vehicle.id = "18342"
    entity.trip_update.trip.route_id = "42"
    entity.trip_update.stop_time_update.add().stop_id = "1001"

    result = parse_eta_feed(cast(bytes, feed.SerializeToString()), COLLECTED_AT)

    assert result.events == ()
    assert result.invalid_entities == 1


def test_rejects_broken_eta_protobuf() -> None:
    with pytest.raises(DecodeError):
        parse_eta_feed(b"\x80", COLLECTED_AT)
