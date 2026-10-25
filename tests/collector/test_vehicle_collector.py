import asyncio
import json
from datetime import UTC, datetime
from typing import Any, cast
from unittest.mock import AsyncMock

import httpx
from google.transit import gtfs_realtime_pb2  # type: ignore[import-untyped]
from spb_transport_collector import collector as collector_module
from spb_transport_collector.collector import VehicleCollector
from spb_transport_collector.http_client import GtfsHttpClient, RetryPolicy
from spb_transport_collector.kafka import KafkaPublisher


def valid_feed(vehicle_ids: tuple[str, ...] = ("18342",)) -> bytes:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    for vehicle_id in vehicle_ids:
        entity = feed.entity.add()
        entity.id = f"position-{vehicle_id}"
        vehicle = entity.vehicle
        vehicle.vehicle.id = vehicle_id
        vehicle.trip.route_id = "42"
        vehicle.position.latitude = 59.9343
        vehicle.position.longitude = 30.3351
        vehicle.timestamp = 1_757_754_000
    return cast(bytes, feed.SerializeToString())


def eta_feed(vehicle_ids: list[str]) -> bytes:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    for vehicle_id in vehicle_ids:
        entity = feed.entity.add()
        entity.id = f"eta-{vehicle_id}"
        trip_update = entity.trip_update
        trip_update.vehicle.id = vehicle_id
        trip_update.trip.route_id = "42"
    return cast(bytes, feed.SerializeToString())


def test_broken_feed_does_not_stop_following_poll_cycle() -> None:
    requests = 0
    stop_event = asyncio.Event()

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal requests
        requests += 1
        return httpx.Response(
            200,
            content=b"\x80" if requests == 1 else valid_feed(),
            request=request,
        )

    producer: Any = AsyncMock()

    publications = 0

    async def stop_after_publish(*args: object, **kwargs: object) -> None:
        nonlocal publications
        publications += 1
        if publications == 3:
            stop_event.set()

    producer.send_and_wait.side_effect = stop_after_publish
    http_client = httpx.AsyncClient(
        base_url="https://example.test/realtime/",
        transport=httpx.MockTransport(handler),
    )
    client = GtfsHttpClient(
        http_client,
        RetryPolicy(1, 0, 0, 0),
    )
    publisher = KafkaPublisher(producer, "vehicle-position.v1", "vehicle-eta.v1", "raw.v1")
    collector = VehicleCollector(
        client,
        publisher,
        vehicle_poll_interval_seconds=0.001,
        vehicle_trips_poll_interval_seconds=1,
        vehicle_trips_batch_size=10,
    )

    async def scenario() -> None:
        async with client:
            await asyncio.wait_for(collector.run(stop_event), timeout=1)

    asyncio.run(scenario())

    assert requests == 2
    assert producer.send_and_wait.await_count == 3


def test_vehicle_trips_requests_are_split_into_configured_batches() -> None:
    requested_batches: list[list[str]] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/vehicle"):
            content = valid_feed(("3", "1", "2"))
        else:
            vehicle_ids = request.url.params.get_list("vehicleIDs")
            requested_batches.append(vehicle_ids)
            content = eta_feed(vehicle_ids)
        return httpx.Response(200, content=content, request=request)

    producer: Any = AsyncMock()
    http_client = httpx.AsyncClient(
        base_url="https://example.test/realtime/",
        transport=httpx.MockTransport(handler),
    )
    client = GtfsHttpClient(http_client, RetryPolicy(1, 0, 0, 0))
    publisher = KafkaPublisher(producer, "vehicle-position.v1", "vehicle-eta.v1", "raw.v1")
    collector = VehicleCollector(client, publisher, 10, 30, 2)

    async def scenario() -> int:
        async with client:
            await collector.collect_positions_once()
            return await collector.collect_eta_once()

    published = asyncio.run(scenario())

    assert requested_batches == [["1", "2"], ["3"]]
    assert published == 3


def test_failed_vehicle_trips_batch_does_not_skip_later_batches() -> None:
    eta_requests = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal eta_requests
        if request.url.path.endswith("/vehicle"):
            return httpx.Response(200, content=valid_feed(("1", "2")), request=request)
        eta_requests += 1
        vehicle_ids = request.url.params.get_list("vehicleIDs")
        return httpx.Response(
            200,
            content=b"\x80" if eta_requests == 1 else eta_feed(vehicle_ids),
            request=request,
        )

    producer: Any = AsyncMock()
    http_client = httpx.AsyncClient(
        base_url="https://example.test/realtime/",
        transport=httpx.MockTransport(handler),
    )
    client = GtfsHttpClient(http_client, RetryPolicy(1, 0, 0, 0))
    collector = VehicleCollector(
        client,
        KafkaPublisher(producer, "vehicle-position.v1", "vehicle-eta.v1", "raw.v1"),
        10,
        30,
        1,
    )

    async def scenario() -> int:
        async with client:
            await collector.collect_positions_once()
            return await collector.collect_eta_once()

    published = asyncio.run(scenario())

    assert eta_requests == 2
    assert published == 1

    raw_values = [
        call.kwargs["value"]
        for call in producer.send_and_wait.await_args_list
        if call.args[0] == "raw.v1"
    ]
    assert len(raw_values) == 3
    assert b'"decode_error":"DecodeError"' in raw_values[1]


def test_collection_time_never_precedes_request_time_when_wall_clock_steps_back(
    monkeypatch: Any,
) -> None:
    # The old code read the wall clock twice; here the second reading would be 5 s earlier.
    moments = iter(
        [
            datetime(2026, 10, 3, 15, 19, 45, tzinfo=UTC),
            datetime(2026, 10, 3, 15, 19, 40, tzinfo=UTC),
        ]
    )

    class SteppingBackClock:
        @staticmethod
        def now(tz: object = None) -> datetime:
            return next(moments)

    monkeypatch.setattr(collector_module, "datetime", SteppingBackClock)
    producer: Any = AsyncMock()
    http_client = httpx.AsyncClient(
        base_url="https://example.test/realtime/",
        transport=httpx.MockTransport(
            lambda request: httpx.Response(200, content=valid_feed(), request=request)
        ),
    )
    collector = VehicleCollector(
        GtfsHttpClient(http_client, RetryPolicy(1, 0, 0, 0)),
        KafkaPublisher(producer, "vehicle-position.v1", "vehicle-eta.v1", "raw.v1"),
        vehicle_poll_interval_seconds=1,
        vehicle_trips_poll_interval_seconds=1,
        vehicle_trips_batch_size=10,
    )

    async def scenario() -> int:
        async with http_client:
            return await collector.collect_positions_once()

    assert asyncio.run(scenario()) == 1
    raw = producer.send_and_wait.await_args_list[0]
    payload = raw.kwargs.get("value") or raw.args[1]
    envelope = json.loads(payload)
    collected_at = datetime.fromisoformat(envelope["collected_at"])
    assert collected_at >= datetime.fromisoformat(envelope["requested_at"])


def test_unexpected_error_in_eta_cycle_does_not_stop_the_collector() -> None:
    stop_event = asyncio.Event()
    calls = 0

    producer: Any = AsyncMock()
    collector = VehicleCollector(
        GtfsHttpClient(
            httpx.AsyncClient(base_url="https://example.test/"), RetryPolicy(1, 0, 0, 0)
        ),
        KafkaPublisher(producer, "vehicle-position.v1", "vehicle-eta.v1", "raw.v1"),
        vehicle_poll_interval_seconds=1,
        vehicle_trips_poll_interval_seconds=0.001,
        vehicle_trips_batch_size=10,
    )

    async def failing_once() -> int:
        nonlocal calls
        calls += 1
        if calls == 1:
            raise ValueError("unexpected")
        stop_event.set()
        return 0

    collector.collect_eta_once = failing_once  # type: ignore[method-assign]

    asyncio.run(collector._run_eta(stop_event))

    assert calls == 2
