import asyncio
from typing import Any, cast
from unittest.mock import AsyncMock

import httpx
from google.transit import gtfs_realtime_pb2  # type: ignore[import-untyped]
from spb_transport_collector.collector import VehicleCollector
from spb_transport_collector.http_client import GtfsHttpClient, RetryPolicy
from spb_transport_collector.kafka import KafkaPositionPublisher


def valid_feed() -> bytes:
    feed = gtfs_realtime_pb2.FeedMessage()
    feed.header.gtfs_realtime_version = "2.0"
    entity = feed.entity.add()
    entity.id = "source-entity-id"
    vehicle = entity.vehicle
    vehicle.vehicle.id = "18342"
    vehicle.trip.route_id = "42"
    vehicle.position.latitude = 59.9343
    vehicle.position.longitude = 30.3351
    vehicle.timestamp = 1_757_754_000
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

    async def stop_after_publish(*args: object, **kwargs: object) -> None:
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
    publisher = KafkaPositionPublisher(producer, "vehicle-position.v1")
    collector = VehicleCollector(client, publisher, poll_interval_seconds=0.001)

    async def scenario() -> None:
        async with client:
            await asyncio.wait_for(collector.run(stop_event), timeout=1)

    asyncio.run(scenario())

    assert requests == 2
    producer.send_and_wait.assert_awaited_once()
