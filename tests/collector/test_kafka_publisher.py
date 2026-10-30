import asyncio
from datetime import UTC, datetime
from typing import Any
from unittest.mock import AsyncMock

import pytest
from spb_transport_collector.kafka import KafkaPublisher
from spb_transport_contracts import (
    RawEndpoint,
    RawResponseV1,
    VehicleEtaSnapshotV1,
    VehiclePositionV1,
)


def test_publisher_uses_vehicle_key_and_closes_producer() -> None:
    producer: Any = AsyncMock()
    event = VehiclePositionV1(
        schema_version="1",
        event_id="1" * 64,
        collected_at=datetime(2026, 9, 13, 9, 0, 3, tzinfo=UTC),
        source_timestamp=datetime(2026, 9, 13, 9, 0, tzinfo=UTC),
        vehicle_id="18342",
        route_id="42",
        latitude=59.9343,
        longitude=30.3351,
        speed=0,
    )

    async def scenario() -> None:
        async with KafkaPublisher(
            producer, "vehicle-position.v1", "vehicle-eta.v1", "raw-response.v1"
        ) as publisher:
            await publisher.publish_positions((event,))

    asyncio.run(scenario())

    producer.start.assert_awaited_once()
    producer.stop.assert_awaited_once()
    producer.send_and_wait.assert_awaited_once_with(
        "vehicle-position.v1",
        key=b"18342",
        value=event.model_dump_json().encode(),
    )


def test_eta_publisher_accepts_empty_forecast_snapshot() -> None:
    producer: Any = AsyncMock()
    event = VehicleEtaSnapshotV1(
        schema_version="1",
        event_id="2" * 64,
        collected_at=datetime(2026, 9, 13, 9, 0, 3, tzinfo=UTC),
        vehicle_id="18342",
        route_id="42",
        forecasts=(),
    )

    async def scenario() -> None:
        publisher = KafkaPublisher(
            producer, "vehicle-position.v1", "vehicle-eta.v1", "raw-response.v1"
        )
        await publisher.publish_eta((event,))

    asyncio.run(scenario())

    producer.send_and_wait.assert_awaited_once_with(
        "vehicle-eta.v1",
        key=b"18342",
        value=event.model_dump_json().encode(),
    )


def test_raw_publisher_uses_endpoint_key() -> None:
    producer: Any = AsyncMock()
    event = RawResponseV1(
        schema_version="1",
        request_id="request-1",
        endpoint=RawEndpoint.VEHICLE,
        requested_at=datetime(2026, 9, 13, 9, 0, tzinfo=UTC),
        collected_at=datetime(2026, 9, 13, 9, 0, 1, tzinfo=UTC),
        http_status=200,
        payload_base64="AA==",
        sha256="0" * 64,
    )

    async def scenario() -> None:
        publisher = KafkaPublisher(
            producer, "vehicle-position.v1", "vehicle-eta.v1", "raw-response.v1"
        )
        await publisher.publish_raw(event)

    asyncio.run(scenario())

    producer.send_and_wait.assert_awaited_once_with(
        "raw-response.v1",
        key=b"vehicle",
        value=event.model_dump_json().encode(),
    )


def test_publish_failure_is_not_hidden() -> None:
    producer: Any = AsyncMock()
    producer.send_and_wait.side_effect = ConnectionError("Kafka is unavailable")
    event = VehiclePositionV1(
        schema_version="1",
        event_id="1" * 64,
        collected_at=datetime(2026, 9, 13, 9, 0, 3, tzinfo=UTC),
        source_timestamp=datetime(2026, 9, 13, 9, 0, tzinfo=UTC),
        vehicle_id="18342",
        route_id="42",
        latitude=59.9343,
        longitude=30.3351,
        speed=0,
    )

    async def scenario() -> None:
        publisher = KafkaPublisher(
            producer, "vehicle-position.v1", "vehicle-eta.v1", "raw-response.v1"
        )
        await publisher.publish_positions((event,))

    with pytest.raises(ConnectionError, match="Kafka is unavailable"):
        asyncio.run(scenario())
