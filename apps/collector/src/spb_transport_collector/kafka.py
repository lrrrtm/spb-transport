import asyncio
from types import TracebackType
from typing import Any

from aiokafka import AIOKafkaProducer  # type: ignore[import-untyped]
from spb_transport_contracts import RawResponseV1, VehicleEtaSnapshotV1, VehiclePositionV1

from spb_transport_collector.config import VehicleCollectorSettings


class KafkaPublisher:
    def __init__(self, producer: Any, position_topic: str, eta_topic: str, raw_topic: str) -> None:
        self._producer = producer
        self._position_topic = position_topic
        self._eta_topic = eta_topic
        self._raw_topic = raw_topic

    async def start(self) -> None:
        await self._producer.start()

    async def publish_positions(self, events: tuple[VehiclePositionV1, ...]) -> None:
        await self._publish(self._position_topic, events)

    async def publish_eta(self, events: tuple[VehicleEtaSnapshotV1, ...]) -> None:
        await self._publish(self._eta_topic, events)

    async def publish_raw(self, event: RawResponseV1) -> None:
        await self._producer.send_and_wait(
            self._raw_topic,
            key=event.endpoint.value.encode(),
            value=event.model_dump_json().encode(),
        )

    async def _publish(
        self,
        topic: str,
        events: tuple[VehiclePositionV1, ...] | tuple[VehicleEtaSnapshotV1, ...],
    ) -> None:
        await asyncio.gather(
            *(
                self._producer.send_and_wait(
                    topic,
                    key=event.vehicle_id.encode(),
                    value=event.model_dump_json().encode(),
                )
                for event in events
            )
        )

    async def stop(self) -> None:
        await self._producer.stop()

    async def __aenter__(self) -> "KafkaPublisher":
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.stop()


def create_publisher(settings: VehicleCollectorSettings) -> KafkaPublisher:
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id=settings.kafka_client_id,
        acks="all",
        enable_idempotence=True,
        linger_ms=settings.kafka_linger_ms,
        max_batch_size=settings.kafka_max_batch_size_bytes,
        request_timeout_ms=settings.kafka_request_timeout_ms,
        max_request_size=settings.kafka_max_request_size_bytes,
    )
    return KafkaPublisher(
        producer,
        settings.kafka_vehicle_position_topic,
        settings.kafka_vehicle_eta_topic,
        settings.kafka_raw_response_topic,
    )
