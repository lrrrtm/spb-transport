import asyncio
from types import TracebackType
from typing import Any

from aiokafka import AIOKafkaProducer  # type: ignore[import-untyped]
from spb_transport_contracts import VehiclePositionV1

from spb_transport_collector.config import VehicleCollectorSettings


class KafkaPositionPublisher:
    def __init__(self, producer: Any, topic: str) -> None:
        self._producer = producer
        self._topic = topic

    async def start(self) -> None:
        await self._producer.start()

    async def publish(self, events: tuple[VehiclePositionV1, ...]) -> None:
        await asyncio.gather(
            *(
                self._producer.send_and_wait(
                    self._topic,
                    key=event.vehicle_id.encode(),
                    value=event.model_dump_json().encode(),
                )
                for event in events
            )
        )

    async def stop(self) -> None:
        await self._producer.stop()

    async def __aenter__(self) -> "KafkaPositionPublisher":
        await self.start()
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.stop()


def create_position_publisher(settings: VehicleCollectorSettings) -> KafkaPositionPublisher:
    producer = AIOKafkaProducer(
        bootstrap_servers=settings.kafka_bootstrap_servers,
        client_id=settings.kafka_client_id,
        acks="all",
        enable_idempotence=True,
        linger_ms=settings.kafka_linger_ms,
        max_batch_size=settings.kafka_max_batch_size_bytes,
        request_timeout_ms=settings.kafka_request_timeout_ms,
    )
    return KafkaPositionPublisher(producer, settings.kafka_vehicle_position_topic)
