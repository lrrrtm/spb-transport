import asyncio
import logging
from contextlib import suppress
from datetime import UTC, datetime

import httpx
from aiokafka.errors import KafkaError  # type: ignore[import-untyped]
from google.protobuf.message import DecodeError

from spb_transport_collector.http_client import GtfsHttpClient
from spb_transport_collector.kafka import KafkaPositionPublisher
from spb_transport_collector.vehicle_feed import parse_vehicle_feed

logger = logging.getLogger(__name__)


class VehicleCollector:
    def __init__(
        self,
        http_client: GtfsHttpClient,
        publisher: KafkaPositionPublisher,
        poll_interval_seconds: float,
    ) -> None:
        self._http_client = http_client
        self._publisher = publisher
        self._poll_interval_seconds = poll_interval_seconds

    async def collect_once(self) -> int:
        response = await self._http_client.get("vehicle")
        result = parse_vehicle_feed(response.content, datetime.now(UTC))
        await self._publisher.publish(result.events)
        logger.info(
            "GTFS vehicle feed collected",
            extra={
                "event": "vehicle_feed_collected",
                "published_events": len(result.events),
                "invalid_entities": result.invalid_entities,
            },
        )
        return len(result.events)

    async def run(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await self.collect_once()
            except (DecodeError, httpx.HTTPError, KafkaError) as error:
                logger.error(
                    "GTFS vehicle collection cycle failed",
                    extra={
                        "event": "vehicle_collection_failed",
                        "error": type(error).__name__,
                    },
                    exc_info=True,
                )

            with suppress(TimeoutError):
                await asyncio.wait_for(stop_event.wait(), timeout=self._poll_interval_seconds)
