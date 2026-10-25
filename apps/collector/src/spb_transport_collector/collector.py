import asyncio
import logging
import time
from contextlib import suppress
from datetime import UTC, datetime, timedelta

import httpx
from google.protobuf.message import DecodeError
from spb_transport_contracts import RawEndpoint

from spb_transport_collector.eta_feed import parse_eta_feed
from spb_transport_collector.http_client import GtfsHttpClient
from spb_transport_collector.kafka import KafkaPublisher
from spb_transport_collector.raw_response import build_raw_response
from spb_transport_collector.vehicle_feed import parse_vehicle_feed

logger = logging.getLogger(__name__)


class VehicleCollector:
    def __init__(
        self,
        http_client: GtfsHttpClient,
        publisher: KafkaPublisher,
        vehicle_poll_interval_seconds: float,
        vehicle_trips_poll_interval_seconds: float,
        vehicle_trips_batch_size: int,
    ) -> None:
        self._http_client = http_client
        self._publisher = publisher
        self._vehicle_poll_interval_seconds = vehicle_poll_interval_seconds
        self._vehicle_trips_poll_interval_seconds = vehicle_trips_poll_interval_seconds
        self._vehicle_trips_batch_size = vehicle_trips_batch_size
        self._active_vehicle_ids: tuple[str, ...] = ()

    async def _timed_get(
        self, endpoint: str, params: httpx.QueryParams | None = None
    ) -> tuple[httpx.Response, datetime, datetime]:
        """GET with request and collection times that never run backwards.

        The wall clock of a container can step back (for example when Docker Desktop resyncs
        the VM clock after sleep). The duration is measured on the monotonic clock, so
        `collected_at >= requested_at` holds even then.
        """
        requested_at = datetime.now(UTC)
        started = time.monotonic()
        response = await self._http_client.get(endpoint, params=params)
        collected_at = requested_at + timedelta(seconds=time.monotonic() - started)
        return response, requested_at, collected_at

    async def collect_positions_once(self) -> int:
        response, requested_at, collected_at = await self._timed_get("vehicle")
        try:
            result = parse_vehicle_feed(response.content, collected_at)
        except DecodeError as error:
            await self._publisher.publish_raw(
                build_raw_response(
                    response,
                    RawEndpoint.VEHICLE,
                    requested_at,
                    collected_at,
                    decode_error=type(error).__name__,
                )
            )
            raise
        await self._publisher.publish_raw(
            build_raw_response(response, RawEndpoint.VEHICLE, requested_at, collected_at)
        )
        await self._publisher.publish_positions(result.events)
        self._active_vehicle_ids = tuple(sorted({event.vehicle_id for event in result.events}))
        logger.info(
            "GTFS vehicle feed collected",
            extra={
                "event": "vehicle_feed_collected",
                "published_events": len(result.events),
                "invalid_entities": result.invalid_entities,
            },
        )
        return len(result.events)

    async def collect_eta_once(self) -> int:
        published_events = 0
        invalid_entities = 0
        failed_batches = 0

        for start in range(0, len(self._active_vehicle_ids), self._vehicle_trips_batch_size):
            vehicle_ids = self._active_vehicle_ids[start : start + self._vehicle_trips_batch_size]
            params = httpx.QueryParams([("vehicleIDs", vehicle_id) for vehicle_id in vehicle_ids])
            try:
                response, requested_at, collected_at = await self._timed_get("vehicletrips", params)
                try:
                    result = parse_eta_feed(response.content, collected_at)
                except DecodeError as error:
                    await self._publisher.publish_raw(
                        build_raw_response(
                            response,
                            RawEndpoint.VEHICLE_TRIPS,
                            requested_at,
                            collected_at,
                            decode_error=type(error).__name__,
                        )
                    )
                    raise
                await self._publisher.publish_raw(
                    build_raw_response(
                        response, RawEndpoint.VEHICLE_TRIPS, requested_at, collected_at
                    )
                )
            except (DecodeError, httpx.HTTPError) as error:
                failed_batches += 1
                logger.error(
                    "GTFS vehicle trips batch failed",
                    extra={
                        "event": "vehicle_trips_batch_failed",
                        "error": type(error).__name__,
                        "requested_vehicles": len(vehicle_ids),
                    },
                    exc_info=True,
                )
                continue

            await self._publisher.publish_eta(result.events)
            published_events += len(result.events)
            invalid_entities += result.invalid_entities

        logger.info(
            "GTFS vehicle trips collected",
            extra={
                "event": "vehicle_trips_collected",
                "requested_vehicles": len(self._active_vehicle_ids),
                "published_eta_events": published_events,
                "invalid_entities": invalid_entities,
                "failed_batches": failed_batches,
            },
        )
        return published_events

    async def run(self, stop_event: asyncio.Event) -> None:
        async with asyncio.TaskGroup() as tasks:
            tasks.create_task(self._run_positions(stop_event))
            tasks.create_task(self._run_eta(stop_event))

    async def _run_positions(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await self.collect_positions_once()
            except Exception as error:  # one failed cycle must not stop the collector
                logger.error(
                    "GTFS vehicle collection cycle failed",
                    extra={
                        "event": "vehicle_collection_failed",
                        "error": type(error).__name__,
                    },
                    exc_info=True,
                )

            with suppress(TimeoutError):
                await asyncio.wait_for(
                    stop_event.wait(), timeout=self._vehicle_poll_interval_seconds
                )

    async def _run_eta(self, stop_event: asyncio.Event) -> None:
        while not stop_event.is_set():
            try:
                await self.collect_eta_once()
            except Exception as error:  # one failed cycle must not stop the collector
                logger.error(
                    "GTFS ETA collection cycle failed",
                    extra={
                        "event": "vehicle_eta_collection_failed",
                        "error": type(error).__name__,
                    },
                    exc_info=True,
                )

            with suppress(TimeoutError):
                await asyncio.wait_for(
                    stop_event.wait(), timeout=self._vehicle_trips_poll_interval_seconds
                )
