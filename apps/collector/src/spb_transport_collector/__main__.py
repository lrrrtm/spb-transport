import asyncio
import signal

from spb_transport_collector.collector import VehicleCollector
from spb_transport_collector.config import VehicleCollectorSettings
from spb_transport_collector.http_client import create_gtfs_http_client
from spb_transport_collector.kafka import create_publisher
from spb_transport_collector.logging_config import configure_logging


async def run() -> None:
    configure_logging()
    settings = VehicleCollectorSettings()  # type: ignore[call-arg]
    stop_event = asyncio.Event()
    loop = asyncio.get_running_loop()
    for shutdown_signal in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(shutdown_signal, stop_event.set)

    async with (
        create_gtfs_http_client(settings) as http_client,
        create_publisher(settings) as publisher,
    ):
        collector = VehicleCollector(
            http_client,
            publisher,
            settings.vehicle_poll_interval_seconds,
            settings.vehicle_trips_poll_interval_seconds,
            settings.vehicle_trips_batch_size,
        )
        await collector.run(stop_event)


if __name__ == "__main__":
    asyncio.run(run())
