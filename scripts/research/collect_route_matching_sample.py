"""Collect a raw research sample for T09 route/shape matching thresholds (ADR 0004).

Downloads the current static GTFS feed once and polls the realtime `/vehicle` endpoint for a
fixed duration. Raw bytes are stored unchanged under `data/research/` (git-ignored), so the
analysis can be repeated without calling the API again.

Usage:
    uv run python scripts/research/collect_route_matching_sample.py --duration-minutes 60
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import time
from datetime import UTC, datetime
from pathlib import Path

import httpx
from pydantic import Field, HttpUrl
from spb_transport_collector.config import GtfsHttpSettings
from spb_transport_collector.http_client import create_gtfs_http_client

logger = logging.getLogger("collect_route_matching_sample")


class SampleSettings(GtfsHttpSettings):
    static_url: HttpUrl = Field(validation_alias="GTFS_STATIC_URL")
    static_max_download_bytes: int = Field(gt=0, validation_alias="GTFS_STATIC_MAX_DOWNLOAD_BYTES")
    vehicle_poll_interval_seconds: float = Field(
        gt=0, validation_alias="GTFS_VEHICLE_POLL_INTERVAL_SECONDS"
    )


def _timestamp(value: datetime) -> str:
    return value.strftime("%Y%m%dT%H%M%S%fZ")


async def _download_static(settings: SampleSettings, output: Path) -> dict[str, object]:
    static_url = str(settings.static_url)
    base_url, _, filename = static_url.rpartition("/")
    static_settings = settings.model_copy(update={"base_url": HttpUrl(base_url)})
    async with create_gtfs_http_client(static_settings) as client:
        response = await client.get(filename)
    payload = response.content
    if len(payload) > settings.static_max_download_bytes:
        raise ValueError("static GTFS download exceeds GTFS_STATIC_MAX_DOWNLOAD_BYTES")
    (output / "feed.zip").write_bytes(payload)
    return {"url": static_url, "bytes": len(payload), "sha256": hashlib.sha256(payload).hexdigest()}


async def _poll_vehicles(
    settings: SampleSettings, output: Path, duration_seconds: float
) -> dict[str, int]:
    directory = output / "vehicle"
    directory.mkdir()
    stats = {"polls": 0, "saved": 0, "failed": 0}
    deadline = time.monotonic() + duration_seconds
    async with create_gtfs_http_client(settings) as client:
        while time.monotonic() < deadline:
            started = time.monotonic()
            stats["polls"] += 1
            try:
                response = await client.get("vehicle")
            except httpx.HTTPError as error:
                stats["failed"] += 1
                logger.warning("vehicle poll failed: %s", type(error).__name__)
            else:
                collected_at = datetime.now(UTC)
                (directory / f"{_timestamp(collected_at)}.pb").write_bytes(response.content)
                stats["saved"] += 1
                logger.info("poll %d saved (%d bytes)", stats["polls"], len(response.content))
            elapsed = time.monotonic() - started
            await asyncio.sleep(max(0.0, settings.vehicle_poll_interval_seconds - elapsed))
    return stats


async def collect(env_file: Path, output_root: Path, duration_minutes: float) -> Path:
    settings = SampleSettings(_env_file=env_file)  # type: ignore[call-arg]
    started_at = datetime.now(UTC)
    output = output_root / _timestamp(started_at)
    output.mkdir(parents=True)
    static = await _download_static(settings, output)
    logger.info("static GTFS saved: %s bytes", static["bytes"])
    polls = await _poll_vehicles(settings, output, duration_minutes * 60)
    manifest = {
        "purpose": "T09 route/shape matching threshold research",
        "started_at": started_at.isoformat(),
        "finished_at": datetime.now(UTC).isoformat(),
        "static": static,
        "vehicle_endpoint": f"{settings.base_url}/vehicle",
        "vehicle_poll_interval_seconds": settings.vehicle_poll_interval_seconds,
        "tls_verify": settings.tls_verify,
        "vehicle_polls": polls,
    }
    (output / "manifest.json").write_text(json.dumps(manifest, indent=2), encoding="utf-8")
    return output


def main() -> None:
    parser = argparse.ArgumentParser(description="Collect a T09 route matching research sample")
    parser.add_argument("--duration-minutes", type=float, required=True)
    parser.add_argument("--env-file", type=Path, default=Path(".env"))
    parser.add_argument("--output", type=Path, default=Path("data/research/route-matching"))
    arguments = parser.parse_args()
    if arguments.duration_minutes <= 0:
        parser.error("--duration-minutes must be positive")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    output = asyncio.run(collect(arguments.env_file, arguments.output, arguments.duration_minutes))
    print(f"sample saved to {output}")


if __name__ == "__main__":
    main()
