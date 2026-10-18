import asyncio
import logging
from collections.abc import Callable

import httpx
import pytest
from pydantic import ValidationError
from spb_transport_collector.config import GtfsHttpSettings
from spb_transport_collector.http_client import (
    GtfsHttpClient,
    RetryPolicy,
    create_gtfs_http_client,
)


def settings(**overrides: object) -> GtfsHttpSettings:
    values: dict[str, object] = {
        "base_url": "https://example.test/realtime/",
        "connect_timeout_seconds": 1,
        "read_timeout_seconds": 2,
        "write_timeout_seconds": 1,
        "pool_timeout_seconds": 1,
        "retry_max_attempts": 3,
        "retry_backoff_base_seconds": 0,
        "retry_backoff_max_seconds": 0,
        "retry_jitter_seconds": 0,
    }
    values.update(overrides)
    return GtfsHttpSettings.model_validate(values)


def client_with(handler: Callable[[httpx.Request], httpx.Response]) -> GtfsHttpClient:
    http_client = httpx.AsyncClient(
        base_url="https://example.test/realtime/",
        transport=httpx.MockTransport(handler),
    )
    return GtfsHttpClient(
        http_client,
        RetryPolicy(
            max_attempts=3,
            backoff_base_seconds=0,
            backoff_max_seconds=0,
            jitter_seconds=0,
        ),
    )


def test_retries_server_errors_then_returns_response() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(503 if attempts < 3 else 200, request=request)

    async def scenario() -> None:
        async with client_with(handler) as client:
            response = await client.get("vehicle")
            assert response.status_code == 200

    asyncio.run(scenario())
    assert attempts == 3


def test_does_not_retry_client_errors() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        return httpx.Response(400, request=request)

    async def scenario() -> None:
        async with client_with(handler) as client:
            with pytest.raises(httpx.HTTPStatusError):
                await client.get("vehicle")

    asyncio.run(scenario())
    assert attempts == 1


def test_retries_transport_errors() -> None:
    attempts = 0

    def handler(request: httpx.Request) -> httpx.Response:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise httpx.ConnectError("temporary failure", request=request)
        return httpx.Response(200, request=request)

    async def scenario() -> None:
        async with client_with(handler) as client:
            response = await client.get("vehicle")
            assert response.status_code == 200

    asyncio.run(scenario())
    assert attempts == 2


def test_rejects_absolute_endpoint() -> None:
    async def scenario() -> None:
        async with client_with(lambda request: httpx.Response(200, request=request)) as client:
            with pytest.raises(ValueError, match="relative"):
                await client.get("https://unexpected.test/vehicle")

    asyncio.run(scenario())


def test_tls_verification_can_only_be_disabled_explicitly(
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING)
    client = create_gtfs_http_client(settings(tls_verify=False))

    asyncio.run(client.aclose())

    assert "TLS certificate verification is disabled" in caplog.text


def test_missing_required_configuration_fails() -> None:
    with pytest.raises(ValidationError):
        GtfsHttpSettings.model_validate({})


def test_invalid_backoff_range_fails() -> None:
    with pytest.raises(ValidationError, match="BACKOFF_MAX"):
        settings(retry_backoff_base_seconds=2, retry_backoff_max_seconds=1)
