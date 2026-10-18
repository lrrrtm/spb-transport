import asyncio
import logging
import random
import ssl
from dataclasses import dataclass
from types import TracebackType

import httpx

from spb_transport_collector.config import GtfsHttpSettings

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class RetryPolicy:
    max_attempts: int
    backoff_base_seconds: float
    backoff_max_seconds: float
    jitter_seconds: float

    def delay(self, failed_attempt: int) -> float:
        backoff = min(
            self.backoff_base_seconds * 2 ** (failed_attempt - 1),
            self.backoff_max_seconds,
        )
        return float(backoff + random.uniform(0, self.jitter_seconds))


class GtfsHttpClient:
    def __init__(self, client: httpx.AsyncClient, retry_policy: RetryPolicy) -> None:
        self._client = client
        self._retry_policy = retry_policy

    async def get(
        self,
        endpoint: str,
        *,
        params: httpx.QueryParams | None = None,
    ) -> httpx.Response:
        if not endpoint or endpoint.startswith("/") or "://" in endpoint:
            raise ValueError("endpoint must be a non-empty path relative to GTFS base URL")

        for attempt in range(1, self._retry_policy.max_attempts + 1):
            try:
                response = await self._client.get(endpoint, params=params)
            except httpx.TransportError as error:
                if attempt == self._retry_policy.max_attempts:
                    raise
                await self._wait_before_retry(attempt, error=type(error).__name__)
                continue

            if 500 <= response.status_code < 600:
                if attempt == self._retry_policy.max_attempts:
                    response.raise_for_status()
                await self._wait_before_retry(attempt, status_code=response.status_code)
                continue

            response.raise_for_status()
            return response

        raise RuntimeError("unreachable retry state")

    async def _wait_before_retry(
        self,
        attempt: int,
        *,
        error: str | None = None,
        status_code: int | None = None,
    ) -> None:
        delay = self._retry_policy.delay(attempt)
        logger.warning(
            "GTFS request failed; retrying",
            extra={
                "event": "gtfs_request_retry",
                "attempt": attempt,
                "delay_seconds": delay,
                "error": error,
                "status_code": status_code,
            },
        )
        await asyncio.sleep(delay)

    async def aclose(self) -> None:
        await self._client.aclose()

    async def __aenter__(self) -> "GtfsHttpClient":
        return self

    async def __aexit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ) -> None:
        await self.aclose()


def create_gtfs_http_client(settings: GtfsHttpSettings) -> GtfsHttpClient:
    verify: bool | ssl.SSLContext
    if not settings.tls_verify:
        logger.warning(
            "GTFS TLS certificate verification is disabled",
            extra={"event": "gtfs_tls_verification_disabled"},
        )
        verify = False
    elif settings.ca_bundle is not None:
        verify = ssl.create_default_context(cafile=str(settings.ca_bundle))
    else:
        verify = True

    client = httpx.AsyncClient(
        base_url=str(settings.base_url),
        timeout=httpx.Timeout(
            connect=settings.connect_timeout_seconds,
            read=settings.read_timeout_seconds,
            write=settings.write_timeout_seconds,
            pool=settings.pool_timeout_seconds,
        ),
        verify=verify,
    )
    policy = RetryPolicy(
        max_attempts=settings.retry_max_attempts,
        backoff_base_seconds=settings.retry_backoff_base_seconds,
        backoff_max_seconds=settings.retry_backoff_max_seconds,
        jitter_seconds=settings.retry_jitter_seconds,
    )
    return GtfsHttpClient(client, policy)
