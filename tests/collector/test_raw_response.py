import base64
import hashlib
from datetime import UTC, datetime, timedelta

import httpx
from spb_transport_collector.raw_response import build_raw_response
from spb_transport_contracts import RawEndpoint


def test_build_raw_response_preserves_source_bytes_and_metadata() -> None:
    payload = b"\x00protobuf\xff"
    requested_at = datetime(2026, 9, 13, 9, 0, tzinfo=UTC)
    request = httpx.Request("GET", "https://example.test/realtime/vehicle")
    response = httpx.Response(
        200,
        content=payload,
        headers={"content-type": "application/x-protobuf"},
        request=request,
    )

    event = build_raw_response(
        response,
        RawEndpoint.VEHICLE,
        requested_at,
        requested_at + timedelta(seconds=1),
    )

    assert base64.b64decode(event.payload_base64) == payload
    assert event.sha256 == hashlib.sha256(payload).hexdigest()
    assert event.http_status == 200
    assert event.content_type == "application/x-protobuf"
