import base64
import hashlib
from datetime import datetime
from uuid import uuid4

import httpx
from spb_transport_contracts import RawEndpoint, RawResponseV1


def build_raw_response(
    response: httpx.Response,
    endpoint: RawEndpoint,
    requested_at: datetime,
    collected_at: datetime,
    *,
    decode_error: str | None = None,
) -> RawResponseV1:
    payload = response.content
    return RawResponseV1(
        schema_version="1",
        request_id=str(uuid4()),
        endpoint=endpoint,
        requested_at=requested_at,
        collected_at=collected_at,
        http_status=response.status_code,
        content_type=response.headers.get("content-type"),
        payload_base64=base64.b64encode(payload).decode("ascii"),
        sha256=hashlib.sha256(payload).hexdigest(),
        decode_error=decode_error,
    )
