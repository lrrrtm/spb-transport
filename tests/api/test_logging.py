import json
import logging

from spb_transport_api.__main__ import JsonFormatter


def test_json_formatter_keeps_operational_fields() -> None:
    record = logging.LogRecord(
        "api",
        logging.INFO,
        __file__,
        1,
        "completed",
        (),
        None,
    )
    record.event = "http_request"
    record.method = "GET"
    record.path = "/ready"
    record.status_code = 200
    record.duration_ms = 12.5

    payload = json.loads(JsonFormatter().format(record))

    assert payload | {"timestamp": "ignored"} == {
        "timestamp": "ignored",
        "level": "INFO",
        "logger": "api",
        "message": "completed",
        "event": "http_request",
        "method": "GET",
        "path": "/ready",
        "status_code": 200,
        "duration_ms": 12.5,
    }
