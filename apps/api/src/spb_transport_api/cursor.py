"""Opaque pagination cursors.

A cursor carries the Cassandra paging state and, for multi-day history, the day partition to
continue from. It is bound to the query scope (endpoint and its filters): a cursor issued for one
query is rejected by another instead of silently returning wrong rows.
"""

from __future__ import annotations

import base64
import binascii
import json
from dataclasses import dataclass
from datetime import date


class InvalidCursor(ValueError):
    pass


@dataclass(frozen=True)
class Cursor:
    scope: str
    paging_state: bytes | None = None
    day: date | None = None


def _b64encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode("ascii").rstrip("=")


def _b64decode(text: str) -> bytes:
    return base64.urlsafe_b64decode(text + "=" * (-len(text) % 4))


def encode_cursor(cursor: Cursor) -> str:
    payload: dict[str, str] = {"q": cursor.scope}
    if cursor.paging_state is not None:
        payload["s"] = _b64encode(cursor.paging_state)
    if cursor.day is not None:
        payload["d"] = cursor.day.isoformat()
    return _b64encode(json.dumps(payload, separators=(",", ":")).encode("utf-8"))


def decode_cursor(token: str | None, scope: str) -> Cursor | None:
    if token is None:
        return None
    try:
        payload = json.loads(_b64decode(token))
        if not isinstance(payload, dict) or payload.get("q") != scope:
            raise InvalidCursor("cursor belongs to a different query")
        state = payload.get("s")
        day = payload.get("d")
        return Cursor(
            scope=scope,
            paging_state=None if state is None else _b64decode(state),
            day=None if day is None else date.fromisoformat(day),
        )
    except InvalidCursor:
        raise
    except (ValueError, TypeError, binascii.Error, UnicodeDecodeError) as error:
        raise InvalidCursor("malformed cursor") from error
