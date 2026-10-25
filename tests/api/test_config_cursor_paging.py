from datetime import date

import pytest
from pydantic import ValidationError
from spb_transport_api.config import ApiSettings
from spb_transport_api.cursor import Cursor, InvalidCursor, decode_cursor, encode_cursor
from spb_transport_api.paging import PageRequest, read_days

from .fakes import page_of


def test_settings_parse_cors_and_empty_credentials(settings: ApiSettings) -> None:
    assert settings.cors_origin_list == ["http://localhost:5173"]
    assert settings.cassandra_username is None and settings.cassandra_password is None


@pytest.mark.parametrize(
    ("name", "value", "message"),
    [
        ("CASSANDRA_USERNAME", "app", "set together"),
        ("API_DEFAULT_PAGE_SIZE", "50", "API_DEFAULT_PAGE_SIZE"),
        ("API_HISTORY_DEFAULT_RANGE_MINUTES", "6000", "API_HISTORY_DEFAULT_RANGE_MINUTES"),
        ("CASSANDRA_KEYSPACE", "Bad-Name", "CASSANDRA_KEYSPACE"),
    ],
)
def test_settings_reject_inconsistent_values(
    settings: ApiSettings, monkeypatch: pytest.MonkeyPatch, name: str, value: str, message: str
) -> None:
    monkeypatch.setenv(name, value)

    with pytest.raises(ValidationError, match=message):
        ApiSettings()  # type: ignore[call-arg]


def test_settings_require_values(settings: ApiSettings, monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("API_VEHICLE_FRESHNESS_SECONDS")

    with pytest.raises(ValidationError, match="API_VEHICLE_FRESHNESS_SECONDS"):
        ApiSettings()  # type: ignore[call-arg]


def test_cursor_round_trip() -> None:
    cursor = Cursor(scope="positions|v1", paging_state=b"\x00\xffstate", day=date(2026, 10, 1))

    assert decode_cursor(encode_cursor(cursor), "positions|v1") == cursor
    assert decode_cursor(None, "anything") is None
    with pytest.raises(InvalidCursor, match="different query"):
        decode_cursor(encode_cursor(cursor), "positions|v2")
    with pytest.raises(InvalidCursor, match="malformed"):
        decode_cursor("bm90LWpzb24", "positions|v1")


def test_read_days_continues_across_partitions_and_pages() -> None:
    data = {date(2026, 10, 2): [1, 2], date(2026, 10, 1): [3, 4, 5]}
    days = sorted(data, reverse=True)
    fetch = lambda day, request: page_of(data[day], request)  # noqa: E731

    seen: list[int] = []
    day, state = None, None
    while True:
        page = read_days(fetch, days, 2, day, state)
        seen += page.items
        if page.next_day is None:
            break
        day, state = page.next_day, page.next_paging_state

    assert seen == [1, 2, 3, 4, 5]
    with pytest.raises(ValueError, match="outside"):
        read_days(fetch, days, 2, date(2020, 1, 1), None)


def test_read_days_reports_end_of_range() -> None:
    page = read_days(lambda _day, _request: page_of([1], PageRequest(5)), [date(2026, 10, 1)], 5)

    assert page.items == [1] and page.next_day is None
