"""Pagination over one Cassandra partition and over a sequence of day partitions."""

from __future__ import annotations

from collections.abc import Callable, Sequence
from dataclasses import dataclass
from datetime import date


@dataclass(frozen=True)
class PageRequest:
    limit: int
    paging_state: bytes | None = None


@dataclass(frozen=True)
class Slice[T]:
    items: list[T]
    paging_state: bytes | None


@dataclass(frozen=True)
class HistoryPage[T]:
    items: list[T]
    next_day: date | None
    next_paging_state: bytes | None


def read_days[T](
    fetch: Callable[[date, PageRequest], Slice[T]],
    days: Sequence[date],
    limit: int,
    start_day: date | None = None,
    paging_state: bytes | None = None,
) -> HistoryPage[T]:
    """Read up to `limit` rows across day partitions in the given order.

    Continues from `start_day`/`paging_state` of a previous page. The returned `next_day` and
    `next_paging_state` locate the following page; `next_day is None` means the range is done.
    """
    if start_day is not None and start_day not in days:
        raise ValueError("cursor day is outside the requested range")
    index = 0 if start_day is None else days.index(start_day)
    items: list[T] = []
    state = paging_state if start_day is not None else None
    while index < len(days) and len(items) < limit:
        page = fetch(days[index], PageRequest(limit - len(items), state))
        items.extend(page.items)
        if page.paging_state is not None and page.paging_state != state:
            state = page.paging_state
            continue
        index += 1
        state = None
    if index >= len(days):
        return HistoryPage(items, None, None)
    return HistoryPage(items, days[index], state)
