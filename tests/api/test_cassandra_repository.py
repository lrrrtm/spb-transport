import json
from collections import namedtuple
from datetime import UTC, date, datetime
from typing import Any

import pytest
from cassandra import InvalidRequest
from cassandra.cluster import NoHostAvailable
from spb_transport_api.config import ApiSettings
from spb_transport_api.paging import PageRequest
from spb_transport_api.repository import (
    CassandraRepository,
    InvalidPagingState,
    ServingUnavailable,
)

PositionRow = namedtuple(
    "PositionRow",
    "vehicle_id route_id event_id source_timestamp collected_at latitude longitude bearing speed"
    " label license_plate quality_flags",
)
EtaRow = namedtuple("EtaRow", "vehicle_id route_id event_id collected_at eta_status forecasts_json")
StopRow = namedtuple(
    "StopRow",
    "shape_id stop_index direction_id pattern_id stop_id stop_code stop_name stop_lat stop_lon"
    " distance_along_m",
)


class FakeResult:
    def __init__(self, rows: list[Any], paging_state: bytes | None = None) -> None:
        self.current_rows = rows
        self.paging_state = paging_state

    def one(self) -> Any:
        return self.current_rows[0] if self.current_rows else None

    def __iter__(self) -> Any:
        return iter(self.current_rows)


class FakeBound:
    def __init__(self, query: str, values: list[Any]) -> None:
        self.query, self.values, self.fetch_size = query, values, None


class FakePrepared:
    def __init__(self, query: str) -> None:
        self.query = query

    def bind(self, values: list[Any]) -> FakeBound:
        return FakeBound(self.query, values)


class FakeSession:
    def __init__(self) -> None:
        self.results: dict[str, FakeResult] = {}
        self.executed: list[tuple[FakeBound, dict[str, Any]]] = []
        self.error: Exception | None = None

    def prepare(self, query: str) -> FakePrepared:
        return FakePrepared(query)

    def execute(self, query: FakeBound, parameters: Any = None, **kwargs: Any) -> FakeResult:
        self.executed.append((query, kwargs))
        if self.error is not None:
            raise self.error
        table = next(
            name
            for name in self.results
            if f".{name} " in query.query or query.query.endswith(name)
        )
        return self.results[table]


@pytest.fixture
def session() -> FakeSession:
    return FakeSession()


@pytest.fixture
def repo(settings: ApiSettings, session: FakeSession) -> CassandraRepository:
    return CassandraRepository(settings, connect=lambda _: session)


def test_positions_are_read_from_one_day_partition_with_paging(
    repo: CassandraRepository, session: FakeSession
) -> None:
    naive = datetime(2026, 10, 1, 8, 0)
    session.results["vehicle_positions_by_vehicle_day"] = FakeResult(
        [PositionRow("v1", "r1", "e1", naive, naive, 59.9, 30.3, None, 0.0, None, None, None)],
        b"next",
    )
    start = datetime(2026, 10, 1, 7, tzinfo=UTC)
    end = datetime(2026, 10, 1, 9, tzinfo=UTC)

    page = repo.vehicle_positions("v1", date(2026, 10, 1), start, end, PageRequest(10, b"prev"))

    bound, kwargs = session.executed[-1]
    assert "WHERE vehicle_id = ? AND event_date = ?" in bound.query
    assert bound.values == [
        "v1",
        date(2026, 10, 1),
        start.replace(tzinfo=None),
        end.replace(tzinfo=None),
    ]
    assert bound.fetch_size == 10 and kwargs["paging_state"] == b"prev"
    assert page.paging_state == b"next"
    [item] = page.items
    assert item.source_timestamp == datetime(2026, 10, 1, 8, tzinfo=UTC)
    assert item.speed == 0.0 and item.quality_flags == []


def test_eta_forecasts_are_parsed_from_spark_json(
    repo: CassandraRepository, session: FakeSession
) -> None:
    forecasts = [
        {"forecast_rank": 1, "stop_id": "s2", "arrival_time": "2026-10-01T08:05:00.000Z"},
        {"forecast_rank": 0, "stop_id": "s1", "arrival_time": "2026-10-01T08:01:00.000Z"},
    ]
    session.results["eta_history_by_vehicle_day"] = FakeResult(
        [EtaRow("v1", "r1", "e" * 64, datetime(2026, 10, 1, 8), "available", json.dumps(forecasts))]
    )
    start = datetime(2026, 10, 1, 7, tzinfo=UTC)

    [snapshot] = repo.vehicle_eta(
        "v1", date(2026, 10, 1), start, start.replace(hour=9), PageRequest(5)
    ).items

    assert [f.stop_id for f in snapshot.forecasts] == ["s1", "s2"]
    assert snapshot.forecasts[0].arrival_time == datetime(2026, 10, 1, 8, 1, tzinfo=UTC)


def test_route_stops_are_grouped_by_shape_in_order(
    repo: CassandraRepository, session: FakeSession
) -> None:
    session.results["stops_by_route"] = FakeResult(
        [
            StopRow("south", 0, 1, "p2", "s9", None, "Конечная", 59.9, 30.3, 0.0),
            StopRow("north", 1, 0, "p1", "s2", None, "Вторая", 59.9, 30.3, 500.0),
            StopRow("north", 0, 0, "p1", "s1", "101", "Первая", 59.9, 30.3, 0.0),
        ]
    )

    stops = repo.route_stops("f" * 64, "r1")

    assert [(v.shape_id, [s.stop_id for s in v.stops]) for v in stops.variants] == [
        ("north", ["s1", "s2"]),
        ("south", ["s9"]),
    ]


def test_connection_failure_is_unavailable_and_retried(settings: ApiSettings) -> None:
    attempts: list[int] = []

    def connect(_: ApiSettings) -> Any:
        attempts.append(1)
        raise NoHostAvailable("down", {})

    repo = CassandraRepository(settings, connect=connect)

    for _ in range(2):
        with pytest.raises(ServingUnavailable):
            repo.ping()
    assert len(attempts) == 2


def test_query_errors_are_translated(repo: CassandraRepository, session: FakeSession) -> None:
    session.results["vehicle_latest_by_vehicle"] = FakeResult([])
    session.error = InvalidRequest("Invalid value for the paging state")
    with pytest.raises(InvalidPagingState):
        repo.vehicles_latest(PageRequest(5, b"garbage"))

    session.error = TimeoutError("read timed out")
    with pytest.raises(ServingUnavailable):
        repo.vehicles_latest(PageRequest(5))


def test_missing_rows_are_none(repo: CassandraRepository, session: FakeSession) -> None:
    session.results["static_feed_current"] = FakeResult([])
    session.results["vehicle_latest_by_vehicle"] = FakeResult([])

    assert repo.current_feed_version() is None
    assert repo.vehicle_latest("nope") is None
