"""CassandraRepository against a real Cassandra (`make test-cassandra`).

Needs CASSANDRA_TEST_HOST (and optionally CASSANDRA_TEST_PORT/CASSANDRA_TEST_DATACENTER), for
example the Compose service: `docker compose up -d cassandra` and CASSANDRA_TEST_HOST=localhost.
The schema is created from infra/cassandra/serving.cql.template in a separate test keyspace.
"""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from datetime import UTC, date, datetime, timedelta
from pathlib import Path
from typing import Any

import pytest
from pydantic import SecretStr
from spb_transport_api.config import ApiSettings
from spb_transport_api.paging import PageRequest
from spb_transport_api.repository import CassandraRepository

pytestmark = pytest.mark.cassandra

KEYSPACE = "spb_transport_test"
TEMPLATE = Path(__file__).parents[2] / "infra" / "cassandra" / "serving.cql.template"
VERSION = "f" * 64
DAY = date(2026, 10, 1)


def _settings(host: str, port: int, datacenter: str) -> ApiSettings:
    return ApiSettings.model_validate(
        {
            "API_HOST": "127.0.0.1",
            "API_PORT": 8000,
            "CASSANDRA_HOST": host,
            "CASSANDRA_NATIVE_PORT": port,
            "CASSANDRA_DATACENTER": datacenter,
            "CASSANDRA_KEYSPACE": KEYSPACE,
            "CASSANDRA_USERNAME": None,
            "CASSANDRA_PASSWORD": None,
            "API_CASSANDRA_REQUEST_TIMEOUT_SECONDS": 10,
            "API_VEHICLE_FRESHNESS_SECONDS": 120,
            "API_DEFAULT_PAGE_SIZE": 2,
            "API_MAX_PAGE_SIZE": 10,
            "API_HISTORY_MAX_RANGE_HOURS": 24,
            "API_HISTORY_DEFAULT_RANGE_MINUTES": 60,
            "API_CORS_ORIGINS": "",
        }
    )


@pytest.fixture(scope="module")
def repository() -> Iterator[CassandraRepository]:
    host = os.environ.get("CASSANDRA_TEST_HOST")
    if not host:
        pytest.skip("CASSANDRA_TEST_HOST is not set")
    from cassandra.cluster import Cluster

    port = int(os.environ.get("CASSANDRA_TEST_PORT", "9042"))
    datacenter = os.environ.get("CASSANDRA_TEST_DATACENTER", "datacenter1")
    cluster = Cluster([host], port=port)
    session = cluster.connect()
    session.execute(f"DROP KEYSPACE IF EXISTS {KEYSPACE}", timeout=60)
    schema = (
        TEMPLATE.read_text(encoding="utf-8")
        .replace("{{KEYSPACE}}", KEYSPACE)
        .replace("{{DATACENTER}}", datacenter)
        .replace("{{REPLICATION_FACTOR}}", "1")
    )
    for statement in (s.strip() for s in schema.split(";")):
        if statement:
            session.execute(statement, timeout=60)
    _seed(session)
    settings = _settings(host, port, datacenter)
    repo = CassandraRepository(settings)
    try:
        yield repo
    finally:
        repo.close()
        session.execute(f"DROP KEYSPACE IF EXISTS {KEYSPACE}", timeout=60)
        cluster.shutdown()


def _seed(session: Any) -> None:
    ks = KEYSPACE
    base = datetime(2026, 10, 1, 8, 0)
    position = (
        f"INSERT INTO {ks}.{{table}} (vehicle_id, route_id, event_id, source_timestamp,"
        " collected_at, latitude, longitude, bearing, speed, label, license_plate, quality_flags,"
        " has_quality_flags{extra})"
        " VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s{marks})"
    )
    for i, vehicle in enumerate(("v1", "v2", "v3")):
        values = (
            vehicle,
            "r1",
            f"e{i}",
            base,
            base,
            59.9,
            30.3,
            None,
            0.0,
            None,
            None,
            ["x"],
            True,
        )
        session.execute(
            position.format(table="vehicle_latest_by_vehicle", extra="", marks=""), values
        )
        session.execute(
            position.format(table="vehicles_latest_by_route", extra="", marks=""), values
        )
    for minute in range(5):
        at = base + timedelta(minutes=minute)
        session.execute(
            position.format(
                table="vehicle_positions_by_vehicle_day", extra=", event_date", marks=", %s"
            ),
            ("v1", "r1", f"p{minute}", at, at, 59.9, 30.3, 90.0, 5.0, None, None, [], False, DAY),
        )
    forecasts = [{"forecast_rank": 0, "stop_id": "s1", "arrival_time": "2026-10-01T08:05:00.000Z"}]
    session.execute(
        f"INSERT INTO {ks}.eta_history_by_vehicle_day (vehicle_id, collected_date, collected_at,"
        " event_id, route_id, forecast_count, eta_status, forecasts_json)"
        " VALUES (%s, %s, %s, %s, %s, %s, %s, %s)",
        ("v1", DAY, base, "a" * 64, "r1", 1, "available", json.dumps(forecasts)),
    )
    session.execute(
        f"INSERT INTO {ks}.routes_by_feed (static_feed_version, route_id, agency_id,"
        " route_short_name, route_long_name, route_type) VALUES (%s, %s, %s, %s, %s, %s)",
        (VERSION, "r1", "a", "1", "Route one", 3),
    )
    for index, stop in enumerate(("s1", "s2")):
        session.execute(
            f"INSERT INTO {ks}.stops_by_route (static_feed_version, route_id, shape_id, stop_index,"
            " direction_id, pattern_id, stop_id, stop_code, stop_name, stop_lat, stop_lon,"
            " distance_along_m) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)",
            (VERSION, "r1", "north", index, 0, "p", stop, None, stop, 59.9, 30.3, index * 500.0),
        )
    session.execute(
        f"INSERT INTO {ks}.static_feed_current (feed, static_feed_version, published_at)"
        " VALUES (%s, %s, %s)",
        ("gtfs", VERSION, base),
    )


def test_latest_vehicles_page_with_cassandra_paging_state(repository: CassandraRepository) -> None:
    first = repository.vehicles_latest(PageRequest(2))
    assert len(first.items) == 2 and first.paging_state is not None
    second = repository.vehicles_latest(PageRequest(2, first.paging_state))

    ids = {p.vehicle_id for p in first.items + second.items}
    assert ids == {"v1", "v2", "v3"}
    assert first.items[0].source_timestamp.tzinfo is not None
    assert repository.vehicle_latest("v2") is not None
    assert [
        p.vehicle_id for p in repository.route_vehicles_latest("r1", PageRequest(10)).items
    ] == [
        "v1",
        "v2",
        "v3",
    ]


def test_history_range_is_half_open_and_newest_first(repository: CassandraRepository) -> None:
    start = datetime(2026, 10, 1, 8, 1, tzinfo=UTC)
    end = datetime(2026, 10, 1, 8, 4, tzinfo=UTC)

    page = repository.vehicle_positions("v1", DAY, start, end, PageRequest(10))

    assert [p.event_id for p in page.items] == ["p3", "p2", "p1"]


def test_eta_routes_stops_and_feed(repository: CassandraRepository) -> None:
    start = datetime(2026, 10, 1, 7, tzinfo=UTC)
    [eta] = repository.vehicle_eta(
        "v1", DAY, start, start + timedelta(hours=2), PageRequest(5)
    ).items
    assert eta.forecasts[0].stop_id == "s1"

    assert repository.current_feed_version() == VERSION
    assert [r.route_id for r in repository.routes(VERSION, PageRequest(5)).items] == ["r1"]
    stops = repository.route_stops(VERSION, "r1")
    assert [s.stop_id for s in stops.variants[0].stops] == ["s1", "s2"]
    repository.ping()


def test_secret_password_type_is_accepted() -> None:
    settings = _settings("localhost", 9042, "dc").model_copy(
        update={"cassandra_username": "u", "cassandra_password": SecretStr("p")}
    )
    assert settings.cassandra_password is not None
