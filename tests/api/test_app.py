from datetime import timedelta

from fastapi.testclient import TestClient
from spb_transport_api.app import create_app
from spb_transport_api.config import ApiSettings
from spb_transport_api.cursor import Cursor, encode_cursor
from spb_transport_api.models import (
    AnalyticsDataset,
    EtaSnapshot,
    Route,
    RouteStop,
    RouteStops,
    RouteVariant,
)

from api.fakes import NOW, FakeRepository, position

BASE = "/api/v1"


def test_health_does_not_touch_cassandra(client: TestClient, repository: FakeRepository) -> None:
    repository.available = False

    assert client.get("/health").json() == {"status": "ok"}


def test_ready_reports_cassandra_and_feed_version(
    client: TestClient, repository: FakeRepository
) -> None:
    ready = client.get("/ready")
    assert ready.status_code == 200
    assert ready.json()["static_feed_version"] == "f" * 64

    repository.available = False
    down = client.get("/ready")
    assert down.status_code == 503
    assert down.json()["status"] == "unavailable"


def test_vehicles_are_paged_and_filtered_by_freshness(
    client: TestClient, repository: FakeRepository
) -> None:
    for name, age in (("a", 10), ("b", 500), ("c", 30), ("d", 60)):
        repository.latest[name] = position(name, NOW - timedelta(seconds=age))

    first = client.get(f"{BASE}/vehicles").json()
    assert [v["vehicle_id"] for v in first["items"]] == ["a"]  # b is stale, page size 2
    second = client.get(f"{BASE}/vehicles", params={"cursor": first["next_cursor"]}).json()
    assert [v["vehicle_id"] for v in second["items"]] == ["c", "d"]
    assert second["next_cursor"] is None


def test_speed_zero_and_quality_flags_are_returned(
    client: TestClient, repository: FakeRepository
) -> None:
    repository.latest["a"] = position("a", NOW - timedelta(seconds=5), speed=0.0)

    body = client.get(f"{BASE}/vehicles/a").json()

    assert body["speed"] == 0.0
    assert body["quality_flags"] == ["stationary_observation"]
    assert body["source_timestamp"].endswith("Z") or body["source_timestamp"].endswith("+00:00")


def test_single_vehicle_without_fresh_position_is_404(
    client: TestClient, repository: FakeRepository
) -> None:
    repository.latest["old"] = position("old", NOW - timedelta(hours=1))

    assert client.get(f"{BASE}/vehicles/old").status_code == 404
    assert client.get(f"{BASE}/vehicles/missing").status_code == 404


def test_history_spans_day_partitions_newest_first(
    client: TestClient, repository: FakeRepository
) -> None:
    # NOW is 00:30 UTC: the default one-hour window covers two event_date partitions.
    for minutes in (10, 20, 40, 50):
        at = NOW - timedelta(minutes=minutes)
        repository.history.setdefault(("v1", at.date()), []).append(position("v1", at))

    pages = []
    cursor = None
    while True:
        params = {"limit": 3} | ({"cursor": cursor} if cursor else {})
        body = client.get(f"{BASE}/vehicles/v1/positions", params=params).json()
        pages.append([item["source_timestamp"][11:16] for item in body["items"]])
        cursor = body["next_cursor"]
        if cursor is None:
            break

    assert [t for page in pages for t in page] == ["00:20", "00:10", "23:50", "23:40"]
    assert pages[0] == ["00:20", "00:10", "23:50"]


def test_history_validates_range(client: TestClient) -> None:
    url = f"{BASE}/vehicles/v1/positions"
    too_long = {"from": "2026-09-29T00:00:00Z", "to": "2026-10-01T00:00:00Z"}
    reversed_range = {"from": "2026-10-01T01:00:00Z", "to": "2026-10-01T00:00:00Z"}
    naive = {"from": "2026-10-01T00:00:00", "to": "2026-10-01T00:30:00Z"}

    assert client.get(url, params=too_long).status_code == 422
    assert client.get(url, params=reversed_range).status_code == 422
    assert client.get(url, params=naive).status_code == 422
    assert client.get(url, params={"limit": 6}).status_code == 422
    assert client.get(url, params={"limit": 0}).status_code == 422


def test_cursor_is_bound_to_its_query(client: TestClient, repository: FakeRepository) -> None:
    for name in "abc":
        repository.latest[name] = position(name, NOW)
    cursor = client.get(f"{BASE}/vehicles").json()["next_cursor"]

    other_query = client.get(f"{BASE}/routes/r1/vehicles", params={"cursor": cursor})
    garbage = client.get(f"{BASE}/vehicles", params={"cursor": "%%%"})

    assert other_query.status_code == 400
    assert garbage.status_code == 400


def test_eta_history_keeps_empty_snapshots(client: TestClient, repository: FakeRepository) -> None:
    at = NOW - timedelta(minutes=5)
    repository.eta[("v1", at.date())] = [
        EtaSnapshot(
            vehicle_id="v1",
            route_id="r1",
            event_id="e" * 64,
            collected_at=at,
            eta_status="empty",
            forecasts=[],
        )
    ]

    body = client.get(f"{BASE}/vehicles/v1/eta").json()

    assert [(e["eta_status"], e["forecasts"]) for e in body["items"]] == [("empty", [])]


def test_routes_use_the_published_feed_version(
    client: TestClient, repository: FakeRepository
) -> None:
    repository.route_rows = [
        Route(route_id=f"r{i}", agency_id="a", short_name=str(i), long_name=None, route_type=3)
        for i in range(3)
    ]
    repository.stops["r1"] = RouteStops(
        route_id="r1",
        static_feed_version="f" * 64,
        variants=[
            RouteVariant(
                shape_id="north",
                direction_id=0,
                pattern_id="p",
                stops=[
                    RouteStop(
                        stop_index=0,
                        stop_id="s1",
                        stop_code=None,
                        stop_name="Невский",
                        latitude=59.93,
                        longitude=30.31,
                        distance_along_m=0.0,
                    )
                ],
            )
        ],
    )

    page = client.get(f"{BASE}/routes").json()
    assert page["static_feed_version"] == "f" * 64
    assert [r["route_id"] for r in page["items"]] == ["r0", "r1"]
    assert client.get(f"{BASE}/routes/r1").json()["short_name"] == "1"
    stops = client.get(f"{BASE}/routes/r1/stops").json()
    assert stops["variants"][0]["stops"][0]["stop_name"] == "Невский"
    assert client.get(f"{BASE}/routes/missing").status_code == 404
    assert client.get(f"{BASE}/routes/missing/stops").status_code == 404


def test_routes_are_503_until_static_metadata_is_published(
    client: TestClient, repository: FakeRepository
) -> None:
    repository.feed_version = None

    assert client.get(f"{BASE}/routes").status_code == 503


def test_route_vehicles_are_fresh_vehicles_of_the_route(
    client: TestClient, repository: FakeRepository
) -> None:
    repository.latest["a"] = position("a", NOW, route_id="r1")
    repository.latest["b"] = position("b", NOW, route_id="r2")
    repository.latest["c"] = position("c", NOW - timedelta(hours=1), route_id="r1")

    body = client.get(f"{BASE}/routes/r1/vehicles").json()

    assert [v["vehicle_id"] for v in body["items"]] == ["a"]


def test_analytics_uses_published_dataset_pointers(
    client: TestClient, repository: FakeRepository
) -> None:
    repository.analytics["segment_metrics"] = AnalyticsDataset(
        dataset="segment_metrics",
        algorithm_version="segment-metrics-v1",
        model_id=None,
        static_feed_version="f" * 64,
        period_start=NOW.date(),
        period_end=NOW.date(),
        training_start=None,
        training_end=None,
        updated_at=NOW,
    )

    overview = client.get(f"{BASE}/analytics")
    segments = client.get(f"{BASE}/analytics/routes/r1/segments")

    assert [item["dataset"] for item in overview.json()["datasets"]] == ["segment_metrics"]
    assert segments.status_code == 200 and segments.json() == []
    assert client.get(f"{BASE}/analytics/routes/r1/anomalies").status_code == 404


def test_cassandra_failure_is_503(client: TestClient, repository: FakeRepository) -> None:
    repository.available = False

    assert client.get(f"{BASE}/vehicles").status_code == 503
    assert client.get(f"{BASE}/vehicles/v1/positions").status_code == 503


def test_stale_paging_state_is_400(client: TestClient) -> None:
    cursor = encode_cursor(Cursor(scope="vehicles", paging_state=b"not-a-state"))

    assert client.get(f"{BASE}/vehicles", params={"cursor": cursor}).status_code == 400


def test_cors_allows_configured_origin_only(client: TestClient) -> None:
    allowed = client.get("/health", headers={"Origin": "http://localhost:5173"})
    other = client.get("/health", headers={"Origin": "http://evil.example"})

    assert allowed.headers["access-control-allow-origin"] == "http://localhost:5173"
    assert "access-control-allow-origin" not in other.headers


def test_openapi_documents_every_endpoint(client: TestClient) -> None:
    paths = set(client.get("/openapi.json").json()["paths"])

    assert paths == {
        "/health",
        "/ready",
        f"{BASE}/analytics",
        f"{BASE}/analytics/clusters",
        f"{BASE}/analytics/routes/{{route_id}}/segments",
        f"{BASE}/analytics/routes/{{route_id}}/anomalies",
        f"{BASE}/analytics/routes/{{route_id}}/eta-comparison",
        f"{BASE}/vehicles",
        f"{BASE}/vehicles/{{vehicle_id}}",
        f"{BASE}/vehicles/{{vehicle_id}}/positions",
        f"{BASE}/vehicles/{{vehicle_id}}/eta",
        f"{BASE}/routes",
        f"{BASE}/routes/{{route_id}}",
        f"{BASE}/routes/{{route_id}}/stops",
        f"{BASE}/routes/{{route_id}}/vehicles",
    }


def test_repository_is_closed_on_shutdown(
    settings: ApiSettings, repository: FakeRepository
) -> None:
    with TestClient(create_app(settings, lambda _: repository, clock=lambda: NOW)):
        pass

    assert repository.closed
