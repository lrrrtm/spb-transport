"""In-memory ServingRepository with Cassandra-like paging (opaque byte offsets)."""

from __future__ import annotations

from datetime import UTC, date, datetime

from spb_transport_api.models import EtaSnapshot, Route, RouteStops, VehiclePosition
from spb_transport_api.paging import PageRequest, Slice
from spb_transport_api.repository import InvalidPagingState, ServingUnavailable

NOW = datetime(2026, 10, 1, 0, 30, tzinfo=UTC)


def page_of[T](rows: list[T], request: PageRequest) -> Slice[T]:
    if request.paging_state is not None and not request.paging_state.startswith(b"off:"):
        raise InvalidPagingState("bad state")
    offset = 0 if request.paging_state is None else int(request.paging_state[4:])
    chunk = rows[offset : offset + request.limit]
    following = offset + request.limit
    return Slice(chunk, f"off:{following}".encode() if following < len(rows) else None)


class FakeRepository:
    def __init__(self) -> None:
        self.available = True
        self.feed_version: str | None = "f" * 64
        self.latest: dict[str, VehiclePosition] = {}
        self.history: dict[tuple[str, date], list[VehiclePosition]] = {}
        self.eta: dict[tuple[str, date], list[EtaSnapshot]] = {}
        self.route_rows: list[Route] = []
        self.stops: dict[str, RouteStops] = {}
        self.calls: list[tuple[str, object]] = []
        self.closed = False

    def _check(self) -> None:
        if not self.available:
            raise ServingUnavailable("Cassandra is not available")

    def ping(self) -> None:
        self._check()

    def current_feed_version(self) -> str | None:
        self._check()
        return self.feed_version

    def vehicle_latest(self, vehicle_id: str) -> VehiclePosition | None:
        self._check()
        return self.latest.get(vehicle_id)

    def vehicles_latest(self, page: PageRequest) -> Slice[VehiclePosition]:
        self._check()
        return page_of(sorted(self.latest.values(), key=lambda p: p.vehicle_id), page)

    def route_vehicles_latest(self, route_id: str, page: PageRequest) -> Slice[VehiclePosition]:
        self._check()
        rows = [p for p in self.latest.values() if p.route_id == route_id]
        return page_of(sorted(rows, key=lambda p: p.vehicle_id), page)

    def vehicle_positions(
        self, vehicle_id: str, day: date, start: datetime, end: datetime, page: PageRequest
    ) -> Slice[VehiclePosition]:
        self._check()
        self.calls.append(("positions", day))
        rows = [
            p for p in self.history.get((vehicle_id, day), []) if start <= p.source_timestamp < end
        ]
        return page_of(sorted(rows, key=lambda p: p.source_timestamp, reverse=True), page)

    def vehicle_eta(
        self, vehicle_id: str, day: date, start: datetime, end: datetime, page: PageRequest
    ) -> Slice[EtaSnapshot]:
        self._check()
        rows = [e for e in self.eta.get((vehicle_id, day), []) if start <= e.collected_at < end]
        return page_of(sorted(rows, key=lambda e: e.collected_at, reverse=True), page)

    def routes(self, feed_version: str, page: PageRequest) -> Slice[Route]:
        self._check()
        return page_of(self.route_rows, page)

    def route(self, feed_version: str, route_id: str) -> Route | None:
        self._check()
        return next((r for r in self.route_rows if r.route_id == route_id), None)

    def route_stops(self, feed_version: str, route_id: str) -> RouteStops:
        self._check()
        return self.stops.get(
            route_id, RouteStops(route_id=route_id, static_feed_version=feed_version, variants=[])
        )

    def close(self) -> None:
        self.closed = True


def position(
    vehicle_id: str, at: datetime, route_id: str = "r1", speed: float = 0.0
) -> VehiclePosition:
    return VehiclePosition(
        vehicle_id=vehicle_id,
        route_id=route_id,
        event_id=f"{vehicle_id}-{at.isoformat()}",
        source_timestamp=at.astimezone(UTC),
        collected_at=at.astimezone(UTC),
        latitude=59.93,
        longitude=30.31,
        bearing=None,
        speed=speed,
        label=None,
        license_plate=None,
        quality_flags=["stationary_observation"] if speed == 0 else [],
    )
