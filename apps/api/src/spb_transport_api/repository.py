"""Read access to the Cassandra serving tables.

Every method maps to one prepared query on one partition (or the small single-row-per-vehicle
latest table), as required by ADR 0007: the API never runs ad hoc or analytical queries.
"""

from __future__ import annotations

import json
import logging
import threading
from collections.abc import Callable, Sequence
from datetime import UTC, date, datetime
from typing import Any, Protocol

from spb_transport_api.config import ApiSettings
from spb_transport_api.models import (
    EtaSnapshot,
    Route,
    RouteStop,
    RouteStops,
    RouteVariant,
    StopForecast,
    VehiclePosition,
)
from spb_transport_api.paging import PageRequest, Slice

logger = logging.getLogger(__name__)

FEED_NAME = "gtfs"
POSITION_COLUMNS = (
    "vehicle_id",
    "route_id",
    "event_id",
    "source_timestamp",
    "collected_at",
    "latitude",
    "longitude",
    "bearing",
    "speed",
    "label",
    "license_plate",
    "quality_flags",
)
ETA_COLUMNS = ("vehicle_id", "route_id", "event_id", "collected_at", "eta_status", "forecasts_json")
ROUTE_COLUMNS = ("route_id", "agency_id", "route_short_name", "route_long_name", "route_type")
STOP_COLUMNS = (
    "shape_id",
    "stop_index",
    "direction_id",
    "pattern_id",
    "stop_id",
    "stop_code",
    "stop_name",
    "stop_lat",
    "stop_lon",
    "distance_along_m",
)


class ServingUnavailable(RuntimeError):
    """Cassandra cannot answer right now; the API responds 503."""


class InvalidPagingState(ValueError):
    """Cassandra rejected the paging state taken from a client cursor."""


class ServingRepository(Protocol):
    def ping(self) -> None: ...

    def current_feed_version(self) -> str | None: ...

    def vehicle_latest(self, vehicle_id: str) -> VehiclePosition | None: ...

    def vehicles_latest(self, page: PageRequest) -> Slice[VehiclePosition]: ...

    def route_vehicles_latest(self, route_id: str, page: PageRequest) -> Slice[VehiclePosition]: ...

    def vehicle_positions(
        self, vehicle_id: str, day: date, start: datetime, end: datetime, page: PageRequest
    ) -> Slice[VehiclePosition]: ...

    def vehicle_eta(
        self, vehicle_id: str, day: date, start: datetime, end: datetime, page: PageRequest
    ) -> Slice[EtaSnapshot]: ...

    def routes(self, feed_version: str, page: PageRequest) -> Slice[Route]: ...

    def route(self, feed_version: str, route_id: str) -> Route | None: ...

    def route_stops(self, feed_version: str, route_id: str) -> RouteStops: ...

    def close(self) -> None: ...


class Session(Protocol):
    """The part of `cassandra.cluster.Session` the repository uses."""

    def prepare(self, query: str) -> Any: ...

    def execute(self, query: Any, parameters: Any = ..., **kwargs: Any) -> Any: ...


def as_utc(value: datetime | None) -> datetime | None:
    """Cassandra returns naive UTC datetimes; the API exposes timezone-aware values."""
    if value is None:
        return None
    return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)


def _naive_utc(value: datetime) -> datetime:
    return value.astimezone(UTC).replace(tzinfo=None)


def position_from_row(row: Any) -> VehiclePosition:
    source_timestamp = as_utc(row.source_timestamp)
    assert source_timestamp is not None
    return VehiclePosition(
        vehicle_id=row.vehicle_id,
        route_id=row.route_id,
        event_id=row.event_id,
        source_timestamp=source_timestamp,
        collected_at=as_utc(row.collected_at),
        latitude=row.latitude,
        longitude=row.longitude,
        bearing=row.bearing,
        speed=row.speed,
        label=row.label,
        license_plate=row.license_plate,
        quality_flags=list(row.quality_flags or ()),
    )


def _parse_time(value: str) -> datetime:
    parsed = datetime.fromisoformat(value.replace("Z", "+00:00"))
    return parsed.replace(tzinfo=UTC) if parsed.tzinfo is None else parsed.astimezone(UTC)


def eta_from_row(row: Any) -> EtaSnapshot:
    """`forecasts_json` is Spark `to_json` of the contract forecasts (UTC timestamps)."""
    collected_at = as_utc(row.collected_at)
    assert collected_at is not None
    forecasts = json.loads(row.forecasts_json) if row.forecasts_json else []
    return EtaSnapshot(
        vehicle_id=row.vehicle_id,
        route_id=row.route_id,
        event_id=row.event_id,
        collected_at=collected_at,
        eta_status=row.eta_status,
        forecasts=[
            StopForecast(
                forecast_rank=item["forecast_rank"],
                stop_id=item["stop_id"],
                arrival_time=_parse_time(item["arrival_time"]),
            )
            for item in sorted(forecasts, key=lambda item: item["forecast_rank"])
        ],
    )


def route_from_row(row: Any) -> Route:
    return Route(
        route_id=row.route_id,
        agency_id=row.agency_id,
        short_name=row.route_short_name,
        long_name=row.route_long_name,
        route_type=row.route_type,
    )


def route_stops_from_rows(feed_version: str, route_id: str, rows: Sequence[Any]) -> RouteStops:
    variants: dict[str, tuple[Any, list[RouteStop]]] = {}
    for row in sorted(rows, key=lambda r: (r.shape_id, r.stop_index)):
        _, stops = variants.setdefault(row.shape_id, (row, []))
        stops.append(
            RouteStop(
                stop_index=row.stop_index,
                stop_id=row.stop_id,
                stop_code=row.stop_code,
                stop_name=row.stop_name,
                latitude=row.stop_lat,
                longitude=row.stop_lon,
                distance_along_m=row.distance_along_m,
            )
        )
    return RouteStops(
        route_id=route_id,
        static_feed_version=feed_version,
        variants=[
            RouteVariant(
                shape_id=shape_id,
                direction_id=first.direction_id,
                pattern_id=first.pattern_id,
                stops=stops,
            )
            for shape_id, (first, stops) in variants.items()
        ],
    )


def connect_cluster(settings: ApiSettings) -> Session:
    from cassandra.auth import PlainTextAuthProvider
    from cassandra.cluster import Cluster
    from cassandra.policies import DCAwareRoundRobinPolicy

    auth = None
    if settings.cassandra_username is not None and settings.cassandra_password is not None:
        auth = PlainTextAuthProvider(
            settings.cassandra_username, settings.cassandra_password.get_secret_value()
        )
    cluster = Cluster(
        [settings.cassandra_host],
        port=settings.cassandra_port,
        load_balancing_policy=DCAwareRoundRobinPolicy(local_dc=settings.cassandra_datacenter),
        auth_provider=auth,
        connect_timeout=settings.cassandra_request_timeout_seconds,
    )
    session: Session = cluster.connect()
    return session


class CassandraRepository:
    """Lazily connected repository: the API starts and reports not-ready while Cassandra is down."""

    def __init__(
        self,
        settings: ApiSettings,
        connect: Callable[[ApiSettings], Session] = connect_cluster,
    ) -> None:
        self._settings = settings
        self._connect = connect
        self._lock = threading.Lock()
        self._session: Session | None = None
        self._statements: dict[str, Any] = {}

    def _queries(self) -> dict[str, str]:
        ks = self._settings.cassandra_keyspace
        positions = ", ".join(POSITION_COLUMNS)
        return {
            "ping": "SELECT release_version FROM system.local",
            "current_feed": (
                f"SELECT static_feed_version FROM {ks}.static_feed_current WHERE feed = ?"
            ),
            "vehicle_latest": (
                f"SELECT {positions} FROM {ks}.vehicle_latest_by_vehicle WHERE vehicle_id = ?"
            ),
            "vehicles_latest": f"SELECT {positions} FROM {ks}.vehicle_latest_by_vehicle",
            "route_vehicles_latest": (
                f"SELECT {positions} FROM {ks}.vehicles_latest_by_route WHERE route_id = ?"
            ),
            "vehicle_positions": (
                f"SELECT {positions} FROM {ks}.vehicle_positions_by_vehicle_day"
                " WHERE vehicle_id = ? AND event_date = ?"
                " AND source_timestamp >= ? AND source_timestamp < ?"
            ),
            "vehicle_eta": (
                f"SELECT {', '.join(ETA_COLUMNS)} FROM {ks}.eta_history_by_vehicle_day"
                " WHERE vehicle_id = ? AND collected_date = ?"
                " AND collected_at >= ? AND collected_at < ?"
            ),
            "routes": (
                f"SELECT {', '.join(ROUTE_COLUMNS)} FROM {ks}.routes_by_feed"
                " WHERE static_feed_version = ?"
            ),
            "route": (
                f"SELECT {', '.join(ROUTE_COLUMNS)} FROM {ks}.routes_by_feed"
                " WHERE static_feed_version = ? AND route_id = ?"
            ),
            "route_stops": (
                f"SELECT {', '.join(STOP_COLUMNS)} FROM {ks}.stops_by_route"
                " WHERE static_feed_version = ? AND route_id = ?"
            ),
        }

    def _ready_session(self) -> Session:
        with self._lock:
            if self._session is None:
                try:
                    session = self._connect(self._settings)
                    statements = {
                        name: session.prepare(query) for name, query in self._queries().items()
                    }
                except Exception as error:  # driver raises many unrelated exception types
                    logger.warning("Cassandra is not available", extra={"error": str(error)})
                    raise ServingUnavailable("Cassandra is not available") from error
                self._session, self._statements = session, statements
            return self._session

    def _execute(
        self, name: str, parameters: Sequence[Any], page: PageRequest | None = None
    ) -> Any:
        from cassandra import InvalidRequest

        session = self._ready_session()
        statement = self._statements[name]
        bound = statement.bind(list(parameters))
        bound.fetch_size = None if page is None else page.limit
        try:
            return session.execute(
                bound,
                paging_state=None if page is None else page.paging_state,
                timeout=self._settings.cassandra_request_timeout_seconds,
            )
        except InvalidRequest as error:
            if page is not None and page.paging_state is not None:
                raise InvalidPagingState(str(error)) from error
            raise ServingUnavailable(f"Cassandra rejected query {name}") from error
        except Exception as error:
            logger.warning("Cassandra query failed", extra={"error": f"{name}: {error}"})
            raise ServingUnavailable(f"Cassandra query {name} failed") from error

    def _slice[T](
        self, name: str, parameters: Sequence[Any], page: PageRequest, convert: Callable[[Any], T]
    ) -> Slice[T]:
        result = self._execute(name, parameters, page)
        return Slice([convert(row) for row in result.current_rows], result.paging_state)

    def ping(self) -> None:
        self._execute("ping", ())

    def current_feed_version(self) -> str | None:
        row = self._execute("current_feed", (FEED_NAME,)).one()
        return None if row is None else row.static_feed_version

    def vehicle_latest(self, vehicle_id: str) -> VehiclePosition | None:
        row = self._execute("vehicle_latest", (vehicle_id,)).one()
        return None if row is None else position_from_row(row)

    def vehicles_latest(self, page: PageRequest) -> Slice[VehiclePosition]:
        return self._slice("vehicles_latest", (), page, position_from_row)

    def route_vehicles_latest(self, route_id: str, page: PageRequest) -> Slice[VehiclePosition]:
        return self._slice("route_vehicles_latest", (route_id,), page, position_from_row)

    def vehicle_positions(
        self, vehicle_id: str, day: date, start: datetime, end: datetime, page: PageRequest
    ) -> Slice[VehiclePosition]:
        parameters = (vehicle_id, day, _naive_utc(start), _naive_utc(end))
        return self._slice("vehicle_positions", parameters, page, position_from_row)

    def vehicle_eta(
        self, vehicle_id: str, day: date, start: datetime, end: datetime, page: PageRequest
    ) -> Slice[EtaSnapshot]:
        parameters = (vehicle_id, day, _naive_utc(start), _naive_utc(end))
        return self._slice("vehicle_eta", parameters, page, eta_from_row)

    def routes(self, feed_version: str, page: PageRequest) -> Slice[Route]:
        return self._slice("routes", (feed_version,), page, route_from_row)

    def route(self, feed_version: str, route_id: str) -> Route | None:
        row = self._execute("route", (feed_version, route_id)).one()
        return None if row is None else route_from_row(row)

    def route_stops(self, feed_version: str, route_id: str) -> RouteStops:
        rows = list(self._execute("route_stops", (feed_version, route_id)))
        return route_stops_from_rows(feed_version, route_id, rows)

    def close(self) -> None:
        with self._lock:
            if self._session is not None:
                shutdown = getattr(getattr(self._session, "cluster", None), "shutdown", None)
                if callable(shutdown):
                    shutdown()
                self._session = None
