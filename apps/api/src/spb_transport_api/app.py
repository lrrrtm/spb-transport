"""FastAPI application: typed read-only endpoints over prepared serving tables."""

from __future__ import annotations

import logging
from collections.abc import AsyncIterator, Callable
from contextlib import asynccontextmanager
from datetime import UTC, date, datetime, timedelta
from typing import Annotated

from fastapi import FastAPI, HTTPException, Path, Query, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse

from spb_transport_api.config import ApiSettings
from spb_transport_api.cursor import Cursor, InvalidCursor, decode_cursor, encode_cursor
from spb_transport_api.models import (
    EtaPage,
    Health,
    Readiness,
    Route,
    RoutePage,
    RouteStops,
    VehiclePage,
    VehiclePosition,
)
from spb_transport_api.paging import PageRequest, read_days
from spb_transport_api.repository import (
    CassandraRepository,
    InvalidPagingState,
    ServingRepository,
    ServingUnavailable,
)

logger = logging.getLogger(__name__)

API_PREFIX = "/api/v1"
Identifier = Annotated[str, Path(min_length=1, max_length=128)]
CursorQuery = Annotated[str | None, Query(max_length=4096, description="next_cursor of a page")]
FromQuery = Annotated[
    datetime | None, Query(alias="from", description="ISO 8601 with timezone; default to - window")
]
ToQuery = Annotated[
    datetime | None, Query(alias="to", description="ISO 8601 with timezone; default now")
]


def create_app(
    settings: ApiSettings,
    repository_factory: Callable[[ApiSettings], ServingRepository] = CassandraRepository,
    clock: Callable[[], datetime] = lambda: datetime.now(UTC),
) -> FastAPI:
    repository: ServingRepository = repository_factory(settings)

    @asynccontextmanager
    async def lifespan(_: FastAPI) -> AsyncIterator[None]:
        try:
            yield
        finally:
            repository.close()

    app = FastAPI(
        title="SPb transport API",
        version="0.1.0",
        description=(
            "Read-only API over prepared Cassandra serving tables. Realtime observations are not "
            "linked to scheduled trips: trip_id and stop_sequence are never exposed."
        ),
        lifespan=lifespan,
    )
    if settings.cors_origin_list:
        app.add_middleware(
            CORSMiddleware,
            allow_origins=settings.cors_origin_list,
            allow_methods=["GET"],
            allow_headers=["*"],
        )

    @app.exception_handler(ServingUnavailable)
    async def unavailable(_: Request, error: ServingUnavailable) -> JSONResponse:
        return JSONResponse(status_code=503, content={"detail": str(error)})

    @app.exception_handler(InvalidCursor)
    async def invalid_cursor(_: Request, error: InvalidCursor) -> JSONResponse:
        return JSONResponse(status_code=400, content={"detail": f"invalid cursor: {error}"})

    @app.exception_handler(InvalidPagingState)
    async def invalid_state(_: Request, __: InvalidPagingState) -> JSONResponse:
        return JSONResponse(
            status_code=400, content={"detail": "invalid cursor: stale paging state"}
        )

    def page_size(limit: int | None) -> int:
        if limit is None:
            return settings.default_page_size
        if not 1 <= limit <= settings.max_page_size:
            raise HTTPException(422, f"limit must be between 1 and {settings.max_page_size}")
        return limit

    def fresh(positions: list[VehiclePosition]) -> list[VehiclePosition]:
        cutoff = clock() - timedelta(seconds=settings.vehicle_freshness_seconds)
        return [p for p in positions if p.source_timestamp >= cutoff]

    def time_range(start: datetime | None, end: datetime | None) -> tuple[datetime, datetime]:
        for value in (start, end):
            if value is not None and value.tzinfo is None:
                raise HTTPException(422, "from and to must include a timezone")
        end = (end or clock()).astimezone(UTC)
        start = (
            start or end - timedelta(minutes=settings.history_default_range_minutes)
        ).astimezone(UTC)
        if start >= end:
            raise HTTPException(422, "from must be earlier than to")
        if end - start > timedelta(hours=settings.history_max_range_hours):
            raise HTTPException(
                422, f"range must not exceed {settings.history_max_range_hours} hours"
            )
        return start, end

    def days_desc(start: datetime, end: datetime) -> list[date]:
        last = (end - timedelta(microseconds=1)).date()
        return [last - timedelta(days=i) for i in range((last - start.date()).days + 1)]

    def current_feed() -> str:
        version = repository.current_feed_version()
        if version is None:
            raise HTTPException(503, "static GTFS metadata has not been published yet")
        return version

    @app.get("/health", response_model=Health, tags=["service"])
    def health() -> Health:
        """Liveness: the process answers HTTP. Does not touch Cassandra."""
        return Health(status="ok")

    @app.get(
        "/ready",
        response_model=Readiness,
        responses={503: {"model": Readiness}},
        tags=["service"],
    )
    def ready() -> Readiness | JSONResponse:
        """Readiness: Cassandra answers and the serving schema is readable."""
        try:
            repository.ping()
            version = repository.current_feed_version()
        except ServingUnavailable as error:
            body = Readiness(
                status="unavailable",
                cassandra="unavailable",
                static_feed_version=None,
                detail=str(error),
            )
            return JSONResponse(status_code=503, content=body.model_dump())
        return Readiness(status="ready", cassandra="ok", static_feed_version=version)

    @app.get(f"{API_PREFIX}/vehicles", response_model=VehiclePage, tags=["vehicles"])
    def vehicles(
        limit: Annotated[int | None, Query()] = None, cursor: CursorQuery = None
    ) -> VehiclePage:
        """Latest position of every vehicle seen within the freshness window.

        Stale vehicles are filtered out, so a page may hold fewer than `limit` items while
        `next_cursor` is still set.
        """
        scope = "vehicles"
        state = decode_cursor(cursor, scope)
        page = repository.vehicles_latest(
            PageRequest(page_size(limit), None if state is None else state.paging_state)
        )
        return VehiclePage(items=fresh(page.items), next_cursor=_next(scope, page.paging_state))

    @app.get(
        f"{API_PREFIX}/vehicles/{{vehicle_id}}",
        response_model=VehiclePosition,
        responses={404: {"description": "No fresh position"}},
        tags=["vehicles"],
    )
    def vehicle(vehicle_id: Identifier) -> VehiclePosition:
        position = repository.vehicle_latest(vehicle_id)
        if position is None or not fresh([position]):
            raise HTTPException(404, "no fresh position for this vehicle")
        return position

    @app.get(
        f"{API_PREFIX}/vehicles/{{vehicle_id}}/positions",
        response_model=VehiclePage,
        tags=["vehicles"],
    )
    def vehicle_positions(
        vehicle_id: Identifier,
        start: FromQuery = None,
        end: ToQuery = None,
        limit: Annotated[int | None, Query()] = None,
        cursor: CursorQuery = None,
    ) -> VehiclePage:
        """Position history of one vehicle in `[from, to)`, newest first."""
        start, end = time_range(start, end)
        scope = f"positions|{vehicle_id}|{start.isoformat()}|{end.isoformat()}"
        state = decode_cursor(cursor, scope)
        try:
            page = read_days(
                lambda day, request: repository.vehicle_positions(
                    vehicle_id, day, start, end, request
                ),
                days_desc(start, end),
                page_size(limit),
                None if state is None else state.day,
                None if state is None else state.paging_state,
            )
        except ValueError as error:
            raise InvalidCursor(str(error)) from error
        return VehiclePage(
            items=page.items, next_cursor=_next(scope, page.next_paging_state, page.next_day)
        )

    @app.get(f"{API_PREFIX}/vehicles/{{vehicle_id}}/eta", response_model=EtaPage, tags=["eta"])
    def vehicle_eta(
        vehicle_id: Identifier,
        start: FromQuery = None,
        end: ToQuery = None,
        limit: Annotated[int | None, Query()] = None,
        cursor: CursorQuery = None,
    ) -> EtaPage:
        """Official ETA snapshots of one vehicle collected in `[from, to)`, newest first."""
        start, end = time_range(start, end)
        scope = f"eta|{vehicle_id}|{start.isoformat()}|{end.isoformat()}"
        state = decode_cursor(cursor, scope)
        try:
            page = read_days(
                lambda day, request: repository.vehicle_eta(vehicle_id, day, start, end, request),
                days_desc(start, end),
                page_size(limit),
                None if state is None else state.day,
                None if state is None else state.paging_state,
            )
        except ValueError as error:
            raise InvalidCursor(str(error)) from error
        return EtaPage(
            items=page.items, next_cursor=_next(scope, page.next_paging_state, page.next_day)
        )

    @app.get(f"{API_PREFIX}/routes", response_model=RoutePage, tags=["routes"])
    def routes(
        limit: Annotated[int | None, Query()] = None, cursor: CursorQuery = None
    ) -> RoutePage:
        """Routes of the currently published static GTFS version, ordered by route_id."""
        version = current_feed()
        scope = f"routes|{version}"
        state = decode_cursor(cursor, scope)
        page = repository.routes(
            version, PageRequest(page_size(limit), None if state is None else state.paging_state)
        )
        return RoutePage(
            items=page.items,
            static_feed_version=version,
            next_cursor=_next(scope, page.paging_state),
        )

    @app.get(f"{API_PREFIX}/routes/{{route_id}}", response_model=Route, tags=["routes"])
    def route(route_id: Identifier) -> Route:
        found = repository.route(current_feed(), route_id)
        if found is None:
            raise HTTPException(404, "route not found in the current static GTFS version")
        return found

    @app.get(f"{API_PREFIX}/routes/{{route_id}}/stops", response_model=RouteStops, tags=["routes"])
    def route_stops(route_id: Identifier) -> RouteStops:
        """Stops of every route variant (shape) in travel order."""
        version = current_feed()
        if repository.route(version, route_id) is None:
            raise HTTPException(404, "route not found in the current static GTFS version")
        return repository.route_stops(version, route_id)

    @app.get(
        f"{API_PREFIX}/routes/{{route_id}}/vehicles", response_model=VehiclePage, tags=["routes"]
    )
    def route_vehicles(
        route_id: Identifier,
        limit: Annotated[int | None, Query()] = None,
        cursor: CursorQuery = None,
    ) -> VehiclePage:
        """Vehicles currently on the route (latest positions within the freshness window)."""
        scope = f"route-vehicles|{route_id}"
        state = decode_cursor(cursor, scope)
        page = repository.route_vehicles_latest(
            route_id, PageRequest(page_size(limit), None if state is None else state.paging_state)
        )
        return VehiclePage(items=fresh(page.items), next_cursor=_next(scope, page.paging_state))

    return app


def _next(scope: str, paging_state: bytes | None, day: date | None = None) -> str | None:
    if paging_state is None and day is None:
        return None
    return encode_cursor(Cursor(scope=scope, paging_state=paging_state, day=day))
