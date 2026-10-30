"""Response models: the stable HTTP contract of the API (OpenAPI is generated from them)."""

from datetime import date, datetime

from pydantic import BaseModel, ConfigDict, Field


class ApiModel(BaseModel):
    model_config = ConfigDict(frozen=True)


class VehiclePosition(ApiModel):
    vehicle_id: str
    route_id: str | None
    event_id: str | None
    source_timestamp: datetime
    collected_at: datetime | None
    latitude: float
    longitude: float
    bearing: float | None
    speed: float | None = Field(description="Source speed, m/s; 0 is a valid value")
    label: str | None
    license_plate: str | None
    quality_flags: list[str]


class StopForecast(ApiModel):
    forecast_rank: int = Field(description="Order in the source response, not GTFS stop_sequence")
    stop_id: str
    arrival_time: datetime


class EtaSnapshot(ApiModel):
    vehicle_id: str
    route_id: str | None
    event_id: str
    collected_at: datetime
    eta_status: str = Field(description="available or empty (the source returned no forecasts)")
    forecasts: list[StopForecast]


class Route(ApiModel):
    route_id: str
    agency_id: str | None
    short_name: str | None
    long_name: str | None
    route_type: int | None


class RouteStop(ApiModel):
    stop_index: int = Field(description="Position in the stop pattern, not GTFS stop_sequence")
    stop_id: str
    stop_code: str | None
    stop_name: str | None
    latitude: float
    longitude: float
    distance_along_m: float | None = Field(
        description="Distance along the shape; null when the stop is too far from the shape"
    )


class RouteVariant(ApiModel):
    shape_id: str
    direction_id: int | None
    pattern_id: str | None
    stops: list[RouteStop]


class RouteStops(ApiModel):
    route_id: str
    static_feed_version: str
    variants: list[RouteVariant]


class VehiclePage(ApiModel):
    items: list[VehiclePosition]
    next_cursor: str | None


class EtaPage(ApiModel):
    items: list[EtaSnapshot]
    next_cursor: str | None


class RoutePage(ApiModel):
    items: list[Route]
    static_feed_version: str
    next_cursor: str | None


class Health(ApiModel):
    status: str


class Readiness(ApiModel):
    status: str
    cassandra: str
    static_feed_version: str | None
    detail: str | None = None


class AnalyticsDataset(ApiModel):
    dataset: str
    algorithm_version: str
    model_id: str | None
    static_feed_version: str | None
    period_start: date
    period_end: date
    training_start: date | None
    training_end: date | None
    updated_at: datetime


class AnalyticsOverview(ApiModel):
    datasets: list[AnalyticsDataset]


class SegmentMetric(ApiModel):
    shape_id: str
    direction_id: int
    from_stop_id: str
    to_stop_id: str
    from_stop_index: int
    segment_length_m: float
    qualified_samples: int
    coverage: float
    travel_time_p50_s: float
    travel_time_p90_s: float
    speed_p50_mps: float
    stop_delay_share: float
    peak_offpeak_ratio: float
    enough_data: bool
    from_stop_name: str | None = None
    to_stop_name: str | None = None
    from_latitude: float | None = None
    from_longitude: float | None = None
    to_latitude: float | None = None
    to_longitude: float | None = None


class TransportAnomaly(ApiModel):
    started_at: datetime
    anomaly_id: str
    anomaly_type: str
    vehicle_id: str | None
    related_vehicle_id: str | None
    stop_id: str | None
    ended_at: datetime | None
    duration_s: float | None
    distance_gap_m: float | None
    headway_s: float | None


class EtaComparison(ApiModel):
    horizon_bucket: str
    labeled_forecasts: int
    paired_forecasts: int
    baseline_coverage: float
    official_mae_s: float | None
    baseline_mae_s: float | None


class RouteCluster(ApiModel):
    route_id: str
    cluster_id: int
    distance: float
    qualified_samples: int
    speed_p50_mps: float
    travel_variability: float
    peak_offpeak_ratio: float
    stop_delay_share: float
    coverage: float
    bunching_per_day: float
    irregular_headways_per_day: float
    stationary_per_day: float
