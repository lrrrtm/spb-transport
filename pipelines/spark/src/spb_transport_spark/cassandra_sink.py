from __future__ import annotations

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any

from spb_transport_spark.config import CassandraSettings

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

CASSANDRA_FORMAT = "org.apache.spark.sql.cassandra"
WRITETIME_COLUMN = "writetime_us"

_POSITION_COLUMNS = (
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
    "has_quality_flags",
)


@dataclass(frozen=True)
class ServingTable:
    """Columns written to one Cassandra serving table.

    `expiring` tables are operational latest views and receive the mandatory latest TTL.
    History tables never expire: their primary keys are deterministic, so replay is idempotent.
    """

    name: str
    columns: tuple[str, ...]
    expiring: bool


POSITION_TABLES = (
    ServingTable("vehicle_latest_by_vehicle", _POSITION_COLUMNS, expiring=True),
    ServingTable("vehicles_latest_by_route", _POSITION_COLUMNS, expiring=True),
    ServingTable(
        "vehicle_positions_by_vehicle_day", ("event_date", *_POSITION_COLUMNS), expiring=False
    ),
)

ETA_TABLES = (
    ServingTable(
        "eta_history_by_vehicle_day",
        (
            "vehicle_id",
            "collected_date",
            "collected_at",
            "event_id",
            "route_id",
            "forecast_count",
            "eta_status",
            "forecasts_json",
        ),
        expiring=False,
    ),
)


STATIC_TABLES = (
    ServingTable(
        "routes_by_feed",
        (
            "static_feed_version",
            "route_id",
            "agency_id",
            "route_short_name",
            "route_long_name",
            "route_type",
        ),
        expiring=False,
    ),
    ServingTable(
        "stops_by_route",
        (
            "static_feed_version",
            "route_id",
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
        ),
        expiring=False,
    ),
    # Written last: switching the pointer publishes a fully written feed version.
    ServingTable(
        "static_feed_current", ("feed", "static_feed_version", "published_at"), expiring=False
    ),
)

SEGMENT_METRICS_TABLE = ServingTable(
    "segment_metrics_by_route_bucket",
    (
        "static_feed_version",
        "route_id",
        "analysis_start",
        "analysis_end",
        "day_type",
        "hour",
        "shape_id",
        "direction_id",
        "pattern_id",
        "from_stop_id",
        "to_stop_id",
        "from_stop_index",
        "to_stop_index",
        "segment_length_m",
        "total_samples",
        "qualified_samples",
        "vehicle_count",
        "service_day_count",
        "coverage",
        "travel_time_p25_s",
        "travel_time_p50_s",
        "travel_time_p75_s",
        "travel_time_p90_s",
        "travel_time_p95_s",
        "travel_time_iqr_s",
        "travel_time_mad_s",
        "speed_p50_mps",
        "stop_delay_share",
        "peak_offpeak_ratio",
        "enough_data",
        "algorithm_version",
    ),
    expiring=False,
)

ANOMALIES_TABLE = ServingTable(
    "anomalies_by_route_day",
    (
        "algorithm_version",
        "event_date",
        "route_id",
        "started_at",
        "anomaly_id",
        "anomaly_type",
        "static_feed_version",
        "shape_id",
        "direction_id",
        "vehicle_id",
        "related_vehicle_id",
        "stop_id",
        "ended_at",
        "duration_s",
        "distance_gap_m",
        "headway_s",
        "latitude",
        "longitude",
    ),
    expiring=False,
)

ETA_COMPARISON_TABLE = ServingTable(
    "eta_comparison_by_route_day",
    (
        "algorithm_version",
        "evaluation_date",
        "training_start",
        "training_end",
        "route_id",
        "horizon_bucket",
        "labeled_forecasts",
        "paired_forecasts",
        "baseline_coverage",
        "official_mae_s",
        "official_median_absolute_error_s",
        "baseline_mae_s",
        "baseline_median_absolute_error_s",
    ),
    expiring=False,
)

ROUTE_FEATURES_TABLE = ServingTable(
    "route_features_by_period",
    (
        "model_id",
        "static_feed_version",
        "route_id",
        "period_start",
        "period_end",
        "qualified_samples",
        "speed_p50_mps",
        "travel_variability",
        "peak_offpeak_ratio",
        "stop_delay_share",
        "coverage",
        "bunching_per_day",
        "irregular_headways_per_day",
        "stationary_per_day",
        "algorithm_version",
    ),
    expiring=False,
)

ROUTE_CLUSTERS_TABLE = ServingTable(
    "route_cluster_by_model",
    (
        "model_id",
        "static_feed_version",
        "route_id",
        "cluster_id",
        "distance",
        "period_start",
        "period_end",
        "algorithm_version",
    ),
    expiring=False,
)

ANALYTICS_CURRENT_TABLE = ServingTable(
    "analytics_current",
    (
        "dataset",
        "algorithm_version",
        "model_id",
        "static_feed_version",
        "period_start",
        "period_end",
        "training_start",
        "training_end",
        "updated_at",
    ),
    expiring=False,
)


def configure_cassandra(builder: Any, settings: CassandraSettings) -> Any:
    builder = (
        builder.config("spark.cassandra.connection.host", settings.host)
        .config("spark.cassandra.connection.port", str(settings.port))
        .config("spark.cassandra.connection.localDC", settings.local_dc)
    )
    if settings.username is not None:
        builder = builder.config("spark.cassandra.auth.username", settings.username).config(
            "spark.cassandra.auth.password", settings.password
        )
    return builder


def _write(
    frame: DataFrame, settings: CassandraSettings, table: ServingTable, *, writetime: bool = True
) -> None:
    columns = (*table.columns, WRITETIME_COLUMN) if writetime else table.columns
    writer = (
        frame.select(*columns)
        .write.format(CASSANDRA_FORMAT)
        .option("keyspace", settings.keyspace)
        .option("table", table.name)
    )
    if writetime:
        writer = writer.option("writetime", WRITETIME_COLUMN)
    if table.expiring:
        writer = writer.option("ttl", str(settings.latest_ttl_seconds))
    writer.mode("append").save()


def _with_writetime(frame: DataFrame, event_time_column: str) -> DataFrame:
    from pyspark.sql import functions as F

    return frame.withColumn(
        WRITETIME_COLUMN, (F.col(event_time_column).cast("double") * 1_000_000).cast("long")
    )


def write_positions(frame: DataFrame, settings: CassandraSettings) -> None:
    # Source observation time is the Cassandra writetime: replaying an older event
    # cannot overwrite a newer latest state.
    rows = _with_writetime(frame, "source_timestamp")
    for table in POSITION_TABLES:
        _write(rows, settings, table)


def write_eta_history(frame: DataFrame, settings: CassandraSettings) -> None:
    from pyspark.sql import functions as F

    rows = _with_writetime(
        frame.withColumn("forecasts_json", F.to_json("forecasts")), "collected_at"
    )
    for table in ETA_TABLES:
        _write(rows, settings, table)


def write_static_table(frame: DataFrame, settings: CassandraSettings, table: ServingTable) -> None:
    """Static metadata keyed by feed version: rewriting the same version is idempotent."""
    _write(frame, settings, table, writetime=False)


def write_segment_metrics(frame: DataFrame, settings: CassandraSettings) -> None:
    """Publish deterministic aggregates; the table primary key makes replay idempotent."""
    _write(frame, settings, SEGMENT_METRICS_TABLE, writetime=False)


def write_anomalies(frame: DataFrame, settings: CassandraSettings) -> None:
    """Publish deterministic anomaly events for route/day queries."""
    _write(frame, settings, ANOMALIES_TABLE, writetime=False)


def write_eta_comparison(frame: DataFrame, settings: CassandraSettings) -> None:
    """Publish route/day ETA evaluation aggregates."""
    _write(frame, settings, ETA_COMPARISON_TABLE, writetime=False)


def write_route_clusters(
    features: DataFrame, assignments: DataFrame, settings: CassandraSettings
) -> None:
    """Publish route features and their assignment under one deterministic model id."""
    _write(features, settings, ROUTE_FEATURES_TABLE, writetime=False)
    _write(assignments, settings, ROUTE_CLUSTERS_TABLE, writetime=False)


def write_analytics_current(frame: DataFrame, settings: CassandraSettings) -> None:
    """Promote a successfully published analytical dataset for read-side discovery."""
    _write(frame, settings, ANALYTICS_CURRENT_TABLE, writetime=False)
