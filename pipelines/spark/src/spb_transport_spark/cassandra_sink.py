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


def _write(frame: DataFrame, settings: CassandraSettings, table: ServingTable) -> None:
    writer = (
        frame.select(*table.columns, WRITETIME_COLUMN)
        .write.format(CASSANDRA_FORMAT)
        .option("keyspace", settings.keyspace)
        .option("table", table.name)
        .option("writetime", WRITETIME_COLUMN)
    )
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
