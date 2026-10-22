from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from spb_transport_spark.config import CassandraSettings

CASSANDRA_FORMAT = "org.apache.spark.sql.cassandra"


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
    frame: DataFrame, settings: CassandraSettings, table: str, *, expiring: bool = False
) -> None:
    writer = (
        frame.write.format(CASSANDRA_FORMAT)
        .option("keyspace", settings.keyspace)
        .option("table", table)
        .option("writetime", "writetime_us")
    )
    if expiring:
        writer = writer.option("ttl", str(settings.latest_ttl_seconds))
    writer.mode("append").save()


def write_positions(frame: DataFrame, settings: CassandraSettings) -> None:
    columns = [
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
        "writetime_us",
    ]
    rows = frame.withColumn(
        "writetime_us", (F.col("source_timestamp").cast("double") * 1_000_000).cast("long")
    )
    _write(rows.select(*columns), settings, "vehicle_latest_by_vehicle", expiring=True)
    _write(rows.select(*columns), settings, "vehicles_latest_by_route", expiring=True)
    _write(rows.select("event_date", *columns), settings, "vehicle_positions_by_vehicle_day")


def write_eta_history(frame: DataFrame, settings: CassandraSettings) -> None:
    rows = (
        frame.withColumn("forecasts_json", F.to_json("forecasts"))
        .withColumn("writetime_us", (F.col("collected_at").cast("double") * 1_000_000).cast("long"))
        .select(
            "vehicle_id",
            "collected_date",
            "collected_at",
            "event_id",
            "route_id",
            "forecast_count",
            "eta_status",
            "forecasts_json",
            "writetime_us",
        )
    )
    _write(rows, settings, "eta_history_by_vehicle_day")
