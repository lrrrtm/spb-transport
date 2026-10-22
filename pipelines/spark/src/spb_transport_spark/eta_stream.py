from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import (
    ArrayType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from spb_transport_spark.cassandra_sink import configure_cassandra, write_eta_history
from spb_transport_spark.config import EtaStreamSettings

FORECAST_SCHEMA = StructType(
    [
        StructField("forecast_rank", IntegerType()),
        StructField("stop_id", StringType()),
        StructField("arrival_time", TimestampType()),
    ]
)
ETA_SCHEMA = StructType(
    [
        StructField("schema_version", StringType()),
        StructField("event_id", StringType()),
        StructField("collected_at", TimestampType()),
        StructField("vehicle_id", StringType()),
        StructField("route_id", StringType()),
        StructField("forecasts", ArrayType(FORECAST_SCHEMA)),
    ]
)


def classify(source: DataFrame) -> DataFrame:
    parsed = source.select(
        F.col("value").cast("string").alias("raw_json"),
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_timestamp"),
        F.from_json(F.col("value").cast("string"), ETA_SCHEMA).alias("event"),
    ).select("raw_json", "topic", "partition", "offset", "kafka_timestamp", "event.*")

    missing_required = (
        F.col("event_id").isNull()
        | F.col("collected_at").isNull()
        | F.col("vehicle_id").isNull()
        | F.col("route_id").isNull()
        | (F.length(F.trim("vehicle_id")) == 0)
        | (F.length(F.trim("route_id")) == 0)
        | F.col("forecasts").isNull()
    )
    invalid_forecast = F.exists(
        "forecasts",
        lambda forecast: (
            forecast["forecast_rank"].isNull()
            | (forecast["forecast_rank"] < 0)
            | forecast["stop_id"].isNull()
            | (F.length(F.trim(forecast["stop_id"])) == 0)
            | forecast["arrival_time"].isNull()
        ),
    )
    actual_ranks = F.transform("forecasts", lambda forecast: forecast["forecast_rank"])
    expected_ranks = F.sequence(F.lit(0), F.size("forecasts") - 1)
    invalid_rank_order = (F.size("forecasts") > 0) & (actual_ranks != expected_ranks)
    reason = (
        F.when(F.col("schema_version").isNull(), "malformed_json_or_missing_schema")
        .when(F.col("schema_version") != "1", "unsupported_schema_version")
        .when(missing_required, "missing_required_field")
        .when(~F.col("event_id").rlike("^[a-f0-9]{64}$"), "invalid_event_id")
        .when(invalid_forecast, "invalid_forecast")
        .when(invalid_rank_order, "invalid_forecast_rank_order")
    )
    return (
        parsed.withColumn("quarantine_reason", reason)
        .withColumn("forecast_count", F.size("forecasts"))
        .withColumn(
            "eta_status",
            F.when(F.size("forecasts") == 0, "empty").otherwise("available"),
        )
        .withColumn("collected_date", F.to_date("collected_at"))
        .withColumn("collected_hour", F.date_format("collected_at", "HH"))
        .withColumn("ingest_date", F.to_date("kafka_timestamp"))
        .withColumn("ingest_hour", F.date_format("kafka_timestamp", "HH"))
    )


def start(settings: EtaStreamSettings) -> tuple[StreamingQuery, StreamingQuery, StreamingQuery]:
    spark = configure_cassandra(
        SparkSession.builder.appName("vehicle-eta-stream"), settings.cassandra
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    source = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", settings.kafka_bootstrap_servers)
        .option("subscribe", settings.kafka_topic)
        .option("startingOffsets", settings.starting_offsets)
        .load()
    )
    classified = classify(source)
    history = (
        classified.filter(F.col("quarantine_reason").isNull())
        .drop("raw_json", "quarantine_reason", "ingest_date", "ingest_hour")
        .withWatermark("collected_at", settings.watermark)
        .dropDuplicatesWithinWatermark(["event_id"])
    )
    invalid = classified.filter(F.col("quarantine_reason").isNotNull()).drop(
        "forecast_count", "eta_status", "collected_date", "collected_hour"
    )

    history_query = (
        history.writeStream.format("parquet")
        .option("path", settings.history_path)
        .option("checkpointLocation", f"{settings.checkpoint_path}/history")
        .partitionBy("collected_date", "collected_hour")
        .trigger(processingTime=settings.trigger_interval)
        .start()
    )
    quarantine_query = (
        invalid.writeStream.format("parquet")
        .option("path", settings.quarantine_path)
        .option("checkpointLocation", f"{settings.checkpoint_path}/quarantine")
        .partitionBy("quarantine_reason", "ingest_date", "ingest_hour")
        .trigger(processingTime=settings.trigger_interval)
        .start()
    )
    cassandra_query = (
        history.writeStream.foreachBatch(
            lambda frame, _batch_id: write_eta_history(frame, settings.cassandra)
        )
        .option("checkpointLocation", f"{settings.checkpoint_path}/cassandra")
        .trigger(processingTime=settings.trigger_interval)
        .start()
    )
    return history_query, quarantine_query, cassandra_query


def main() -> None:
    queries = start(EtaStreamSettings.from_env())
    spark = SparkSession.getActiveSession()
    if spark is None:
        raise RuntimeError("Spark session did not start")
    try:
        spark.streams.awaitAnyTermination()
    finally:
        for query in queries:
            query.stop()


if __name__ == "__main__":
    main()
