from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import DoubleType, StringType, StructField, StructType, TimestampType

from spb_transport_spark.cassandra_sink import configure_cassandra, write_positions
from spb_transport_spark.config import PositionStreamSettings

POSITION_SCHEMA = StructType(
    [
        StructField("schema_version", StringType()),
        StructField("event_id", StringType()),
        StructField("collected_at", TimestampType()),
        StructField("source_timestamp", TimestampType()),
        StructField("vehicle_id", StringType()),
        StructField("route_id", StringType()),
        StructField("latitude", DoubleType()),
        StructField("longitude", DoubleType()),
        StructField("bearing", DoubleType()),
        StructField("speed", DoubleType()),
        StructField("label", StringType()),
        StructField("license_plate", StringType()),
    ]
)


def classify(source: DataFrame, settings: PositionStreamSettings) -> DataFrame:
    parsed = source.select(
        F.col("value").cast("string").alias("raw_json"),
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_timestamp"),
        F.from_json(F.col("value").cast("string"), POSITION_SCHEMA).alias("event"),
    ).select("raw_json", "topic", "partition", "offset", "kafka_timestamp", "event.*")

    missing_required = (
        F.col("event_id").isNull()
        | F.col("collected_at").isNull()
        | F.col("source_timestamp").isNull()
        | F.col("vehicle_id").isNull()
        | F.col("route_id").isNull()
        | (F.length(F.trim("vehicle_id")) == 0)
        | (F.length(F.trim("route_id")) == 0)
        | F.col("latitude").isNull()
        | F.col("longitude").isNull()
    )
    invalid_coordinates = (
        F.isnan("latitude")
        | F.isnan("longitude")
        | ~F.col("latitude").between(-90.0, 90.0)
        | ~F.col("longitude").between(-180.0, 180.0)
    )
    invalid_motion = (
        F.col("bearing").isNotNull()
        & (F.isnan("bearing") | (~F.col("bearing").between(0.0, 359.999999)))
    ) | (F.col("speed").isNotNull() & (F.isnan("speed") | (F.col("speed") < 0.0)))
    reason = (
        F.when(F.col("schema_version").isNull(), "malformed_json_or_missing_schema")
        .when(F.col("schema_version") != "1", "unsupported_schema_version")
        .when(missing_required, "missing_required_field")
        .when(~F.col("event_id").rlike("^[a-f0-9]{64}$"), "invalid_event_id")
        .when(invalid_coordinates, "invalid_coordinates")
        .when(invalid_motion, "invalid_motion")
    )
    age_seconds = F.unix_timestamp("collected_at") - F.unix_timestamp("source_timestamp")
    quality_flags = F.array_compact(
        F.array(
            F.when(age_seconds > settings.max_source_age_seconds, "stale_source_timestamp"),
            F.when(age_seconds < -settings.max_future_skew_seconds, "future_source_timestamp"),
            F.when(F.col("speed").isNull(), "speed_missing"),
            F.when(F.col("bearing").isNull(), "bearing_missing"),
            F.when(F.col("speed") == 0.0, "stationary_observation"),
        )
    )
    return (
        parsed.withColumn("quarantine_reason", reason)
        .withColumn("quality_flags", quality_flags)
        .withColumn("has_quality_flags", F.size("quality_flags") > 0)
        .withColumn("event_date", F.to_date("source_timestamp"))
        .withColumn("event_hour", F.date_format("source_timestamp", "HH"))
        .withColumn("ingest_date", F.to_date("kafka_timestamp"))
        .withColumn("ingest_hour", F.date_format("kafka_timestamp", "HH"))
    )


def start(
    settings: PositionStreamSettings,
) -> tuple[StreamingQuery, StreamingQuery, StreamingQuery]:
    spark = configure_cassandra(
        SparkSession.builder.appName("vehicle-position-stream"), settings.cassandra
    ).getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    source = spark.readStream.format("kafka").options(**settings.kafka.reader_options()).load()
    classified = classify(source, settings)
    valid = (
        classified.filter(F.col("quarantine_reason").isNull())
        .drop("raw_json", "quarantine_reason", "ingest_date", "ingest_hour")
        .withWatermark("source_timestamp", settings.watermark)
        .dropDuplicatesWithinWatermark(["vehicle_id", "source_timestamp"])
    )
    invalid = classified.filter(F.col("quarantine_reason").isNotNull()).drop(
        "quality_flags", "has_quality_flags", "event_date", "event_hour"
    )

    processed_query = (
        valid.writeStream.format("parquet")
        .option("path", settings.processed_path)
        .option("checkpointLocation", f"{settings.checkpoint_path}/processed")
        .partitionBy("event_date", "event_hour")
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
        valid.writeStream.foreachBatch(
            lambda frame, _batch_id: write_positions(frame, settings.cassandra)
        )
        .option("checkpointLocation", f"{settings.checkpoint_path}/cassandra")
        .trigger(processingTime=settings.trigger_interval)
        .start()
    )
    return processed_query, quarantine_query, cassandra_query


def main() -> None:
    queries = start(PositionStreamSettings.from_env())
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
