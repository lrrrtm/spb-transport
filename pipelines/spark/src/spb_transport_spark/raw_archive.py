from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import IntegerType, StringType, StructField, StructType, TimestampType

from spb_transport_spark.config import RawArchiveSettings

RAW_SCHEMA = StructType(
    [
        StructField("schema_version", StringType()),
        StructField("request_id", StringType()),
        StructField("endpoint", StringType()),
        StructField("requested_at", TimestampType()),
        StructField("collected_at", TimestampType()),
        StructField("http_status", IntegerType()),
        StructField("content_type", StringType()),
        StructField("payload_base64", StringType()),
        StructField("sha256", StringType()),
        StructField("decode_error", StringType()),
    ]
)


def classify(raw_kafka: DataFrame) -> DataFrame:
    parsed = raw_kafka.select(
        F.col("value").cast("string").alias("raw_json"),
        "topic",
        "partition",
        "offset",
        F.col("timestamp").alias("kafka_timestamp"),
        F.from_json(F.col("value").cast("string"), RAW_SCHEMA).alias("event"),
    ).select("raw_json", "topic", "partition", "offset", "kafka_timestamp", "event.*")

    decoded = parsed.withColumn(
        "payload", F.expr("try_to_binary(payload_base64, 'base64')")
    ).withColumn("actual_sha256", F.sha2("payload", 256))
    required_missing = (
        F.col("request_id").isNull()
        | F.col("requested_at").isNull()
        | F.col("collected_at").isNull()
        | F.col("payload_base64").isNull()
        | F.col("sha256").isNull()
    )
    reason = (
        F.when(F.col("schema_version").isNull(), "malformed_json_or_missing_schema")
        .when(F.col("schema_version") != "1", "unsupported_schema_version")
        .when(~F.col("endpoint").isin("vehicle", "vehicletrips"), "invalid_endpoint")
        .when(required_missing, "missing_required_field")
        .when(F.col("collected_at") < F.col("requested_at"), "invalid_time_order")
        .when(F.col("payload").isNull(), "invalid_base64")
        .when(F.col("actual_sha256") != F.col("sha256"), "checksum_mismatch")
        .when(F.col("decode_error").isNotNull(), "protobuf_decode_error")
    )
    return (
        decoded.withColumn("quarantine_reason", reason)
        .withColumn("ingest_date", F.to_date("kafka_timestamp"))
        .withColumn("ingest_hour", F.date_format("kafka_timestamp", "HH"))
    )


def start(settings: RawArchiveSettings) -> tuple[StreamingQuery, StreamingQuery]:
    spark = SparkSession.builder.appName("raw-response-archive").getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    source = spark.readStream.format("kafka").options(**settings.kafka.reader_options()).load()
    classified = classify(source)
    valid = classified.filter(F.col("quarantine_reason").isNull()).drop(
        "raw_json", "payload_base64", "actual_sha256", "quarantine_reason"
    )
    invalid = classified.filter(F.col("quarantine_reason").isNotNull()).drop(
        "payload", "actual_sha256"
    )

    raw_query = (
        valid.writeStream.format("parquet")
        .option("path", settings.raw_path)
        .option("checkpointLocation", f"{settings.checkpoint_path}/raw")
        .partitionBy("endpoint", "ingest_date", "ingest_hour")
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
    return raw_query, quarantine_query


def main() -> None:
    queries = start(RawArchiveSettings.from_env())
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
