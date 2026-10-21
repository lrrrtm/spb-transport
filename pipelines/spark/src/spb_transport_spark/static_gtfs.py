from __future__ import annotations

import csv
import hashlib
import logging
import ssl
import time
import zipfile
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory
from typing import TYPE_CHECKING, Any
from uuid import uuid4

import httpx

if TYPE_CHECKING:
    from pyspark.sql import Column, DataFrame, SparkSession

from spb_transport_spark.config import required

logger = logging.getLogger(__name__)

REQUIRED_COLUMNS = {
    "agency.txt": {"agency_id", "agency_name", "agency_url", "agency_timezone"},
    "calendar.txt": {"service_id", "start_date", "end_date"},
    "calendar_dates.txt": {"service_id", "date", "exception_type"},
    "routes.txt": {"route_id", "route_short_name", "route_type"},
    "shapes.txt": {"shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"},
    "stop_times.txt": {"trip_id", "stop_id", "stop_sequence"},
    "stops.txt": {"stop_id", "stop_name", "stop_lat", "stop_lon"},
    "trips.txt": {"route_id", "service_id", "trip_id", "shape_id"},
}

CSV_READ_OPTIONS = {
    "header": "true",
    "encoding": "UTF-8",
    "quote": '"',
    "escape": '"',
    "multiLine": "true",
    "mode": "FAILFAST",
}


@dataclass(frozen=True)
class StaticGtfsSettings:
    url: str
    tls_verify: bool
    ca_bundle: Path | None
    timeout_seconds: float
    retry_max_attempts: int
    retry_backoff_seconds: float
    max_download_bytes: int
    max_uncompressed_bytes: int
    raw_path: str
    processed_path: str
    staging_path: str

    @classmethod
    def from_env(cls) -> StaticGtfsSettings:
        import os

        tls_value = required("GTFS_TLS_VERIFY").lower()
        if tls_value not in {"true", "false"}:
            raise ValueError("GTFS_TLS_VERIFY must be true or false")
        ca_value = os.environ.get("GTFS_CA_BUNDLE", "").strip()
        ca_bundle = Path(ca_value) if ca_value else None
        if ca_bundle is not None and not ca_bundle.is_file():
            raise ValueError(f"GTFS_CA_BUNDLE is not a readable file: {ca_bundle}")
        if tls_value == "false" and ca_bundle is not None:
            raise ValueError("GTFS_CA_BUNDLE cannot be used when GTFS_TLS_VERIFY=false")

        return cls(
            url=required("GTFS_STATIC_URL"),
            tls_verify=tls_value == "true",
            ca_bundle=ca_bundle,
            timeout_seconds=_positive_float("GTFS_STATIC_TIMEOUT_SECONDS"),
            retry_max_attempts=_positive_int("GTFS_STATIC_RETRY_MAX_ATTEMPTS"),
            retry_backoff_seconds=_positive_float("GTFS_STATIC_RETRY_BACKOFF_SECONDS"),
            max_download_bytes=_positive_int("GTFS_STATIC_MAX_DOWNLOAD_BYTES"),
            max_uncompressed_bytes=_positive_int("GTFS_STATIC_MAX_UNCOMPRESSED_BYTES"),
            raw_path=required("HDFS_STATIC_RAW_PATH").rstrip("/"),
            processed_path=required("HDFS_STATIC_PROCESSED_PATH").rstrip("/"),
            staging_path=required("HDFS_STATIC_STAGING_PATH").rstrip("/"),
        )


def _positive_int(name: str) -> int:
    value = int(required(name))
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def _positive_float(name: str) -> float:
    value = float(required(name))
    if value <= 0:
        raise ValueError(f"{name} must be greater than zero")
    return value


def download(settings: StaticGtfsSettings, destination: Path) -> str:
    verify: bool | ssl.SSLContext
    if not settings.tls_verify:
        logger.warning("GTFS TLS certificate verification is disabled")
        verify = False
    elif settings.ca_bundle is not None:
        verify = ssl.create_default_context(cafile=str(settings.ca_bundle))
    else:
        verify = True

    with httpx.Client(verify=verify, timeout=settings.timeout_seconds) as client:
        for attempt in range(1, settings.retry_max_attempts + 1):
            try:
                digest = hashlib.sha256()
                size = 0
                with client.stream("GET", settings.url) as response:
                    response.raise_for_status()
                    with destination.open("wb") as output:
                        for chunk in response.iter_bytes():
                            size += len(chunk)
                            if size > settings.max_download_bytes:
                                raise ValueError(
                                    "static GTFS download exceeds configured size limit"
                                )
                            digest.update(chunk)
                            output.write(chunk)
                return digest.hexdigest()
            except httpx.HTTPStatusError as error:
                if error.response.status_code < 500 or attempt == settings.retry_max_attempts:
                    raise
                time.sleep(settings.retry_backoff_seconds * 2 ** (attempt - 1))
            except httpx.TransportError:
                if attempt == settings.retry_max_attempts:
                    raise
                time.sleep(settings.retry_backoff_seconds * 2 ** (attempt - 1))
    raise RuntimeError("unreachable retry state")


def validate_archive(path: Path, max_uncompressed_bytes: int) -> None:
    with zipfile.ZipFile(path) as archive:
        names = [info.filename for info in archive.infolist()]
        if len(names) != len(set(names)):
            raise ValueError("static GTFS ZIP contains duplicate filenames")
        if any(Path(name).name != name for name in names):
            raise ValueError("static GTFS ZIP must contain files only at archive root")
        if sum(info.file_size for info in archive.infolist()) > max_uncompressed_bytes:
            raise ValueError("static GTFS ZIP exceeds configured uncompressed size limit")
        missing_files = REQUIRED_COLUMNS.keys() - set(names)
        if missing_files:
            raise ValueError(f"static GTFS ZIP misses required files: {sorted(missing_files)}")
        for filename, required_columns in REQUIRED_COLUMNS.items():
            with archive.open(filename) as source:
                header = next(csv.reader([source.readline().decode("utf-8-sig")]))
            missing_columns = required_columns - set(header)
            if missing_columns:
                raise ValueError(f"{filename} misses required columns: {sorted(missing_columns)}")


def _clean(dataset: str, frame: DataFrame) -> DataFrame:
    from pyspark.sql import functions as F

    if dataset == "routes":
        return frame.select(
            "route_id",
            _optional(frame, "agency_id"),
            "route_short_name",
            _optional(frame, "route_long_name"),
            F.col("route_type").cast("int").alias("route_type"),
        ).filter(F.col("route_id").isNotNull() & F.col("route_type").isNotNull())
    if dataset == "stops":
        return frame.select(
            "stop_id",
            _optional(frame, "stop_code"),
            "stop_name",
            F.col("stop_lat").cast("double").alias("stop_lat"),
            F.col("stop_lon").cast("double").alias("stop_lon"),
        ).filter(
            F.col("stop_id").isNotNull()
            & F.col("stop_lat").between(-90, 90)
            & F.col("stop_lon").between(-180, 180)
        )
    if dataset == "trips":
        return frame.select(
            "trip_id",
            "route_id",
            "service_id",
            _optional(frame, "direction_id", "int"),
            _optional(frame, "shape_id"),
        ).filter(F.col("trip_id").isNotNull() & F.col("route_id").isNotNull())
    return frame.select(
        "shape_id",
        F.col("shape_pt_lat").cast("double").alias("shape_pt_lat"),
        F.col("shape_pt_lon").cast("double").alias("shape_pt_lon"),
        F.col("shape_pt_sequence").cast("int").alias("shape_pt_sequence"),
        _optional(frame, "shape_dist_traveled", "double"),
    ).filter(
        F.col("shape_id").isNotNull()
        & F.col("shape_pt_lat").between(-90, 90)
        & F.col("shape_pt_lon").between(-180, 180)
        & F.col("shape_pt_sequence").isNotNull()
    )


def _optional(frame: DataFrame, name: str, data_type: str = "string") -> Column:
    from pyspark.sql import functions as F

    if name in frame.columns:
        return F.col(name).cast(data_type).alias(name)
    return F.lit(None).cast(data_type).alias(name)


def _copy_to_hdfs(spark: SparkSession, local: Path, target: str, overwrite: bool) -> None:
    jvm: Any = spark._jvm
    context: Any = spark._jsc
    destination = jvm.org.apache.hadoop.fs.Path(target)
    filesystem = destination.getFileSystem(context.hadoopConfiguration())
    if filesystem.exists(destination) and not overwrite:
        return
    filesystem.mkdirs(destination.getParent())
    source = jvm.org.apache.hadoop.fs.Path(str(local))
    if overwrite:
        filesystem.copyFromLocalFile(False, True, source, destination)
        return

    temporary = jvm.org.apache.hadoop.fs.Path(f"{target}.tmp-{uuid4()}")
    try:
        filesystem.copyFromLocalFile(False, False, source, temporary)
        if not filesystem.rename(temporary, destination) and not filesystem.exists(destination):
            raise OSError(f"could not atomically publish HDFS file: {target}")
    finally:
        filesystem.delete(temporary, False)


def ingest(settings: StaticGtfsSettings) -> str:
    from pyspark.sql import SparkSession

    with TemporaryDirectory(prefix="static-gtfs-") as temporary:
        workdir = Path(temporary)
        archive_path = workdir / "feed.zip"
        version = download(settings, archive_path)
        validate_archive(archive_path, settings.max_uncompressed_bytes)
        ingest_date = datetime.now(UTC).date().isoformat()

        spark = SparkSession.builder.appName("static-gtfs-ingestion").getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        try:
            raw_target = f"{settings.raw_path}/ingest_date={ingest_date}/version={version}/feed.zip"
            _copy_to_hdfs(spark, archive_path, raw_target, overwrite=False)

            with zipfile.ZipFile(archive_path) as archive:
                for dataset in ("routes", "stops", "trips", "shapes"):
                    filename = f"{dataset}.txt"
                    archive.extract(filename, workdir)
                    staging = f"{settings.staging_path}/version={version}/{filename}"
                    _copy_to_hdfs(spark, workdir / filename, staging, overwrite=True)
                    frame = spark.read.options(**CSV_READ_OPTIONS).csv(staging)
                    cleaned = _clean(dataset, frame)
                    if cleaned.count() != frame.count():
                        raise ValueError(f"{filename} contains invalid required values")
                    cleaned.write.mode("overwrite").parquet(
                        f"{settings.processed_path}/version={version}/{dataset}"
                    )
            return version
        finally:
            spark.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO)
    version = ingest(StaticGtfsSettings.from_env())
    logger.info("static GTFS ingestion completed", extra={"version": version})


if __name__ == "__main__":
    main()
