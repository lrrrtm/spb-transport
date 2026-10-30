"""T17: route feature engineering and deterministic, interpretable k-means."""

from __future__ import annotations

import argparse
import hashlib
import json
import logging
import math
from dataclasses import dataclass
from datetime import date
from typing import TYPE_CHECKING

from spb_transport_spark.anomalies_batch import ALGORITHM_VERSION as ANOMALY_VERSION
from spb_transport_spark.cassandra_sink import (
    configure_cassandra,
    write_analytics_current,
    write_route_clusters,
)
from spb_transport_spark.config import RouteClusteringSettings
from spb_transport_spark.segment_metrics_batch import ALGORITHM_VERSION as METRICS_VERSION

if TYPE_CHECKING:
    from pyspark.sql import DataFrame

logger = logging.getLogger(__name__)

ALGORITHM_VERSION = "route-clustering-v1"
FEATURE_NAMES = (
    "speed_p50_mps",
    "travel_variability",
    "peak_offpeak_ratio",
    "stop_delay_share",
    "coverage",
    "bunching_per_day",
    "irregular_headways_per_day",
    "stationary_per_day",
)


@dataclass(frozen=True)
class RouteVector:
    static_feed_version: str
    route_id: str
    values: tuple[float, ...]


@dataclass(frozen=True)
class ClusterAssignment:
    static_feed_version: str
    route_id: str
    cluster_id: int
    distance: float


@dataclass(frozen=True)
class ClusterProfile:
    cluster_id: int
    route_count: int
    dominant_feature: str
    values: tuple[float, ...]


def build_route_features(
    metrics: DataFrame,
    anomalies: DataFrame,
    start_date: date,
    end_date: date,
    min_qualified_samples: int,
) -> DataFrame:
    from pyspark.sql import functions as F

    segment = (
        metrics.filter(
            (F.col("algorithm_version") == METRICS_VERSION)
            & (F.col("analysis_start") == start_date.isoformat())
            & (F.col("analysis_end") == end_date.isoformat())
        )
        .groupBy("static_feed_version", "route_id")
        .agg(
            F.sum("qualified_samples").alias("qualified_samples"),
            F.sum("total_samples").alias("total_samples"),
            F.avg("speed_p50_mps").alias("speed_p50_mps"),
            F.avg(F.col("travel_time_iqr_s") / F.col("travel_time_p50_s")).alias(
                "travel_variability"
            ),
            F.avg("peak_offpeak_ratio").alias("peak_offpeak_ratio"),
            F.avg("stop_delay_share").alias("stop_delay_share"),
        )
        .filter(F.col("qualified_samples") >= min_qualified_samples)
        .withColumn("coverage", F.col("qualified_samples") / F.col("total_samples"))
    )
    days = (end_date - start_date).days + 1
    anomaly = (
        anomalies.filter(
            (F.col("algorithm_version") == ANOMALY_VERSION)
            & F.col("event_date").between(start_date.isoformat(), end_date.isoformat())
        )
        .groupBy("static_feed_version", "route_id")
        .agg(
            F.sum(F.when(F.col("anomaly_type") == "bunching", 1).otherwise(0)).alias(
                "bunching_count"
            ),
            F.sum(
                F.when(F.col("anomaly_type").isin("short_headway", "long_headway"), 1).otherwise(0)
            ).alias("irregular_headway_count"),
            F.sum(F.when(F.col("anomaly_type") == "stationary", 1).otherwise(0)).alias(
                "stationary_count"
            ),
        )
        .withColumn("bunching_per_day", F.col("bunching_count") / days)
        .withColumn("irregular_headways_per_day", F.col("irregular_headway_count") / days)
        .withColumn("stationary_per_day", F.col("stationary_count") / days)
    )
    return (
        segment.join(anomaly, ["static_feed_version", "route_id"], "left")
        .fillna(
            {
                "peak_offpeak_ratio": 1.0,
                "stop_delay_share": 0.0,
                "bunching_per_day": 0.0,
                "irregular_headways_per_day": 0.0,
                "stationary_per_day": 0.0,
            }
        )
        .select(
            "static_feed_version",
            "route_id",
            "qualified_samples",
            *FEATURE_NAMES,
        )
        .dropna(subset=list(FEATURE_NAMES))
    )


def _distance(left: tuple[float, ...], right: tuple[float, ...]) -> float:
    return sum((a - b) ** 2 for a, b in zip(left, right, strict=True))


def cluster_routes(
    routes: list[RouteVector], cluster_count: int, max_iterations: int
) -> tuple[list[ClusterAssignment], list[ClusterProfile], float]:
    """Small route-level k-means; collecting hundreds of aggregate rows is intentional."""
    if not routes:
        return [], [], 0.0
    if not 1 <= cluster_count <= len(routes):
        raise ValueError("cluster count must not exceed route count")
    width = len(FEATURE_NAMES)
    means = tuple(sum(route.values[i] for route in routes) / len(routes) for i in range(width))
    deviations = tuple(
        math.sqrt(sum((route.values[i] - means[i]) ** 2 for route in routes) / len(routes)) or 1.0
        for i in range(width)
    )
    scaled = {
        (route.static_feed_version, route.route_id): tuple(
            (route.values[i] - means[i]) / deviations[i] for i in range(width)
        )
        for route in routes
    }
    if len(set(scaled.values())) < cluster_count:
        raise ValueError("cluster count must not exceed the number of distinct route vectors")
    ordered = sorted(routes, key=lambda route: (route.route_id, route.static_feed_version))
    centers = [scaled[(ordered[0].static_feed_version, ordered[0].route_id)]]
    selected = {(ordered[0].static_feed_version, ordered[0].route_id)}
    while len(centers) < cluster_count:
        candidate = max(
            [
                route
                for route in ordered
                if (route.static_feed_version, route.route_id) not in selected
            ],
            key=lambda route: (
                min(
                    _distance(scaled[(route.static_feed_version, route.route_id)], c)
                    for c in centers
                ),
                route.route_id,
            ),
        )
        centers.append(scaled[(candidate.static_feed_version, candidate.route_id)])
        selected.add((candidate.static_feed_version, candidate.route_id))

    assignments: dict[tuple[str, str], int] = {}
    for _ in range(max_iterations):
        updated = {
            key: min(
                range(cluster_count), key=lambda index: (_distance(vector, centers[index]), index)
            )
            for key, vector in scaled.items()
        }
        if updated == assignments:
            break
        assignments = updated
        centers = [
            tuple(
                sum(scaled[key][i] for key, cluster in assignments.items() if cluster == cluster_id)
                / sum(cluster == cluster_id for cluster in assignments.values())
                for i in range(width)
            )
            if cluster_id in assignments.values()
            else centers[cluster_id]
            for cluster_id in range(cluster_count)
        ]

    result = [
        ClusterAssignment(
            route.static_feed_version,
            route.route_id,
            assignments[(route.static_feed_version, route.route_id)],
            math.sqrt(
                _distance(
                    scaled[(route.static_feed_version, route.route_id)],
                    centers[assignments[(route.static_feed_version, route.route_id)]],
                )
            ),
        )
        for route in ordered
    ]
    profiles = []
    for cluster_id in range(cluster_count):
        members = [
            route
            for route in routes
            if assignments[(route.static_feed_version, route.route_id)] == cluster_id
        ]
        raw_center = tuple(
            sum(route.values[i] for route in members) / len(members) for i in range(width)
        )
        dominant = max(range(width), key=lambda i: abs(centers[cluster_id][i]))
        profiles.append(
            ClusterProfile(cluster_id, len(members), FEATURE_NAMES[dominant], raw_center)
        )
    return result, profiles, _silhouette(routes, assignments, scaled)


def _silhouette(
    routes: list[RouteVector],
    assignments: dict[tuple[str, str], int],
    scaled: dict[tuple[str, str], tuple[float, ...]],
) -> float:
    if len(set(assignments.values())) < 2:
        return 0.0
    scores = []
    for route in routes:
        key = (route.static_feed_version, route.route_id)
        own = assignments[key]
        same = [
            other
            for other in routes
            if assignments[(other.static_feed_version, other.route_id)] == own and other != route
        ]
        if not same:
            scores.append(0.0)
            continue
        a = sum(
            math.sqrt(_distance(scaled[key], scaled[(r.static_feed_version, r.route_id)]))
            for r in same
        ) / len(same)
        other_clusters = set(assignments.values()) - {own}
        b = min(
            sum(
                math.sqrt(_distance(scaled[key], scaled[(r.static_feed_version, r.route_id)]))
                for r in routes
                if assignments[(r.static_feed_version, r.route_id)] == cluster
            )
            / sum(assignments[(r.static_feed_version, r.route_id)] == cluster for r in routes)
            for cluster in other_clusters
        )
        scores.append((b - a) / max(a, b) if max(a, b) else 0.0)
    return sum(scores) / len(scores)


def model_id(start_date: date, end_date: date, cluster_count: int) -> str:
    payload = json.dumps(
        [ALGORITHM_VERSION, start_date.isoformat(), end_date.isoformat(), cluster_count],
        separators=(",", ":"),
    )
    return hashlib.sha256(payload.encode()).hexdigest()


def run_job(
    settings: RouteClusteringSettings, start_date: date, end_date: date
) -> dict[str, float]:
    from pyspark.sql import SparkSession
    from pyspark.sql import functions as F

    if end_date < start_date:
        raise ValueError("end date must not precede start date")
    spark = (
        configure_cassandra(SparkSession.builder.appName("route-clustering"), settings.cassandra)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.sources.partitionOverwriteMode", "dynamic")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    try:
        features = build_route_features(
            spark.read.parquet(settings.segment_metrics_path),
            spark.read.parquet(settings.anomalies_path),
            start_date,
            end_date,
            settings.min_qualified_samples,
        ).cache()
        routes = [
            RouteVector(
                row.static_feed_version,
                row.route_id,
                tuple(float(row[name]) for name in FEATURE_NAMES),
            )
            for row in features.collect()
        ]
        assignments, profiles, silhouette = cluster_routes(
            routes, settings.cluster_count, settings.max_iterations
        )
        identifier = model_id(start_date, end_date, settings.cluster_count)
        assignment_rows = [
            (a.static_feed_version, a.route_id, a.cluster_id, a.distance) for a in assignments
        ]
        cluster_frame = spark.createDataFrame(
            assignment_rows,
            "static_feed_version string, route_id string, cluster_id int, distance double",
        )
        profile_rows = [
            (p.cluster_id, p.route_count, p.dominant_feature, *p.values) for p in profiles
        ]
        profile_schema = "cluster_id int, route_count int, dominant_feature string, " + ", ".join(
            f"{name} double" for name in FEATURE_NAMES
        )
        profile_frame = spark.createDataFrame(profile_rows, profile_schema)
        lineage = {
            "model_id": identifier,
            "algorithm_version": ALGORITHM_VERSION,
            "period_start": start_date.isoformat(),
            "period_end": end_date.isoformat(),
        }
        for name, value in lineage.items():
            value_column = F.lit(value).cast("date") if name.startswith("period_") else F.lit(value)
            features = features.withColumn(name, value_column)
            cluster_frame = cluster_frame.withColumn(name, value_column)
            profile_frame = profile_frame.withColumn(name, value_column)
        metrics = spark.createDataFrame(
            [(identifier, settings.cluster_count, len(routes), silhouette)],
            "model_id string, cluster_count int, route_count int, silhouette double",
        ).withColumn("algorithm_version", F.lit(ALGORITHM_VERSION))
        features.write.mode("overwrite").partitionBy("model_id").parquet(
            f"{settings.output_path}/features"
        )
        cluster_frame.write.mode("overwrite").partitionBy("model_id").parquet(
            f"{settings.output_path}/assignments"
        )
        profile_frame.write.mode("overwrite").partitionBy("model_id").parquet(
            f"{settings.output_path}/profiles"
        )
        metrics.write.mode("overwrite").partitionBy("model_id").parquet(
            f"{settings.output_path}/metrics"
        )
        write_route_clusters(features, cluster_frame, settings.cassandra)
        pointer = (
            features.select("static_feed_version")
            .distinct()
            .limit(1)
            .select(
                F.lit("route_clusters").alias("dataset"),
                F.lit(ALGORITHM_VERSION).alias("algorithm_version"),
                F.lit(identifier).alias("model_id"),
                "static_feed_version",
                F.lit(start_date.isoformat()).cast("date").alias("period_start"),
                F.lit(end_date.isoformat()).cast("date").alias("period_end"),
                F.lit(None).cast("date").alias("training_start"),
                F.lit(None).cast("date").alias("training_end"),
                F.current_timestamp().alias("updated_at"),
            )
        )
        write_analytics_current(pointer, settings.cassandra)
        features.unpersist()
        return {"routes": float(len(routes)), "silhouette": silhouette}
    finally:
        spark.stop()


def main() -> None:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    parser = argparse.ArgumentParser(description="Build route features and clusters")
    parser.add_argument("--start-date", required=True, type=date.fromisoformat)
    parser.add_argument("--end-date", required=True, type=date.fromisoformat)
    arguments = parser.parse_args()
    metrics = run_job(RouteClusteringSettings.from_env(), arguments.start_date, arguments.end_date)
    logger.info("route clustering completed: %s", " ".join(f"{k}={v}" for k, v in metrics.items()))


if __name__ == "__main__":
    main()
