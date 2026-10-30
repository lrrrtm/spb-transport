from datetime import date

import pytest
from pytest import MonkeyPatch
from spb_transport_spark.config import RouteClusteringSettings
from spb_transport_spark.route_clustering_batch import RouteVector, cluster_routes, model_id


def _environment(monkeypatch: MonkeyPatch) -> None:
    values = {
        "HDFS_SEGMENT_METRICS_PATH": "hdfs://nn/gold/segments",
        "HDFS_TRANSPORT_ANOMALIES_PATH": "hdfs://nn/gold/anomalies",
        "HDFS_ROUTE_CLUSTERS_PATH": "hdfs://nn/gold/clusters/",
        "ROUTE_CLUSTER_COUNT": "4",
        "ROUTE_CLUSTER_MAX_ITERATIONS": "30",
        "ROUTE_CLUSTER_MIN_QUALIFIED_SAMPLES": "100",
        "CASSANDRA_HOST": "cassandra",
        "CASSANDRA_NATIVE_PORT": "9042",
        "CASSANDRA_DATACENTER": "datacenter1",
        "CASSANDRA_KEYSPACE": "spb_transport",
        "CASSANDRA_LATEST_TTL_SECONDS": "120",
    }
    for name, value in values.items():
        monkeypatch.setenv(name, value)


def test_settings_read_clustering_parameters(monkeypatch: MonkeyPatch) -> None:
    _environment(monkeypatch)

    settings = RouteClusteringSettings.from_env()

    assert settings.output_path == "hdfs://nn/gold/clusters"
    assert settings.cluster_count == 4
    assert settings.min_qualified_samples == 100


def test_kmeans_is_deterministic_and_separates_obvious_groups() -> None:
    routes = [
        RouteVector("f", "a", (1.0,) * 8),
        RouteVector("f", "b", (1.1,) * 8),
        RouteVector("f", "c", (9.0,) * 8),
        RouteVector("f", "d", (9.1,) * 8),
    ]

    assignments, profiles, silhouette = cluster_routes(routes, 2, 20)
    repeated = cluster_routes(routes, 2, 20)[0]
    by_route = {assignment.route_id: assignment.cluster_id for assignment in assignments}

    assert assignments == repeated
    assert by_route["a"] == by_route["b"]
    assert by_route["c"] == by_route["d"]
    assert by_route["a"] != by_route["c"]
    assert len(profiles) == 2 and silhouette > 0.9


def test_cluster_count_cannot_exceed_distinct_vectors() -> None:
    routes = [RouteVector("f", "a", (1.0,) * 8), RouteVector("f", "b", (1.0,) * 8)]

    with pytest.raises(ValueError, match="distinct"):
        cluster_routes(routes, 2, 10)


def test_model_id_includes_period_and_cluster_count() -> None:
    first = model_id(date(2026, 10, 1), date(2026, 10, 7), 4)
    second = model_id(date(2026, 10, 1), date(2026, 10, 7), 5)

    assert len(first) == 64 and first != second
