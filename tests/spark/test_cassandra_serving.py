import re
from pathlib import Path
from typing import Any

import pytest
from spb_transport_spark.cassandra_sink import (
    ANOMALIES_TABLE,
    ETA_COMPARISON_TABLE,
    ETA_TABLES,
    POSITION_TABLES,
    ROUTE_CLUSTERS_TABLE,
    ROUTE_FEATURES_TABLE,
    SEGMENT_METRICS_TABLE,
    STATIC_TABLES,
    ServingTable,
    configure_cassandra,
)
from spb_transport_spark.config import CassandraSettings

TEMPLATE = Path(__file__).parents[2] / "infra" / "cassandra" / "serving.cql.template"
PLACEHOLDERS = {"KEYSPACE", "DATACENTER", "REPLICATION_FACTOR"}
LATEST_TABLES = {"vehicle_latest_by_vehicle", "vehicles_latest_by_route"}
EVENT_TABLES = (*POSITION_TABLES, *ETA_TABLES)
SERVING_TABLES = (
    *EVENT_TABLES,
    *STATIC_TABLES,
    SEGMENT_METRICS_TABLE,
    ANOMALIES_TABLE,
    ETA_COMPARISON_TABLE,
    ROUTE_FEATURES_TABLE,
    ROUTE_CLUSTERS_TABLE,
)


def _render_template() -> str:
    # Mirrors the substitution performed by the cassandra-init Compose service.
    return (
        TEMPLATE.read_text(encoding="utf-8")
        .replace("{{KEYSPACE}}", "spb_transport")
        .replace("{{DATACENTER}}", "datacenter1")
        .replace("{{REPLICATION_FACTOR}}", "1")
    )


def _parse_tables(cql: str) -> dict[str, tuple[set[str], set[str]]]:
    """Return table name -> (all columns, primary key columns)."""
    tables: dict[str, tuple[set[str], set[str]]] = {}
    pattern = re.compile(
        r"CREATE TABLE IF NOT EXISTS spb_transport\.(\w+) \((.*?)\)\s*(?:WITH [^;]*)?;", re.S
    )
    for name, body in pattern.findall(cql):
        columns: set[str] = set()
        key: set[str] = set()
        for line in (part.strip() for part in body.splitlines()):
            if not line:
                continue
            compound = re.fullmatch(r"PRIMARY KEY \((.*)\),?", line)
            if compound is not None:
                key.update(re.findall(r"\w+", compound.group(1)))
                continue
            column = line.split()[0]
            columns.add(column)
            if "PRIMARY KEY" in line:
                key.add(column)
        tables[name] = (columns, key)
    return tables


def test_template_uses_only_placeholders_rendered_by_cassandra_init() -> None:
    template = TEMPLATE.read_text(encoding="utf-8")

    assert set(re.findall(r"\{\{(\w+)\}\}", template)) == PLACEHOLDERS
    assert "{{" not in _render_template()


def test_schema_statements_are_idempotent() -> None:
    statements = [s.strip() for s in _render_template().split(";") if s.strip()]

    assert statements
    assert all(re.match(r"CREATE (KEYSPACE|TABLE) IF NOT EXISTS ", s) for s in statements)


def test_schema_creates_exactly_the_tables_written_by_spark() -> None:
    tables = _parse_tables(_render_template())

    assert set(tables) == {table.name for table in SERVING_TABLES}


@pytest.mark.parametrize("table", SERVING_TABLES, ids=lambda table: table.name)
def test_sink_columns_match_schema(table: ServingTable) -> None:
    columns, key = _parse_tables(_render_template())[table.name]

    assert len(table.columns) == len(set(table.columns))
    assert set(table.columns) == columns
    assert key <= set(table.columns)


def test_history_keys_are_deterministic_event_identifiers() -> None:
    tables = _parse_tables(_render_template())

    for table in EVENT_TABLES:
        if table.name not in LATEST_TABLES:
            assert "event_id" in tables[table.name][1]


def test_static_tables_are_keyed_by_feed_version() -> None:
    tables = _parse_tables(_render_template())

    for table in STATIC_TABLES:
        if table.name != "static_feed_current":
            assert "static_feed_version" in tables[table.name][1]
    assert STATIC_TABLES[-1].name == "static_feed_current"


def test_only_latest_tables_expire() -> None:
    assert {table.name for table in SERVING_TABLES if table.expiring} == LATEST_TABLES


class _RecordingBuilder:
    def __init__(self) -> None:
        self.values: dict[str, Any] = {}

    def config(self, key: str, value: Any) -> "_RecordingBuilder":
        self.values[key] = value
        return self


def _settings(username: str | None = None, password: str | None = None) -> CassandraSettings:
    return CassandraSettings(
        host="cassandra",
        port=9042,
        local_dc="datacenter1",
        keyspace="spb_transport",
        latest_ttl_seconds=120,
        username=username,
        password=password,
    )


def test_configure_cassandra_without_credentials() -> None:
    builder = _RecordingBuilder()

    configure_cassandra(builder, _settings())

    assert builder.values == {
        "spark.cassandra.connection.host": "cassandra",
        "spark.cassandra.connection.port": "9042",
        "spark.cassandra.connection.localDC": "datacenter1",
    }


def test_configure_cassandra_passes_credentials_as_pair() -> None:
    builder = _RecordingBuilder()

    configure_cassandra(builder, _settings("app", "secret"))

    assert builder.values["spark.cassandra.auth.username"] == "app"
    assert builder.values["spark.cassandra.auth.password"] == "secret"
