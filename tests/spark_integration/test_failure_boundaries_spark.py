import json
from datetime import datetime

import pytest

pytest.importorskip("pyspark")

from spb_transport_spark.raw_archive import classify  # noqa: E402

from spark_integration.spark_lake import local_spark

pytestmark = pytest.mark.spark


def test_broken_raw_events_are_quarantined_without_losing_kafka_coordinates() -> None:
    spark = local_spark()
    source = spark.createDataFrame(
        [
            (b"not-json", "raw-response.v1", 1, 10, datetime(2026, 10, 1, 8)),
            (
                json.dumps(
                    {
                        "schema_version": "1",
                        "request_id": "request-1",
                        "endpoint": "vehicle",
                        "requested_at": "2026-10-01T08:00:00Z",
                        "collected_at": "2026-10-01T08:00:01Z",
                        "http_status": 200,
                        "payload_base64": "AA==",
                        "sha256": "0" * 64,
                    }
                ).encode(),
                "raw-response.v1",
                1,
                11,
                datetime(2026, 10, 1, 8),
            ),
        ],
        "value binary, topic string, partition int, offset long, timestamp timestamp",
    )

    rows = classify(source).orderBy("offset").collect()

    assert [(row.offset, row.quarantine_reason) for row in rows] == [
        (10, "malformed_json_or_missing_schema"),
        (11, "checksum_mismatch"),
    ]
    assert all(row.raw_json is not None for row in rows)
