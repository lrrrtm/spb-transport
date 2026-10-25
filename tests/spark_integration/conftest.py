"""Fixtures of Spark integration tests (`make test-spark`)."""

import os
import sys
from pathlib import Path

import pytest
from pytest import MonkeyPatch
from spb_transport_spark.config import ROUTE_MATCH_PARAMETER_ENV

from spark_integration.spark_lake import PARAMETERS, write_inputs


@pytest.fixture
def lake(tmp_path: Path, monkeypatch: MonkeyPatch) -> Path:
    """Static GTFS and processed positions of one day; environment of both batch jobs."""
    pytest.importorskip("pyspark")
    if sys.platform == "win32" and not os.environ.get("HADOOP_HOME"):
        pytest.skip(
            "local Spark on Windows needs HADOOP_HOME with winutils.exe;"
            " run `make test-spark` in WSL or rely on the CI spark job"
        )
    write_inputs(tmp_path)
    monkeypatch.setenv("HDFS_POSITION_PROCESSED_PATH", str(tmp_path / "positions"))
    monkeypatch.setenv("HDFS_STATIC_PROCESSED_PATH", str(tmp_path / "static"))
    monkeypatch.setenv("HDFS_ROUTE_MATCHES_PATH", str(tmp_path / "matches"))
    for field, name in ROUTE_MATCH_PARAMETER_ENV.items():
        monkeypatch.setenv(name, PARAMETERS[field])
    return tmp_path
