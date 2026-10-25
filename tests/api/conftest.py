import pytest
from spb_transport_api.config import ApiSettings

ENVIRONMENT = {
    "API_HOST": "0.0.0.0",
    "API_PORT": "8000",
    "CASSANDRA_HOST": "cassandra",
    "CASSANDRA_NATIVE_PORT": "9042",
    "CASSANDRA_DATACENTER": "datacenter1",
    "CASSANDRA_KEYSPACE": "spb_transport",
    "CASSANDRA_USERNAME": "",
    "CASSANDRA_PASSWORD": "",
    "API_CASSANDRA_REQUEST_TIMEOUT_SECONDS": "5",
    "API_VEHICLE_FRESHNESS_SECONDS": "120",
    "API_DEFAULT_PAGE_SIZE": "2",
    "API_MAX_PAGE_SIZE": "5",
    "API_HISTORY_MAX_RANGE_HOURS": "24",
    "API_HISTORY_DEFAULT_RANGE_MINUTES": "60",
    "API_CORS_ORIGINS": "http://localhost:5173",
}


@pytest.fixture
def settings(monkeypatch: pytest.MonkeyPatch) -> ApiSettings:
    for name, value in ENVIRONMENT.items():
        monkeypatch.setenv(name, value)
    return ApiSettings()  # type: ignore[call-arg]
