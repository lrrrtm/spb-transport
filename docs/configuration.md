# Контракт конфигурации

## Правила

- Infrastructure image tags и порты задаются через `.env` без fallback в `compose.yaml`.
- `.env.example` содержит явную локальную конфигурацию и копируется в `.env`.
- Прикладные сервисы валидируют обязательные параметры до начала работы.
- Секреты не выводятся в лог и не сохраняются в raw metadata.
- Безопасный default допустим только для поведения, например включённой проверки TLS; адреса, topics, credentials, polling и batch sizes задаются явно.

## Bootstrap infrastructure

| Переменная | Назначение |
|---|---|
| `KAFKA_IMAGE` | Закреплённый образ Kafka |
| `KAFKA_CLUSTER_ID` | Идентификатор локального KRaft cluster |
| `KAFKA_EXTERNAL_PORT` | Порт Kafka на host |
| `HADOOP_IMAGE` | Закреплённый образ Hadoop |
| `HDFS_NAMENODE_RPC_PORT` | NameNode RPC на host |
| `HDFS_NAMENODE_HTTP_PORT` | NameNode UI на host |
| `HDFS_DATANODE_HTTP_PORT` | DataNode UI на host |
| `SPARK_IMAGE` | Закреплённый образ Spark с Python |
| `SPARK_MASTER_PORT` | Spark master RPC на host |
| `SPARK_MASTER_WEB_PORT` | Spark master UI на host |
| `SPARK_WORKER_WEB_PORT` | Spark worker UI на host |
| `SPARK_WORKER_CORES` | Локальный лимит cores worker |
| `SPARK_WORKER_MEMORY` | Локальный лимит памяти worker |
| `CASSANDRA_IMAGE` | Закреплённый образ Cassandra |
| `CASSANDRA_PORT` | Native transport port на host |
| `CASSANDRA_CLUSTER_NAME` | Имя локального cluster |
| `CASSANDRA_DATACENTER` | Имя local datacenter |
| `CASSANDRA_HEAP_NEWSIZE` | Young generation heap |
| `CASSANDRA_MAX_HEAP_SIZE` | Максимальный JVM heap |

## Будущая конфигурация collector

До T01 значения только документируют согласованный интерфейс:

- `GTFS_STATIC_URL`;
- `GTFS_REALTIME_BASE_URL`;
- `GTFS_TLS_VERIFY`;
- optional `GTFS_CA_BUNDLE`;
- `GTFS_CONNECT_TIMEOUT_SECONDS`;
- `GTFS_READ_TIMEOUT_SECONDS`;
- `GTFS_WRITE_TIMEOUT_SECONDS`;
- `GTFS_POOL_TIMEOUT_SECONDS`;
- `GTFS_RETRY_MAX_ATTEMPTS`;
- `GTFS_RETRY_BACKOFF_BASE_SECONDS`;
- `GTFS_RETRY_BACKOFF_MAX_SECONDS`;
- `GTFS_RETRY_JITTER_SECONDS`;
- `GTFS_VEHICLE_POLL_INTERVAL_SECONDS`;
- `GTFS_VEHICLE_TRIPS_POLL_INTERVAL_SECONDS`;
- `GTFS_VEHICLE_TRIPS_BATCH_SIZE`;
- `KAFKA_BOOTSTRAP_SERVERS`;
- `KAFKA_VEHICLE_POSITION_TOPIC`;
- `KAFKA_VEHICLE_ETA_TOPIC`;
- `KAFKA_CLIENT_ID`;
- `KAFKA_LINGER_MS`;
- `KAFKA_MAX_BATCH_SIZE_BYTES`;
- `KAFKA_REQUEST_TIMEOUT_MS`;
- Kafka bootstrap servers и topic names.

HTTP timeout и retry-параметры обязательны: collector не подставляет их молча. Единственное встроенное безопасное значение — `GTFS_TLS_VERIFY=true`.
