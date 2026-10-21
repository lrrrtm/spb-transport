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
| `UV_INSECURE_HOST` | Необязательный список package hosts без TLS-проверки только на этапе локальной сборки Python-образов |
| `KAFKA_IMAGE` | Закреплённый образ Kafka |
| `KAFKA_CLUSTER_ID` | Идентификатор локального KRaft cluster |
| `KAFKA_EXTERNAL_PORT` | Порт Kafka на host |
| `KAFKA_MESSAGE_MAX_BYTES` | Максимальный размер raw response в Kafka broker |
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

`UV_INSECURE_HOST` по умолчанию пуст. Если локальная сеть мешает `uv` проверить цепочку
сертификатов PyPI, временный workaround задаётся явно:

```shell
UV_INSECURE_HOST='pypi.org files.pythonhosted.org' docker compose build
```

Сборка выводит warning. Эта переменная не управляет HTTP-клиентом GTFS и не должна
сохраняться в production-конфигурации.

## Collector

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
- `KAFKA_RAW_RESPONSE_TOPIC`;
- `KAFKA_CLIENT_ID`;
- `KAFKA_LINGER_MS`;
- `KAFKA_MAX_BATCH_SIZE_BYTES`;
- `KAFKA_MAX_REQUEST_SIZE_BYTES`;
- `KAFKA_REQUEST_TIMEOUT_MS`;
- Kafka bootstrap servers и topic names.

HTTP timeout и retry-параметры обязательны: collector не подставляет их молча. Единственное встроенное безопасное значение — `GTFS_TLS_VERIFY=true`.

## Raw archive Spark job

- `SPARK_KAFKA_STARTING_OFFSETS` — только `earliest` или `latest`;
- `SPARK_TRIGGER_INTERVAL` — интервал micro-batch в синтаксисе Spark;
- `HDFS_RAW_REALTIME_PATH` — Parquet-архив валидных raw envelope;
- `HDFS_RAW_QUARANTINE_PATH` — Parquet для ошибок контракта, checksum и protobuf;
- `HDFS_RAW_CHECKPOINT_PATH` — отдельные checkpoint каталоги обоих sinks.

Все пути, topic и интервал обязательны. Kafka connector закреплён в Spark-образе вместе с
checksum зависимостей: job не скачивает JAR при старте.

## Static GTFS ingestion

- `GTFS_STATIC_URL`, `GTFS_TLS_VERIFY` и optional `GTFS_CA_BUNDLE`;
- `GTFS_STATIC_TIMEOUT_SECONDS`, `GTFS_STATIC_RETRY_MAX_ATTEMPTS`,
  `GTFS_STATIC_RETRY_BACKOFF_SECONDS`;
- `GTFS_STATIC_MAX_DOWNLOAD_BYTES` и `GTFS_STATIC_MAX_UNCOMPRESSED_BYTES`;
- `HDFS_STATIC_RAW_PATH`, `HDFS_STATIC_PROCESSED_PATH`, `HDFS_STATIC_STAGING_PATH`.

Одноразовый job запускается явно: `docker compose --profile ingestion run --rm spark-static-gtfs`.
Версия feed равна SHA-256 ZIP; raw ZIP не перезаписывается, производные Parquet можно
детерминированно перестроить для той же версии.

Для текущей проблемы сертификата официального источника локальный запуск допускает явное
переопределение: `GTFS_TLS_VERIFY=false docker compose --profile ingestion run --rm
spark-static-gtfs`. При этом job пишет warning; значение в `.env.example` остаётся безопасным —
`true`. `GTFS_CA_BUNDLE` следует использовать вместо отключения проверки, когда доступна
доверенная цепочка.
