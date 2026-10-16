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
- отдельные polling intervals;
- `vehicleIDs` batch size;
- HTTP timeouts;
- retry count и backoff/jitter;
- Kafka bootstrap servers и topic names.

