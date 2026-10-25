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
| `SPARK_EXECUTOR_CORES` | Число cores одного job; локально оставляет ресурсы параллельным streaming jobs |
| `SPARK_MAX_CORES_PER_JOB` | Общий лимит cores одного Spark application |
| `SPARK_SHUFFLE_PARTITIONS` | Число shuffle partitions для micro-batch jobs |
| `CASSANDRA_IMAGE` | Закреплённый образ Cassandra |
| `CASSANDRA_PORT` | Native transport port на host |
| `CASSANDRA_NATIVE_PORT` | Native transport port внутри Compose-сети |
| `CASSANDRA_CLUSTER_NAME` | Имя локального cluster |
| `CASSANDRA_DATACENTER` | Имя local datacenter |
| `CASSANDRA_HOST` | Адрес Cassandra для Spark jobs |
| `CASSANDRA_KEYSPACE` | Keyspace serving-слоя; lowercase CQL identifier |
| `CASSANDRA_REPLICATION_FACTOR` | Replication factor для инициализации keyspace |
| `CASSANDRA_LATEST_TTL_SECONDS` | TTL operational latest-строк; ограничивает stale route membership |
| `CASSANDRA_USERNAME`, `CASSANDRA_PASSWORD` | Необязательная пара credentials; задаются только вместе |
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

## Vehicle position Spark job

- `KAFKA_VEHICLE_POSITION_TOPIC` — входной topic typed position events;
- `SPARK_POSITION_WATERMARK` — допустимое опоздание для stateful deduplication;
- `SPARK_POSITION_MAX_SOURCE_AGE_SECONDS` — порог flag `stale_source_timestamp`;
- `SPARK_POSITION_MAX_FUTURE_SKEW_SECONDS` — порог flag `future_source_timestamp`;
- `HDFS_POSITION_PROCESSED_PATH` — валидные дедуплицированные позиции;
- `HDFS_POSITION_QUARANTINE_PATH` — malformed и contract-invalid события;
- `HDFS_POSITION_CHECKPOINT_PATH` — отдельные checkpoints processed, quarantine и Cassandra sinks.

Дедупликация выполняется по `(vehicle_id, source_timestamp)`, а не по времени polling.
`speed=0` остаётся валидным значением и получает информационный flag
`stationary_observation`. Отсутствующие `speed` и `bearing` также сохраняются с quality flags.
В Cassandra job поддерживает lookup последнего состояния по машине и по маршруту, а также
дневную историю машины. Для latest-таблиц используется обязательный TTL: это ограничивает
жизнь старой записи при смене маршрута или исчезновении машины. API всё равно должен применять
freshness cutoff по `source_timestamp`, а не считать наличие строки доказательством активности.

## Vehicle ETA Spark job

- `KAFKA_VEHICLE_ETA_TOPIC` — входной topic ETA snapshots;
- `SPARK_ETA_WATERMARK` — bounded state для content deduplication по `event_id`;
- `HDFS_ETA_HISTORY_PATH` — история уникальных ETA snapshots;
- `HDFS_ETA_QUARANTINE_PATH` — malformed и contract-invalid события;
- `HDFS_ETA_CHECKPOINT_PATH` — отдельные checkpoints history, quarantine и Cassandra sinks.

Пустой `forecasts=[]` сохраняется как успешный snapshot с `eta_status=empty`. Состояние
`missing` не создаётся искусственно: это отсутствие snapshot для машины в заданном временном
окне и вычисляется позднее по истории. `forecast_rank` валидируется только как порядок
элементов payload и не интерпретируется как GTFS `stop_sequence`.

## Route/shape matching Spark job (T09)

- `HDFS_POSITION_PROCESSED_PATH` и `HDFS_STATIC_PROCESSED_PATH` — входы;
- `HDFS_ROUTE_MATCHES_PATH` — silver `vehicle_route_matches`;
- `ROUTE_MATCH_*` — 14 параметров matcher; все обязательны и проверяются при старте.

Значения в `.env.example` предварительные, их смысл и способ калибровки описаны в
[`research/route-matching.md`](research/route-matching.md). День и версия static feed
передаются аргументами запуска, а не через окружение:

```shell
docker compose --profile batch run --rm spark-route-matching \
    --event-date 2026-10-01 --static-version <sha256>
```

## Stop arrival и segment traversal Spark job (T10)

- `HDFS_ROUTE_MATCHES_PATH`, `HDFS_STATIC_PROCESSED_PATH` — входы;
- `HDFS_STOP_PATTERNS_PATH`, `HDFS_STOP_ARRIVALS_PATH`, `HDFS_SEGMENT_TRAVERSALS_PATH` — выходы;
- `STOP_EVENT_STOP_MAX_OFFSET_M`, `STOP_EVENT_ZONE_BEFORE_M`, `STOP_EVENT_ZONE_AFTER_M`,
  `STOP_EVENT_MAX_INTERPOLATION_GAP_SECONDS`, `STOP_EVENT_STOPPED_MAX_SPEED_MPS` — пороги
  алгоритма, все обязательны; обоснование значений — [`research/stop-arrivals.md`](research/stop-arrivals.md).

Запускается после T09 для того же дня и той же версии static feed:

```shell
docker compose --profile batch run --rm spark-stop-events \
    --event-date 2026-10-01 --static-version <sha256>
```

## Cassandra serving layer

Schema bootstrap создаёт только таблицы, для которых уже есть конкретные query patterns:

| Таблица | Запрос | Граница partition |
|---|---|---|
| `vehicle_latest_by_vehicle` | Последнее состояние одной машины | Одна строка на `vehicle_id` |
| `vehicles_latest_by_route` | Текущая карта машин маршрута | Один маршрут |
| `vehicle_positions_by_vehicle_day` | История машины за день | `(vehicle_id, event_date)` |
| `eta_history_by_vehicle_day` | История ETA машины за день | `(vehicle_id, collected_date)` |
| `static_feed_current` | Опубликованная версия static GTFS | Одна строка |
| `routes_by_feed` | Список маршрутов версии feed | `static_feed_version`: ≈ 500 строк |
| `stops_by_route` | Остановки вариантов маршрута по порядку | `(static_feed_version, route_id)`: ≈ 2 × 30 строк |

При polling раз в 10 секунд position partition содержит не более 8 640 наблюдений в день до
дедупликации; при ETA polling раз в 30 секунд — не более 2 880 snapshots. Это ограничивает
размер partition без произвольного retention history. HDFS остаётся каноническим raw/processed
архивом, Cassandra — query-oriented serving storage.

Повторная запись безопасна: history primary keys детерминированы, а latest-записи используют
source event time как Cassandra writetime. Поэтому более старое повторно обработанное событие не
перезаписывает новое состояние. Пустые credentials разрешены в локальном no-auth Compose;
если задано одно из двух значений, конфигурация завершается ошибкой.

## Static metadata для API (T11)

`spark-static-serving` публикует в Cassandra маршруты и остановки одной версии static GTFS:
`routes_by_feed`, `stops_by_route`, затем `static_feed_current` (переключение версии последним).
Использует `HDFS_STATIC_PROCESSED_PATH`, `STOP_EVENT_STOP_MAX_OFFSET_M` (та же проекция остановок,
что в T10) и `CASSANDRA_*`. Запуск после `spark-static-gtfs`:

```shell
docker compose --profile batch run --rm spark-static-serving --static-version <sha256>
```

## FastAPI (T11)

| Переменная | Назначение |
|---|---|
| `API_EXTERNAL_PORT` | Порт API на host (в контейнере — 8000) |
| `CASSANDRA_HOST`, `CASSANDRA_NATIVE_PORT`, `CASSANDRA_DATACENTER`, `CASSANDRA_KEYSPACE` | Подключение к serving layer |
| `CASSANDRA_USERNAME`, `CASSANDRA_PASSWORD` | Необязательная пара credentials |
| `API_CASSANDRA_REQUEST_TIMEOUT_SECONDS` | Таймаут подключения и одного запроса к Cassandra |
| `API_VEHICLE_FRESHNESS_SECONDS` | Позиции старше порога не считаются текущими |
| `API_DEFAULT_PAGE_SIZE`, `API_MAX_PAGE_SIZE` | Размер страницы списков |
| `API_HISTORY_MAX_RANGE_HOURS` | Максимальный диапазон истории в одном запросе (≤ 72) |
| `API_HISTORY_DEFAULT_RANGE_MINUTES` | Диапазон истории, если `from` не задан |
| `API_CORS_ORIGINS` | Разрешённые origins через запятую; пусто — CORS выключен |

Все значения, кроме credentials, обязательны и проверяются при старте. API стартует и при
недоступной Cassandra: `/health` отвечает 200, `/ready` и запросы к данным — 503, подключение
повторяется на следующем запросе. Контракт endpoints: [`data-contracts/api.md`](data-contracts/api.md);
OpenAPI — `http://localhost:${API_EXTERNAL_PORT}/docs`.

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
