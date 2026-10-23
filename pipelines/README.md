# Data pipelines

`spark/` содержит Structured Streaming jobs. Raw archive читает только
`gtfs-raw-response.v1`: валидные envelope сохраняются в Parquet/HDFS, а ошибки
контракта, checksum/base64 и protobuf decode направляются в отдельный quarantine dataset.
Pipeline-код не импортирует внутренности FastAPI или frontend.

Static GTFS job сохраняет исходный ZIP по content hash, проверяет обязательные файлы и
колонки и создаёт versioned Parquet datasets `routes`, `stops`, `trips`, `shapes`, `stop_times`.
Схемы и HDFS paths: [`../docs/data-contracts/static-gtfs.md`](../docs/data-contracts/static-gtfs.md).

## Streaming jobs

Все jobs запускаются Compose-сервисами `spark-raw-archive`, `spark-position-stream` и
`spark-eta-stream`. Параметры описаны в [`../docs/configuration.md`](../docs/configuration.md).
У каждого sink отдельный checkpoint: `<checkpoint_path>/{processed|history,quarantine,cassandra}`.

| Job | Вход | Выход в HDFS | Partitioning | Дедупликация |
|---|---|---|---|---|
| `raw_archive.py` | `gtfs-raw-response.v1` | raw Parquet + quarantine | `endpoint/ingest_date/ingest_hour`; quarantine — `quarantine_reason/ingest_date/ingest_hour` | нет: raw является журналом запросов |
| `position_stream.py` | `vehicle-position.v1` | processed positions + quarantine | `event_date/event_hour`; quarantine — `quarantine_reason/ingest_date/ingest_hour` | `(vehicle_id, source_timestamp)` в пределах `SPARK_POSITION_WATERMARK` |
| `eta_stream.py` | `vehicle-eta.v1` | ETA history + quarantine | `collected_date/collected_hour`; quarantine — `quarantine_reason/ingest_date/ingest_hour` | `event_id` (хеш содержимого) в пределах `SPARK_ETA_WATERMARK` |

Processed positions сохраняют поля `vehicle-position.v1`, Kafka metadata, `quality_flags`
(`stale_source_timestamp`, `future_source_timestamp`, `speed_missing`, `bearing_missing`,
`stationary_observation`) и `has_quality_flags`. ETA history добавляет `forecast_count` и
`eta_status` (`available` или `empty`). Quarantine хранит исходный JSON и `quarantine_reason`.

## Batch jobs

| Job | Вход | Выход в HDFS | Partitioning | Идемпотентность |
|---|---|---|---|---|
| `route_match_batch.py` (T09) | processed positions за `event_date` + static `trips`, `shapes` | silver `vehicle_route_matches` | `algorithm_version/event_date` | dynamic overwrite партиции, детерминированные `track_id`/`run_id` |
| `stop_events_batch.py` (T10) | `matched` строки `vehicle_route_matches` + static `trips`, `shapes`, `stops`, `stop_times` | `shape_stop_patterns`, `stop_arrival_events`, `segment_traversals` | patterns — `algorithm_version/static_feed_version`; события — `algorithm_version/event_date` | dynamic overwrite партиций, `event_id` из run, pattern и индекса остановки |

Batch jobs запускаются по порядку для одного дня и одной версии static feed:
`spark-route-matching`, затем `spark-stop-events` (см. `docs/configuration.md`).

Интеграционные тесты batch jobs запускают локальный Spark (`make test-spark`, нужна Java 17);
обычный `make check` их пропускает, в CI они идут отдельным job `spark`.

Matching выполняется на чистом Python (`route_matching.py`, `shape_geometry.py`) внутри
`groupByKey` по `vehicle_id`: последовательность наблюдений машины нужна целиком. Shapes
передаются broadcast-переменной, геометрия строится один раз на executor. Схема выхода:
[`../docs/data-contracts/route-matches.md`](../docs/data-contracts/route-matches.md).
T10 так же группирует `matched` наблюдения по `run_id` и вычисляет события в
`stop_events.py`; stop patterns строятся на driver из агрегированных в Spark `stop_times`.
Схемы: [`../docs/data-contracts/stop-events.md`](../docs/data-contracts/stop-events.md).

## Cassandra serving sink

`cassandra_sink.py` пишет валидные дедуплицированные события через `foreachBatch`.
Список колонок и TTL-политика каждой таблицы объявлены в `POSITION_TABLES` и `ETA_TABLES`;
`tests/spark/test_cassandra_serving.py` проверяет их точное совпадение со схемой
[`../infra/cassandra/serving.cql.template`](../infra/cassandra/serving.cql.template).

| Таблица | Источник | Writetime | TTL |
|---|---|---|---|
| `vehicle_latest_by_vehicle` | positions | `source_timestamp` | `CASSANDRA_LATEST_TTL_SECONDS` |
| `vehicles_latest_by_route` | positions | `source_timestamp` | `CASSANDRA_LATEST_TTL_SECONDS` |
| `vehicle_positions_by_vehicle_day` | positions | `source_timestamp` | нет |
| `eta_history_by_vehicle_day` | ETA | `collected_at` | нет |

Идемпотентность: history primary keys содержат детерминированный `event_id`, поэтому
повтор micro-batch перезаписывает те же строки. Для latest-таблиц writetime равен времени
исходного наблюдения, поэтому replay старого события не затирает более новое состояние.
Добавление новой serving-таблицы начинается с query pattern и оценки partition size
(ADR 0007), затем обновляются schema template, `ServingTable` и тесты.
