# Диагностика и восстановление

Команды ниже не удаляют volumes и собранные данные. Начинать следует с состояния контейнеров и
свежести данных:

```bash
docker compose --env-file .env.example ps
curl -fsS http://localhost:8000/ready
curl -fsS http://localhost:8000/api/v1/observability
```

`/ready` проверяет доступность Cassandra. `/api/v1/observability` показывает время последней
позиции, лаг источника, число свежих/устаревших машин и счётчики `quality_flags`. Нулевое число
машин допустимо после чистого запуска; растущий `source_lag_seconds` при работающем collector
означает разрыв между collector, Kafka, Spark или Cassandra.

## Collector или исходный API

```bash
docker compose logs --since=10m collector
docker compose restart collector
```

Искать JSON events `vehicle_collection_failed`, `vehicle_trips_batch_failed`, HTTP status и число
`invalid_entities`. Повторные HTTP-ошибки проверяются до перезапуска: рестарт не исправляет URL,
TLS CA или недоступность источника.

## Kafka

```bash
docker compose logs --since=10m kafka kafka-init
docker compose exec kafka /opt/kafka/bin/kafka-topics.sh \
  --bootstrap-server localhost:29092 --list
docker compose restart kafka
```

После healthy Kafka сервисы с `restart: unless-stopped` переподключаются сами. Не удалять
`kafka-data`: checkpoint Spark ссылается на сохранённые offsets.

## HDFS и Spark streaming

```bash
docker compose logs --since=10m hdfs-namenode hdfs-datanode spark-position-stream spark-eta-stream
docker compose restart hdfs-namenode hdfs-datanode
docker compose restart spark-position-stream spark-eta-stream
```

Сначала восстановить HDFS, затем streaming jobs. Их checkpoint directories обеспечивают
продолжение с подтверждённых Kafka offsets. Удаление checkpoint допустимо только при осознанном
полном replay и может создать повторную нагрузку; штатным способом восстановления не является.

## Cassandra и API

```bash
docker compose logs --since=10m cassandra cassandra-init api
docker compose restart cassandra
docker compose up -d cassandra-init api web
curl -fsS http://localhost:8000/ready
```

`cassandra-init` выполняет идемпотентный `CREATE ... IF NOT EXISTS`; существующие строки не
удаляются. Если `/ready` остаётся 503, проверить datacenter, keyspace и credentials в `.env`, затем
сообщение `Cassandra query ... failed` в JSON-логе API.

## Повтор batch-задач

Batch запускаются повторно с теми же датами и версиями. Parquet-партиции перезаписываются, а
Cassandra primary keys детерминированы. Конкретные команды находятся в
[`configuration.md`](configuration.md). Перед повтором проверить входные партиции HDFS и не
менять границы периода: другой период является новым расчётом, а не восстановлением старого.
