# Проверка отказов и повторной обработки

T19 проверяет границы, на которых сбой способен потерять или продублировать данные. Тесты не
обращаются к внешнему транспортному API: HTTP-ответы фиксированы, а интеграционные проверки
используют локальные Spark и Cassandra.

| Сценарий | Проверка |
|---|---|
| временная ошибка исходного HTTP API | retry/backoff и завершение после исчерпания попыток в `tests/collector/test_http_client.py` |
| ошибка публикации в Kafka | исключение producer не скрывается; collector не считает цикл успешным |
| битый realtime payload | quarantine вместо падения streaming query |
| дубликаты и повтор batch после рестарта | повтор T09/T10 с теми же ключами оставляет тот же набор Parquet-строк |
| Cassandra недоступна | `/ready` и data endpoints возвращают 503, после восстановления снова 200 |
| перезапуск API | новый Cassandra session читает ранее записанные latest/history/static данные |
| HTTP API ↔ Cassandra | настоящий FastAPI читает seeded Cassandra через production repository |

Быстрые проверки без внешних процессов:

```bash
make test
```

Spark jobs с повторным запуском:

```bash
make test-spark
```

API и repository с настоящей Cassandra:

```bash
docker compose --env-file .env.example up -d cassandra
CASSANDRA_TEST_HOST=localhost make test-cassandra
```

Интеграционные тесты создают отдельный `spb_transport_test`, повторно записывают детерминированные
ключи и удаляют keyspace после прогона. Рабочий keyspace и собранные данные они не затрагивают.
