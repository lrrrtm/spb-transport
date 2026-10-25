# Приложения

| Каталог | Назначение |
|---|---|
| `collector/` | Опрос GTFS Realtime и публикация событий в Kafka (T01–T03) |
| `api/` | Read-only FastAPI над Cassandra serving layer (T11), контракт — [`../docs/data-contracts/api.md`](../docs/data-contracts/api.md) |

API локально: `docker compose up -d api` (поднимет Cassandra и schema), затем
`http://localhost:8000/docs`. Маршруты появятся после `spark-static-serving`.
