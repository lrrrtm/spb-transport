# Приложения

| Каталог | Назначение |
|---|---|
| `collector/` | Опрос GTFS Realtime и публикация событий в Kafka (T01–T03) |
| `api/` | Read-only FastAPI над Cassandra serving layer (T11), контракт — [`../docs/data-contracts/api.md`](../docs/data-contracts/api.md) |
| `web/` | Desktop-интерфейс с картой актуального транспорта (T12) |

API локально: `docker compose up -d api` (поднимет Cassandra и schema), затем
`http://localhost:8000/docs`. Маршруты появятся после `spark-static-serving`.

Web локально: `npm ci --prefix apps/web`, затем `npm run dev --prefix apps/web` и
`http://localhost:5173`. Вместе со стеком: `docker compose up -d web`.
