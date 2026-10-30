# HTTP API v1 (T11)

Read-only API над подготовленными таблицами Cassandra. Каждый endpoint — один prepared query к
одной partition (ADR 0007); тяжёлые расчёты по HTTP-запросу не выполняются. Полная схема ответов —
OpenAPI (`/docs`, `/openapi.json`), генерируется из `apps/api/src/spb_transport_api/models.py`.

## Endpoints

| Метод и путь | Ответ | Таблица Cassandra |
|---|---|---|
| `GET /health` | `{"status": "ok"}`; Cassandra не проверяется | — |
| `GET /ready` | 200 `ready` или 503 `unavailable`; `static_feed_version` | `system.local`, `static_feed_current` |
| `GET /api/v1/vehicles?limit&cursor` | Последние позиции всех машин в окне свежести | `vehicle_latest_by_vehicle` |
| `GET /api/v1/vehicles/{vehicle_id}` | Последняя позиция; 404, если её нет или она старше окна | `vehicle_latest_by_vehicle` |
| `GET /api/v1/vehicles/{vehicle_id}/positions?from&to&limit&cursor` | История позиций в `[from, to)`, новые первыми | `vehicle_positions_by_vehicle_day` |
| `GET /api/v1/vehicles/{vehicle_id}/eta?from&to&limit&cursor` | История официальных ETA snapshots | `eta_history_by_vehicle_day` |
| `GET /api/v1/routes?limit&cursor` | Маршруты опубликованной версии static GTFS | `routes_by_feed` |
| `GET /api/v1/routes/{route_id}` | Маршрут; 404, если нет в текущей версии | `routes_by_feed` |
| `GET /api/v1/routes/{route_id}/stops` | Варианты маршрута (shape) с остановками по порядку | `stops_by_route` |
| `GET /api/v1/routes/{route_id}/vehicles?limit&cursor` | Текущие машины маршрута | `vehicles_latest_by_route` |
| `GET /api/v1/analytics` | Опубликованные аналитические срезы и их периоды | `analytics_current` |
| `GET /api/v1/analytics/routes/{route_id}/segments?day_type&hour` | Метрики участков с координатами остановок | `segment_metrics_by_route_bucket`, `stops_by_route` |
| `GET /api/v1/analytics/routes/{route_id}/anomalies` | Аномалии последнего опубликованного дня | `anomalies_by_route_day` |
| `GET /api/v1/analytics/routes/{route_id}/eta-comparison` | Ошибки официального и baseline ETA | `eta_comparison_by_route_day` |
| `GET /api/v1/analytics/clusters` | Кластеры и признаки маршрутов | `route_cluster_by_model`, `route_features_by_period` |

## Семантика

- **Время** — ISO 8601 в UTC. `from`/`to` обязаны содержать timezone; по умолчанию `to` = сейчас,
  `from` = `to − API_HISTORY_DEFAULT_RANGE_MINUTES`. Диапазон больше `API_HISTORY_MAX_RANGE_HOURS`,
  `from ≥ to` или время без timezone — 422.
- **Свежесть.** Latest-таблицы имеют TTL, но API дополнительно отбрасывает позиции старше
  `API_VEHICLE_FRESHNESS_SECONDS` по `source_timestamp`: наличие строки не доказывает активность.
  Поэтому страница списка может содержать меньше `limit` элементов при непустом `next_cursor`.
- **Пагинация** — курсором. `next_cursor` непрозрачен: он содержит paging state Cassandra и, для
  истории, день-партицию, с которой продолжать; курсор привязан к запросу (путь и фильтры), чужой
  или испорченный курсор — 400. `limit` от 1 до `API_MAX_PAGE_SIZE`. `next_cursor = null` — конец.
- **История** за диапазон через полночь UTC читается по дневным партициям от новой к старой.
- **ETA** сохраняет пустые snapshots (`eta_status = empty`, `forecasts = []`). `forecast_rank` —
  порядок ответа источника, не GTFS `stop_sequence`.
- **Маршруты и остановки** берутся из версии, указанной в `static_feed_current`; пока она не
  опубликована (`spark-static-serving`), `/routes*` отвечает 503. `stop_index` — позиция в stop
  pattern shape (как в T10), `distance_along_m = null` у остановки дальше порога от shape.
- `trip_id` и `stop_sequence` не возвращаются: realtime-наблюдение не связано с рейсом.

## Ошибки

| Код | Когда |
|---|---|
| 400 | Курсор испорчен, относится к другому запросу или устарел |
| 404 | Машина без свежей позиции, маршрут не найден |
| 422 | Невалидные параметры (`limit`, диапазон времени) |
| 503 | Cassandra недоступна, таймаут, static metadata не опубликованы |

Тело ошибки: `{"detail": "..."}`.
