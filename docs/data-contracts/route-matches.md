# Silver: vehicle_route_matches

Производится batch job `route_match_batch.py` (T09) из processed positions одного дня и одной
версии static GTFS. Алгоритм и пороги: [`../research/route-matching.md`](../research/route-matching.md).

```text
HDFS_ROUTE_MATCHES_PATH/algorithm_version=<v>/event_date=YYYY-MM-DD/
```

| Поле | Тип | Описание |
|---|---|---|
| `event_id` | string | Идентификатор позиции из `vehicle-position.v1` |
| `vehicle_id`, `route_id` | string | Из позиции |
| `source_timestamp` | timestamp (UTC) | Время наблюдения источника |
| `latitude`, `longitude`, `bearing`, `speed` | double | Из позиции; `speed=0` сохраняется |
| `track_id` | string | SHA-256: машина + первое событие трека (один `route_id`, без длинных пропусков) |
| `run_id` | string, nullable | SHA-256: трек + первое событие прохода по одному shape; `null` для off_route и no_static_shape |
| `match_status` | string | `matched`, `low_confidence`, `off_route`, `no_static_shape` |
| `confidence` | double | Posterior выбранного состояния, 0–1 |
| `shape_id` | string, nullable | Выбранный shape |
| `direction_id` | int, nullable | GTFS `direction_id`, только если все trips shape согласны |
| `segment_index` | int, nullable | Индекс отрезка shape |
| `distance_along_m` | double, nullable | Прогресс вдоль shape |
| `shape_length_m` | double, nullable | Длина shape |
| `offset_m` | double, nullable | Расстояние от GPS до shape |
| `bearing_difference_deg` | double, nullable | Разница bearing и направления отрезка, если машина двигалась |
| `static_feed_version` | string | SHA-256 static feed |
| `algorithm_version` | string | Версия алгоритма, также partition |

Правила использования:

- метрики, которым нужен точный участок, используют только `match_status = 'matched'`;
- остальные строки сохраняются для диагностики качества и не удаляются;
- `trip_id` и `stop_sequence` не выводятся.

Идемпотентность: job перезаписывает только партицию `(algorithm_version, event_date)`
(`partitionOverwriteMode=dynamic`); идентификаторы детерминированы. Перед matching повторно
выполняется дедупликация по `(vehicle_id, source_timestamp)`, так как streaming-дедупликация
ограничена watermark. Трек не пересекает границу `event_date` (UTC): полночь UTC — 03:00 по
Москве, когда наземный транспорт почти не работает.

Data-quality counters: job пишет в лог число строк по каждому `match_status`.
