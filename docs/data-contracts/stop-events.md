# Silver: stop arrivals, segment traversals и stop patterns (T10)

Производятся batch job `stop_events_batch.py` из `vehicle_route_matches` (T09) одного дня и
static GTFS той же версии. Алгоритм и пороги: [`../research/stop-arrivals.md`](../research/stop-arrivals.md).
Используются только наблюдения `match_status = 'matched'`.

```text
HDFS_STOP_PATTERNS_PATH/algorithm_version=<v>/static_feed_version=<sha256>/
HDFS_STOP_ARRIVALS_PATH/algorithm_version=<v>/event_date=YYYY-MM-DD/
HDFS_SEGMENT_TRAVERSALS_PATH/algorithm_version=<v>/event_date=YYYY-MM-DD/
```

`event_date` совпадает с партицией входных matches (UTC-день наблюдений), поэтому повторный запуск
перезаписывает ровно свою партицию. Локальный GTFS-день хранится в колонке `service_date`
(`Europe/Moscow`, по первому известному времени события).

## shape_stop_patterns

Доминирующая последовательность остановок shape (по числу trips) с проекцией на shape.

| Поле | Тип | Описание |
|---|---|---|
| `shape_id`, `pattern_id` | string | `pattern_id` — SHA-256 shape и упорядоченных `stop_id` |
| `trip_count`, `trip_share` | int, double | Число trips с этим pattern и его доля среди trips shape |
| `stop_index` | int | Позиция в pattern, с 0. Это не GTFS `stop_sequence` |
| `stop_id` | string | |
| `projected` | boolean | `false`: остановка дальше `STOP_EVENT_STOP_MAX_OFFSET_M` от shape или без координат |
| `distance_along_m`, `offset_m` | double, nullable | Положение на shape; неубывает вдоль pattern |

## stop_arrival_events

Одно событие на остановку pattern и run (монотонный проход машины по shape).

| Поле | Тип | Описание |
|---|---|---|
| `event_id` | string | SHA-256 run, pattern и `stop_index` |
| `vehicle_id`, `route_id`, `shape_id`, `direction_id`, `track_id`, `run_id`, `pattern_id` | | Контекст run |
| `stop_id`, `stop_index`, `stop_distance_m` | | Остановка pattern |
| `arrival_time` | timestamp, nullable | Вход в геозону `[d − zone_before, d + zone_after]` |
| `passage_time` | timestamp, nullable | Пересечение линии остановки; `null` только для `departure_only` |
| `departure_time` | timestamp, nullable | Выход из геозоны; `null`, если run закончился внутри неё |
| `dwell_s` | double, nullable | `departure_time − arrival_time` |
| `stopped` | boolean, nullable | `dwell_s ≥ STOP_EVENT_MIN_DWELL_SECONDS` |
| `observations_in_zone` | int | Наблюдения внутри геозоны |
| `method` | string | `observed` (есть наблюдение в зоне), `interpolated`, `departure_only` (run начался в зоне) |
| `crossing_gap_s` | double | Интервал между наблюдениями, между которыми интерполировано событие |
| `confidence` | double | Минимальная confidence двух наблюдений интерполяции |
| `static_feed_version`, `route_match_algorithm_version`, `algorithm_version` | string | Lineage |
| `service_date` | date | Локальный день |

## segment_traversals

Прохождение участка между соседними остановками pattern в одном run.

| Поле | Тип | Описание |
|---|---|---|
| `event_id` | string | SHA-256 run, pattern и `from_stop_index` |
| контекст run | | Как в `stop_arrival_events` |
| `from_stop_id`, `to_stop_id`, `from_stop_index`, `to_stop_index` | | Участок |
| `segment_length_m` | double | Расстояние вдоль shape |
| `start_time`, `end_time` | timestamp, nullable | `passage_time` начальной и конечной остановки |
| `departure_time`, `arrival_time` | timestamp, nullable | Выход из зоны начальной и вход в зону конечной остановки |
| `travel_time_s` | double, nullable | `end_time − start_time`: включает стоянку после линии начальной остановки |
| `running_time_s` | double, nullable | `arrival_time − departure_time`: время движения без стоянок; `null` при перекрытии зон |
| `speed_mps` | double, nullable | `segment_length_m / travel_time_s` |
| `observations`, `max_gap_s` | int, double | Покрытие участка наблюдениями; нужно T13 для порогов качества |
| `confidence` | double | Минимальная confidence наблюдений участка |
| lineage, `service_date` | | Как в `stop_arrival_events` |

Хотя бы одно из `travel_time_s`, `running_time_s` известно. `trip_id`, `stop_sequence` и
задержка относительно расписания не выводятся: realtime-наблюдение не связано с рейсом.

Data-quality counters (лог job): число runs, событий, `interpolated`, `departure_only`, `stopped`,
traversals, patterns и patterns с непроецированными остановками.
