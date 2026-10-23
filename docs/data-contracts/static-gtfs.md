# Static GTFS datasets

Версия статического feed — SHA-256 исходного ZIP. Исходник сохраняется без изменений:

```text
/raw/static_gtfs/ingest_date=YYYY-MM-DD/version=<sha256>/feed.zip
```

Производные Parquet находятся в `/processed/static_gtfs/version=<sha256>/`:

| Dataset | Поля |
|---|---|
| `routes` | `route_id`, `agency_id`, `route_short_name`, `route_long_name`, `route_type:int` |
| `stops` | `stop_id`, `stop_code`, `stop_name`, `stop_lat:double`, `stop_lon:double` |
| `trips` | `trip_id`, `route_id`, `service_id`, `direction_id:int`, `shape_id` |
| `shapes` | `shape_id`, `shape_pt_lat:double`, `shape_pt_lon:double`, `shape_pt_sequence:int`, `shape_dist_traveled:double` |
| `stop_times` | `trip_id`, `stop_id`, `stop_sequence:int`, `shape_dist_traveled:double` |

Перед записью проверяются обязательные GTFS-файлы и заголовки, размер архива, координаты
и числовые поля. Optional-поля, отсутствующие в feed, сохраняются как `null`. `stop_times` нужен T10 для
статических последовательностей остановок; времена расписания не переносятся: realtime-наблюдение
не связано с конкретным рейсом. Staging CSV
не является аналитическим слоем и может быть удалён после успешной обработки.
