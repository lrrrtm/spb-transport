# Gold: метрики узких участков (T13)

`segment_metrics_batch.py` агрегирует уверенные прохождения T10 по направленному участку между
соседними остановками, типу дня (`weekday`/`weekend`) и локальному часу Europe/Moscow.

```text
HDFS_SEGMENT_METRICS_PATH/
  algorithm_version=segment-metrics-v1/
  analysis_start=YYYY-MM-DD/analysis_end=YYYY-MM-DD/
```

Строка содержит идентификаторы маршрута, shape, pattern и обеих остановок, `hour`, `day_type`,
длину участка, число всех и прошедших quality-фильтр наблюдений, число машин и сервисных дней.
`coverage = qualified_samples / total_samples`; `enough_data` показывает достижение минимальной
выборки.

Устойчивые показатели: `travel_time_p25_s`, `p50`, `p75`, `p90`, `p95`,
`travel_time_iqr_s`, `travel_time_mad_s`, медианная `speed_p50_mps`, средняя доля времени между
линиями остановок сверх чистого движения `stop_delay_share` и отношение медианного времени в
будние пики (07:00–09:59, 16:00–18:59) к остальным часам `peak_offpeak_ratio`. Последнее равно
`null`, если для участка нет данных одного из периодов.

В Cassandra те же строки доступны в `segment_metrics_by_route_bucket`; partition key включает
версию алгоритма и feed, маршрут, диапазон анализа, тип дня и час. HDFS остаётся каноническим
хранилищем.
