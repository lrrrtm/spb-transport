# Gold: ETA baseline и сравнение (T15)

`eta_baseline_batch.py` использует только segment traversals из диапазона, который заканчивается
до дня оценки. Для snapshot ETA определяется последняя недавно пройденная остановка той же
машины и маршрута. Baseline суммирует исторические медианы следующих участков того же pattern,
локального часа и типа дня до остановки из официального прогноза.

Фактическая метка — первое прибытие этой машины на целевую остановку после `collected_at` в
пределах настроенного горизонта. Официальный и собственный прогноз сравниваются только на одной
парной выборке, где известны фактическое прибытие и baseline.

```text
HDFS_ETA_COMPARISON_PATH/predictions/
  algorithm_version=eta-baseline-v1/evaluation_date=YYYY-MM-DD/
HDFS_ETA_COMPARISON_PATH/metrics/
  algorithm_version=eta-baseline-v1/evaluation_date=YYYY-MM-DD/
```

Prediction содержит официальный, baseline и фактический timestamps, абсолютные ошибки, горизонт
и lineage. Метрики по маршруту и горизонту: число размеченных и парных прогнозов, coverage
baseline, MAE и median absolute error обеих моделей. В Cassandra агрегаты публикуются в
`eta_comparison_by_route_day` с обучающим диапазоном в primary key.

Ограничение первой версии: snapshot без недавно пройденной остановки либо без достаточной истории
в том же часовом bucket не получает baseline и учитывается только в знаменателе coverage.
