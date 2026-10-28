# Gold: классификация будущего замедления (T16)

Единица датасета — прохождение текущего участка, за которым в том же run непосредственно следует
ещё один участок. Target равен `1`, если фактическое время следующего участка превышает его p90
для маршрута, pattern, локального часа и типа дня. p50/p90 рассчитываются только на явно заданном
обучающем диапазоне, который заканчивается до дня оценки.

Признаки не используют следующий участок: отношение текущего времени к p50, скорость, доля
задержки относительно running time, confidence, число наблюдений, максимальный разрыв и
циклический час. Первая интерпретируемая модель использует `current_travel_ratio` как risk score;
порог задаётся конфигурацией.

```text
HDFS_SLOWDOWN_CLASSIFIER_PATH/predictions/
  algorithm_version=slowdown-classifier-v1/evaluation_date=YYYY-MM-DD/
HDFS_SLOWDOWN_CLASSIFIER_PATH/metrics/
  algorithm_version=slowdown-classifier-v1/evaluation_date=YYYY-MM-DD/
```

Метрики: confusion matrix, precision, recall, F1, accuracy и ROC AUC. Prediction сохраняет
признаки, label, risk score, класс, временное разбиение и lineage. Cassandra не используется:
результат T16 — воспроизводимый offline dataset; продуктовый экран добавляется в T18.
