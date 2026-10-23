# Исследование: route/shape matching (T09)

Статус: **алгоритм реализован, пороги предварительные.** ADR 0004 требует подобрать пороги на
реальных данных. В момент реализации официальный API был недоступен из среды разработки, поэтому
значения в `.env.example` выбраны по литературе (Newson & Krumm, 2009) и типичной точности
городского GPS. До калибровки результаты matching можно использовать для диагностики, но не для
выводов в отчёте.

## Как собрать и проанализировать выборку

```shell
uv run python scripts/research/collect_route_matching_sample.py --duration-minutes 60
set -a; source <(grep '^ROUTE_MATCH_' .env); set +a
uv run python scripts/research/analyze_route_matching_sample.py --with-matcher \
    data/research/route-matching/<run>
```

Отчёт сохраняется в `<run>/report.md`. Сырые выборки лежат в git-ignored `data/`; в документ
переносятся только агрегаты и выбранные значения.

## Алгоритм `route-match-hmm-v1`

Трек — последовательность наблюдений одной машины с одним `route_id` без пропуска дольше
`ROUTE_MATCH_MAX_GAP_SECONDS`. Кандидаты наблюдения — проекции на каждый проход каждого shape
маршрута в радиусе `ROUTE_MATCH_MAX_OFFSET_M` плюс явное состояние `off_route`.

| Компонент | Формула |
|---|---|
| Emission кандидата | `−½(offset/σ)² + w·(cos Δbearing − 1)`; bearing учитывается только при `speed ≥ ROUTE_MATCH_BEARING_MIN_SPEED` |
| Emission `off_route` | `−½(outlier_offset/σ)²` — off_route правдоподобнее кандидата дальше `outlier_offset` |
| Тот же shape | `−|Δprogress − прямое расстояние| / β`, если `−max_backtrack ≤ Δprogress ≤ max_speed·Δt + max_backtrack`; иначе невозможно |
| Разворот на конечной | `−terminal_restart_cost`, если предыдущая точка в последних `terminal_zone` м shape, а новая — в первых `terminal_zone` м |
| Смена варианта | `−switch_cost` |
| Вход/выход `off_route` | `−off_route_transition_cost` |

Viterbi выбирает путь, forward-backward даёт posterior выбранного состояния — это `confidence`.
Posterior учитывает и будущие наблюдения, поэтому общий участок двух вариантов получает высокую
уверенность, если дальше трек расходится, и около `1/k`, если не расходится.

`run_id` — непрерывный монотонный проход по одному shape. Новый run начинается после разворота,
смены варианта или после off_route-участка, с которого нельзя монотонно продолжить предыдущий
проход. Единичный GPS-выброс run не разрывает.

`match_status`: `matched` (`confidence ≥ ROUTE_MATCH_MIN_CONFIDENCE`), `low_confidence`,
`off_route`, `no_static_shape`. `trip_id` и `stop_sequence` не выводятся.

## Предварительные параметры и что их калибрует

| Параметр | Значение | Раздел отчёта для калибровки |
|---|---:|---|
| `ROUTE_MATCH_MAX_OFFSET_M` | 75 | §3: радиус, покрывающий ≈p99 отклонения |
| `ROUTE_MATCH_GPS_SIGMA_M` | 15 | §3: ≈ p50 / 0.6745 отклонения (MAD-оценка σ) |
| `ROUTE_MATCH_OUTLIER_OFFSET_M` | 50 | §3: ≈ p95 отклонения |
| `ROUTE_MATCH_BEARING_WEIGHT` | 2 | §4–5: доля Δbearing ≤ 45° и разрешимость неоднозначности по bearing |
| `ROUTE_MATCH_BEARING_MIN_SPEED` | 1 | §4: единицы `speed` источника (м/с или км/ч) |
| `ROUTE_MATCH_MAX_BACKTRACK_M` | 30 | §7: квантиль отрицательного прогресса |
| `ROUTE_MATCH_MAX_SPEED_MPS` | 25 | §7: p99.9 скорости по прогрессу с запасом |
| `ROUTE_MATCH_TRANSITION_BETA_M` | 50 | §7: медиана `|Δprogress − прямое расстояние|` / ln 2 |
| `ROUTE_MATCH_SWITCH_COST` | 8 | §8: доля runs короче 3 наблюдений (ложные переключения) |
| `ROUTE_MATCH_TERMINAL_RESTART_COST` | 2 | §8 и ручная проверка разворотов на карте |
| `ROUTE_MATCH_OFF_ROUTE_TRANSITION_COST` | 3 | §8: доля off_route |
| `ROUTE_MATCH_TERMINAL_ZONE_M` | 300 | расстояние от конечной точки shape до места отстоя |
| `ROUTE_MATCH_MAX_GAP_SECONDS` | 300 | §6: интервалы между наблюдениями |
| `ROUTE_MATCH_MIN_CONFIDENCE` | 0.8 | §8: распределение confidence, ручная проверка выборки |

После калибровки обновляются `.env.example`, эта таблица и раздел «Результаты». Изменение
формул, а не только значений, требует новой `ALGORITHM_VERSION`.

## Результаты

Не получены: выборка с реальными данными ещё не собрана.
