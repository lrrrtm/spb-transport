# Realtime event contracts v1

Каноническая реализация DTO находится в `packages/contracts`. Модели используют `extra="forbid"`: появление нового поля источника не означает автоматического расширения внутреннего контракта.

## Vehicle position

Одна запись соответствует одному наблюдению машины. Детерминированный `event_id` вычисляется как SHA-256 от канонического представления как минимум `vehicle_id` и `source_timestamp`. Повторный polling того же source timestamp не создаёт новое бизнес-событие.

`source_timestamp` приходит из GTFS Realtime. `collected_at` устанавливает collector. Оба значения передаются в UTC. `speed` nullable, но ноль не преобразуется в null.

## Vehicle ETA snapshot

Одна запись соответствует содержимому `vehicletrips` для машины. `forecasts` может быть пустым. `forecast_rank` — нумерация позиции элемента в полученном массиве, начиная с нуля; это не GTFS `stop_sequence`.

`event_id` вычисляется из `vehicle_id`, `route_id` и канонического упорядоченного содержимого `forecasts`, без `collected_at`. Благодаря этому одинаковый прогноз, полученный несколькими poll cycles, имеет один бизнес-идентификатор.

В processed history Spark добавляет `forecast_count` и `eta_status`: `available` для
непустого списка и `empty` для явно полученного `forecasts=[]`. `missing` не является
событием источника и определяется как отсутствие snapshot в выбранном временном окне.

## Raw response

Raw response хранит endpoint, request/collection time, HTTP status, content type, исходный payload в base64, SHA-256 payload и возможную ошибку декодирования. В v1 поддерживаются только `vehicle` и `vehicletrips`.

Raw response не дедуплицируется по правилам бизнес-событий: он является журналом обращений к источнику. URL query с `vehicleIDs` и другие потенциально чувствительные диагностические данные в контракт не входят.

## Запрещённые предположения

В v1 отсутствуют `trip_id`, `stop_sequence`, `arrival.delay`, `departure.time` и `departure.delay`. `/stopforecast` не поддерживается до отдельного исследования.
