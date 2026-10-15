# Контракты realtime-событий (v1)

Модели событий лежат в `packages/contracts`. Лишние поля запрещены (`extra="forbid"`), поэтому новое поле в ответе источника само по себе не попадает в наши события.

## Vehicle position

Одна запись — одно наблюдение машины. `event_id` — SHA-256 от `vehicle_id` и `source_timestamp`, поэтому повторный опрос с тем же timestamp не создаёт нового события.

`source_timestamp` приходит из GTFS Realtime. `collected_at` устанавливает collector. Оба значения передаются в UTC. `speed` nullable, но ноль не преобразуется в null.

## Vehicle ETA snapshot

Одна запись соответствует содержимому `vehicletrips` для машины. `forecasts` может быть пустым. `forecast_rank` — нумерация позиции элемента в полученном массиве, начиная с нуля; это не GTFS `stop_sequence`.

`event_id` вычисляется из `vehicle_id`, `route_id` и канонического упорядоченного содержимого `forecasts`, без `collected_at`. Благодаря этому одинаковый прогноз, полученный несколькими poll cycles, имеет один бизнес-идентификатор.

## Raw response

Raw response хранит endpoint, request/collection time, HTTP status, content type, исходный payload в base64, SHA-256 payload и возможную ошибку декодирования. В v1 поддерживаются только `vehicle` и `vehicletrips`.

Raw response не дедуплицируется по правилам бизнес-событий: он является журналом обращений к источнику. URL query с `vehicleIDs` и другие потенциально чувствительные диагностические данные в контракт не входят.

## Запрещённые предположения

В v1 отсутствуют `trip_id`, `stop_sequence`, `arrival.delay`, `departure.time` и `departure.delay`. `/stopforecast` не поддерживается до отдельного исследования.

