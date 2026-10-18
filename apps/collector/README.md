# Collector

Пакет содержит централизованный HTTP boundary для официального GTFS Realtime API. На этапе T01 реализованы только конфигурация клиента, TLS/CA, timeout, ограниченные retry с exponential backoff/jitter и lifecycle. Polling и разбор `/vehicle` относятся к T02.

Обязательные environment variables перечислены в `docs/configuration.md` и `.env.example`. TLS verification включена по умолчанию; явное отключение сопровождается warning.

