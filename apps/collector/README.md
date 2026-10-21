# Collector

Пакет содержит централизованный HTTP boundary и два независимых polling-цикла официального GTFS Realtime API: `/vehicle` и batched `/vehicletrips`. Последний успешный набор активных машин из `/vehicle` служит входом для запросов ETA. Пустой `stop_time_update` публикуется как валидный ETA snapshot без прогнозов.

Обязательные environment variables перечислены в `docs/configuration.md` и `.env.example`. TLS verification включена по умолчанию; явное отключение сопровождается warning.

Если корпоративная сеть подменяет TLS-сертификаты PyPI, CA bundle передаётся при локальной сборке как BuildKit secret и не попадает в image:

```bash
docker build \
  --secret id=build_ca,src=/path/to/company-ca-bundle.pem \
  -f apps/collector/Dockerfile \
  -t spb-transport-collector:local .
```
