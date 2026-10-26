# SPB Transport Analyze

Учебный production-ready проект анализа надёжности и эффективности общественного транспорта Санкт-Петербурга по static GTFS и GTFS Realtime.

В репозитории зафиксированы архитектурные решения и контракты, работает realtime collector,
а raw HTTP responses архивируются через Kafka и Spark Structured Streaming в Parquet/HDFS.
Batch jobs сопоставляют позиции с маршрутами и строят события у остановок; read-only FastAPI
(`apps/api`) отдаёт текущий транспорт, историю, ETA и маршруты из Cassandra. React-интерфейс
(`apps/web`) показывает актуальный транспорт на карте. Последующая аналитика реализуется
отдельными задачами backlog.

## Быстрый старт bootstrap

Требования: `uv`, Docker с Compose и GNU Make. Нужную версию Python 3.13 `uv` установит автоматически при отсутствии.

```bash
cp .env.example .env
make install
make check
docker compose up -d
```

Статус инфраструктуры:

```bash
docker compose ps
```

Остановка без удаления данных:

```bash
docker compose down
```

Архитектура и backlog описаны в [`docs/architecture-plan.md`](docs/architecture-plan.md). Правила работы — в [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Границы monorepo

- `apps/` — collector, FastAPI (`apps/api`) и React frontend (`apps/web`);
- `pipelines/` — static GTFS ingestion и Spark jobs;
- `packages/contracts/` — общие версионированные межсервисные DTO;
- `infra/` — конфигурация локальной инфраструктуры;
- `tests/` — интеграционные fixtures и проверки границ;
- `docs/` — архитектура, контракты, исследования и runbooks.
