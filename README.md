# SPB Transport Analyze

Учебный production-ready проект анализа надёжности и эффективности общественного транспорта Санкт-Петербурга по static GTFS и GTFS Realtime.

Сейчас репозиторий находится на этапе bootstrap: зафиксированы архитектурные решения, контракты realtime-событий, проверки качества и локальная data-инфраструктура. Collector, Spark pipelines, API и frontend реализуются отдельными задачами после bootstrap.

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

- `apps/` — collector, FastAPI и frontend по мере начала соответствующих задач;
- `pipelines/` — static GTFS ingestion и Spark jobs;
- `packages/contracts/` — общие версионированные межсервисные DTO;
- `infra/` — конфигурация локальной инфраструктуры;
- `tests/` — интеграционные fixtures и проверки границ;
- `docs/` — архитектура, контракты, исследования и runbooks.
