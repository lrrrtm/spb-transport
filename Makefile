.PHONY: install format lint typecheck test test-spark test-cassandra test-integration build-web compose-check check

install:
	uv --system-certs sync --locked
	npm ci --prefix apps/web

format:
	uv --system-certs run ruff format .
	uv --system-certs run ruff check --fix .

lint:
	uv --system-certs run ruff format --check .
	uv --system-certs run ruff check .
	npm run lint --prefix apps/web

typecheck:
	uv --system-certs run mypy apps/api/src apps/collector/src packages/contracts/src pipelines/spark/src scripts tests
	npm run typecheck --prefix apps/web

test:
	uv --system-certs run pytest -n 6
	npm test --prefix apps/web

build-web:
	npm run build --prefix apps/web

test-spark:
	uv --system-certs run --group spark pytest -m spark

test-cassandra:
	uv --system-certs run pytest -m cassandra

test-integration: test-spark test-cassandra

compose-check:
	docker compose --env-file .env.example config --quiet

check: lint typecheck test build-web compose-check
