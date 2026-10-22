.PHONY: install format lint typecheck test compose-check check

install:
	uv --system-certs sync --locked

format:
	uv --system-certs run ruff format .
	uv --system-certs run ruff check --fix .

lint:
	uv --system-certs run ruff format --check .
	uv --system-certs run ruff check .

typecheck:
	uv --system-certs run mypy apps/collector/src packages/contracts/src pipelines/spark/src scripts tests

test:
	uv --system-certs run pytest -n 6

compose-check:
	docker compose --env-file .env.example config --quiet

check: lint typecheck test compose-check
