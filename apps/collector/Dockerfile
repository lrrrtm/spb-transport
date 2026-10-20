# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:0.10.2 AS uv
FROM python:3.13-slim

COPY --from=uv /uv /uvx /bin/
WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY packages/contracts packages/contracts
COPY apps/collector apps/collector

RUN --mount=type=secret,id=build_ca \
    if [ -s /run/secrets/build_ca ]; then \
      SSL_CERT_FILE=/run/secrets/build_ca uv sync --frozen --no-dev --package spb-transport-collector; \
    else \
      uv sync --frozen --no-dev --package spb-transport-collector; \
    fi

ENV PATH="/app/.venv/bin:$PATH"
CMD ["python", "-m", "spb_transport_collector"]
