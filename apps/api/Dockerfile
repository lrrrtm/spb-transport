# syntax=docker/dockerfile:1
FROM ghcr.io/astral-sh/uv:0.10.2 AS uv
FROM python:3.13-slim

COPY --from=uv /uv /uvx /bin/
WORKDIR /app

COPY pyproject.toml uv.lock ./
COPY apps/api apps/api

ARG UV_INSECURE_HOST
RUN if [ -n "$UV_INSECURE_HOST" ]; then \
      echo "WARNING: uv TLS verification is disabled for: $UV_INSECURE_HOST"; \
      UV_INSECURE_HOST="$UV_INSECURE_HOST" \
        uv sync --frozen --no-dev --package spb-transport-api; \
    else \
      uv sync --frozen --no-dev --package spb-transport-api; \
    fi

ENV PATH="/app/.venv/bin:$PATH"
USER nobody
EXPOSE 8000
CMD ["python", "-m", "spb_transport_api"]
