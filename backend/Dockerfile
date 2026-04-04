FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src/ src/
COPY README.md .
RUN uv sync --frozen --no-dev

FROM python:3.12-slim-bookworm

RUN groupadd --gid 1000 appuser && useradd --uid 1000 --gid 1000 --no-create-home appuser

WORKDIR /app
COPY --from=builder /app /app
RUN mkdir -p /app/tmp && chown 1000:1000 /app/tmp

USER appuser

CMD /app/.venv/bin/uvicorn boston_needle_map.api:app --host 0.0.0.0 --port ${PORT:-8000}
