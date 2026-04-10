FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder
WORKDIR /app
COPY pipeline/pyproject.toml pipeline/uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY pipeline/src/ src/
RUN uv sync --frozen --no-dev
# en_core_web_sm is declared as a direct URL dep in pyproject.toml, so
# `uv sync` above installs it. No separate `spacy download` step needed.

FROM python:3.12-slim-bookworm
RUN groupadd --gid 1000 appuser && useradd --uid 1000 --gid 1000 --no-create-home appuser
WORKDIR /app
COPY --from=builder /app /app
USER appuser
CMD ["/app/.venv/bin/boston-pipeline"]
