FROM ghcr.io/astral-sh/uv:python3.12-bookworm-slim AS builder

WORKDIR /app
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-dev --no-install-project
COPY src/ src/
COPY templates/ templates/
COPY streamlit_app.py .
COPY README.md .
RUN uv sync --frozen --no-dev

FROM python:3.12-slim-bookworm

RUN groupadd --gid 1000 appuser && useradd --uid 1000 --gid 1000 --no-create-home appuser

WORKDIR /app
COPY --from=builder /app /app
COPY entrypoint.sh .

USER appuser

ENTRYPOINT ["./entrypoint.sh"]
