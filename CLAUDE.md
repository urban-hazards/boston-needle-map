# Boston Urban Hazard Maps - Project Guide

## Quick Reference
- **Architecture:** Monorepo — Astro SSR frontend + daily Python pipeline writing to S3
- **Pipeline:** Python 3.12+, uv, spaCy, boto3, Pydantic v2, Typer CLI
- **Frontend:** Astro (hybrid SSR), React islands, TypeScript (no semicolons), pnpm
- **Storage:** Railway storage bucket (S3-compatible, Tigris)
- **Linting (pipeline):** ruff + mypy (strict mode)
- **Linting (frontend):** Biome (no semicolons, tabs)
- **Git hooks:** lefthook (runs ruff + mypy + biome on pre-commit)
- **Deployment:** Railway (two services: Astro frontend + pipeline cron)
- **Git workflow:** PRs required for main; CI checks must pass before merge

## Pipeline Commands (run from `pipeline/`)
- `uv run boston-pipeline run` -- run full pipeline (all datasets)
- `uv run boston-pipeline run -d needles` -- run single dataset
- `uv run boston-pipeline run -d waste --force` -- force re-fetch from CKAN
- `uv run boston-pipeline run --verbose` -- debug logging
- `uv run ruff check src/ tests/` -- lint
- `uv run ruff format src/ tests/` -- format
- `uv run mypy src/` -- type check
- `uv run pytest` -- run tests

## Frontend Commands (run from `frontend/`)
- `pnpm dev` -- start dev server on :4321
- `pnpm build` -- build for production
- `pnpm preview` -- preview production build
- `pnpm check` -- astro check + biome check
- `pnpm lint` -- biome lint
- `pnpm format` -- biome format

## Architecture
- `pipeline/` -- Daily Python cron job (runs on Railway)
  - `src/pipeline/`
    - `run.py` -- Main orchestrator: fetch → clean → classify → compute → write to S3
    - `cli.py` -- Typer CLI entrypoint
    - `config.py` -- Constants (CKAN resource IDs, type filters, S3 env vars, waste keywords)
    - `models.py` -- Pydantic models (CleanedRecord, DashboardStats, etc.)
    - `fetcher.py` -- CKAN API data fetching
    - `cleaner.py` -- Raw record normalization and validation
    - `analytics.py` -- Stats computation (heatmap bins, neighborhoods, hourly)
    - `classifier.py` -- spaCy NLP classifier for human waste detection
    - `enricher.py` -- Open311 API description enrichment
    - `storage.py` -- S3 read/write wrapper (boto3)
  - `Dockerfile` -- Container for Railway cron service
- `frontend/` -- Astro SSR service (runs on Railway)
  - `src/pages/` -- Astro pages (index, neighborhood detail)
  - `src/components/` -- Astro server components + React client islands
  - `src/lib/bucket.ts` -- S3 reader with in-memory cache
  - `src/lib/types.ts` -- TypeScript types
  - `src/styles/` -- global CSS
  - `Dockerfile` -- frontend container
- `docs/architectural-decisions/` -- ADRs

## Data Flow
1. **Pipeline** (daily cron at 7 AM UTC) fetches current year from Boston's CKAN API
2. For waste: classifies records with spaCy NLP, enriches via Open311 API
3. Computes stats (heatmap bins, neighborhoods, hourly, monthly)
4. Writes precomputed JSON to Railway storage bucket
5. **Frontend** (Astro SSR) reads JSON from bucket on page load (5-min cache)
6. **Client** receives all points as props, filters by year/month in JavaScript

## Data Sources
All datasets come from the Boston 311 Service Requests on data.boston.gov:
- **Sharps:** type = "Needle Pickup" / "Needle Clean-up" / "Needle Cleanup" (2015–present)
- **Encampments:** type = "Encampments" (2025–present only)
- **Human Waste (Beta):** type = "Requests for Street Cleaning", classified via spaCy NLP (2024–present)

These are independent datasets from the same API, presented together for convenience.

## Conventions
- All data flows through Pydantic models (CleanedRecord, DashboardStats)
- Never commit secrets or API keys
- Use `python-dateutil` for datetime parsing
- TypeScript: no semicolons, use Biome, tabs for indentation
- Frontend uses pnpm, pipeline uses uv
- All changes go through PRs; CI must pass before merge
- Pipeline writes to S3 bucket; frontend reads from it (no direct API)
