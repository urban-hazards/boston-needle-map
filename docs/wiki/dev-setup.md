# Dev Setup & Deployment Gotchas

Notes on running the project locally and deploying it, so we stop repeating the same mistakes.

## The dev server sync trap (historically the biggest foot-gun)

**Symptom:** `pnpm dev` loads the page, but the heatmap is blank, stats are zero, district filters don't appear, etc. Prod looks fine. "The dev server is broken again."

**Root cause:** `frontend/src/lib/bucket.ts` reads S3 credentials from env vars. Astro's dev server loads `.env` into `import.meta.env` **but not into `process.env`**. If `bucket.ts` reads only `process.env.ENDPOINT` / `process.env.BUCKET`, those come back empty in dev, `USE_S3` is `false`, and the frontend falls back to `EMPTY_PAGE_STATS` — hence the blank page.

In production on Railway, env vars are injected at the OS level, so `process.env` works. That's why it only broke in dev, and it stayed broken because "add env vars" isn't an obvious fix when prod is fine.

**Fix (April 18, 2026):** `bucket.ts` now reads via a helper that tries `import.meta.env` first, then falls back to `process.env`. Both dev and prod work without having to `source .env` before `pnpm dev`.

```ts
const env = (k: string): string => import.meta.env[k] ?? process.env[k] ?? ""
```

**If you add another env-var lookup in Astro server-side code (anywhere under `frontend/src/`):**
- Don't write `process.env.FOO` directly.
- Use the `env()` helper in `bucket.ts`, or read via `import.meta.env.FOO`.
- Test in dev with a **cold shell** (no `source .env`) before considering the change done. If it works only after sourcing .env, it will work on Railway but break locally the next time someone runs `pnpm dev`.

## The pipeline-deploy-skipped trap

**Symptom:** Frontend ships a new feature that reads a new field from `stats.json` (e.g., `council_district_labels`), feature silently renders empty because the field isn't in the file.

**Root cause:** The pipeline service on Railway is a separate service from the frontend. Pushing to `main` does not always trigger a new pipeline build — watchPatterns can be stale, Railway's auto-deploy toggle can be off, or the build can fail and nobody notices. The cron service keeps running the old image on its schedule, happily writing old-schema JSON to S3 every morning.

**Observed April 18, 2026:** Pipeline image was from April 6; PR #78 (district filters, merged ~April 7) never deployed. Fix: manually trigger a deploy via the Railway GraphQL API with the current commit SHA, verify the new `imageDigest` differs from the old one.

**When adding a new field to pipeline stats output:**
1. Confirm the pipeline Railway service redeploys after the merge (check `imageDigest` on the latest deployment vs. the prior one).
2. If the cron hasn't run yet, run the pipeline locally against prod bucket (`pipeline/.env` points at prod Tigris) to populate the new field immediately.
3. The frontend caches S3 JSON for 5 min, so changes propagate within that window.

## Local pipeline run = prod write

`pipeline/.env` and `frontend/.env` both point at the **production Tigris bucket**. Running `uv run boston-pipeline` or `pnpm dev` uses real prod credentials.

- `uv run boston-pipeline` overwrites prod `stats.json` / `points.json` / `markers.json` / `heatmap.json` in S3. That's fine when the current code is correct (and it's the fastest way to fix a stale deployment), but don't run it with uncommitted or unvetted changes.
- `pnpm dev` only reads from prod — safe.

If you ever want a real local-only setup, bring up MinIO via `docker compose up -d` from the repo root and point `.env` at `http://localhost:9000` / the `boston-hazards` bucket.

## Ports

`pnpm dev` tries 4321, then 4322, then 4323… if a prior dev server is still running. If you see Astro announce port 4322+, either kill the old process (`pkill -9 -f astro`) or be aware of which port your browser is pointed at.
